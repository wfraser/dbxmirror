mod db;
mod downloader;
mod ops;
mod output;

#[macro_use]
extern crate log;

use crate::db::Database;
use crate::downloader::{DownloadRequest, DownloadResult, Downloader};
use crate::ops::Op;
use crate::output::{Output, OUT};
use anyhow::{anyhow, bail, Context};
use clap::Parser;
use clap_wrapper::clap_wrapper;
use crossbeam_channel::Receiver;
use dbxcase::{dbx_eq_ignore_case, dbx_strip_prefix_ignore_case};
use dropbox_content_hash::ContentHasher;
use dropbox_sdk::common::PathRoot;
use dropbox_sdk::default_client::{NoauthDefaultClient, UserAuthDefaultClient};
use dropbox_sdk::files::{
    self, FileMetadata, GetMetadataArg, ListFolderArg, ListFolderContinueArg, Metadata,
};
use dropbox_sdk::oauth2::Authorization;
use scopeguard::{guard, ScopeGuard};
use std::borrow::Borrow;
use std::cell::OnceCell;
use std::collections::VecDeque;
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;
use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime};
use walkdir::WalkDir;

const DATABASE_FILENAME: &str = ".dbxmirror.db";

/// dbxmirror :: Dropbox Mirror
///
/// This program allows you to efficiently maintain a mirror of a Dropbox folder structure.
#[clap_wrapper]
#[derive(Debug, Parser)]
#[command(version = format!("{} by {}", clap::crate_version!(), clap::crate_authors!(", ")))]
struct Args {
    #[command(subcommand)]
    op: Operation,

    #[command(flatten)]
    common: CommonOptions,
}

/// Common options
#[clap_wrapper]
#[derive(Debug, Parser)]
struct CommonOptions {
    /// Print DEBUG level messages from the program.
    #[arg(short, long)]
    verbose: bool,

    /// Print DEBUG level messages from the program and its libraries.
    #[arg(short, long)]
    debug: bool,

    /// Download up to this many files in parallel.
    #[arg(long, default_value_t = 10)]
    parallel_downloads: usize,
}

#[derive(Debug, Clone, Parser)]
enum Operation {
    /// Check the state of the local filesystem.
    Check(CheckArgs),

    /// Pull updates from the server and update the local filesystem.
    Pull(PullArgs),

    /// Perform initial setup for a synced directory.
    ///
    /// Prompts for authentication interactively.
    Setup(SetupArgs),

    /// View or change rules for paths to ignore.
    #[command(subcommand)]
    Ignore(IgnoreArgs),
}

#[clap_wrapper]
#[derive(Debug, Clone, Parser)]
struct SetupArgs {
    /// The path to the root of the Dropbox subtree to mirror. Must begin with a slash.
    #[arg()]
    remote_path: String,

    /// Override the root namespace ID to something else.
    #[arg(long)]
    root_namespace_id: Option<String>,
}

#[clap_wrapper]
#[derive(Debug, Clone, Parser)]
struct PullArgs {
    /// Don't download any files or make other changes, but do check existing local files.
    #[arg(long, alias = "no_download")]
    dry_run: bool,

    /// Pull the entire list of files from scratch instead of resuming from the last successful
    /// pull.
    #[arg(long)]
    full: bool,

    /// Only pull changes under the specified paths.
    ///
    /// If paths are specified, the stored cursor is not updated, so the next pull will also
    /// process the same changes again.
    #[arg(name = "PATH")]
    paths: Vec<PathBuf>,
}

#[clap_wrapper]
#[derive(Debug, Clone, Parser)]
struct CheckArgs {
    /// Check only the specified paths.
    #[arg(name = "PATH")]
    paths: Vec<PathBuf>,
}

#[clap_wrapper]
#[derive(Debug, Clone, Parser)]
enum IgnoreArgs {
    /// Add a path ignore rule.
    Add {
        /// Ignore paths under this.
        #[arg()]
        path: String,

        /// Path should be taken to be a regular expression.
        #[arg(long)]
        regex: bool,
    },

    /// Show current ignore rules.
    Show,

    /// Remove the named rule.
    Remove {
        #[arg()]
        path: String,
    },
}

fn setup(args: SetupArgs) -> anyhow::Result<()> {
    let db = Database::open(PathBuf::from(DATABASE_FILENAME))?;

    if db.config_opt("auth")?.is_some() {
        bail!("this directory is already configured; remove {DATABASE_FILENAME} to setup again");
    }

    let mut auth = dropbox_sdk::oauth2::get_auth_from_env_or_prompt();
    auth.obtain_access_token(NoauthDefaultClient::default())?;

    let client_id = auth.client_id().to_owned();
    let saved_auth = auth.save().unwrap();

    if args.remote_path != "/" {
        let client = UserAuthDefaultClient::new(auth);
        let meta = files::get_metadata(&client, &GetMetadataArg::new(args.remote_path.clone()))
            .with_context(|| format!("Failed to look up remote path {:?}", args.remote_path))?;
        if matches!(meta, Metadata::File(_)) {
            bail!(
                "Remote path {:?} refers to a file, not a folder.",
                args.remote_path
            );
        }
    }

    db.set_config("client_id", &client_id)?;
    db.set_config("auth", &saved_auth)?;

    db.set_config("remote_path", &args.remote_path)?;
    if let Some(nsid) = args.root_namespace_id {
        db.set_config("root_nsid", &nsid)?;
    } else {
        db.unset_config("root_nsid")?;
    }

    db.unset_config("cursor")?;

    Ok(())
}

fn ignore(args: IgnoreArgs, db: &Database) -> anyhow::Result<()> {
    match args {
        IgnoreArgs::Add { path, regex } => {
            db.add_ignore(&path, regex)?;
        }
        IgnoreArgs::Show => {
            let (regexes, paths): (Vec<_>, Vec<_>) =
                db.ignores()?.into_iter().partition(|(_path, regex)| *regex);
            if !paths.is_empty() {
                println!("Path Matches:");
                for (path, _) in paths {
                    println!("\t{path}");
                }
            }
            if !regexes.is_empty() {
                println!("Regex Matches:");
                for (regex, _) in regexes {
                    println!("\t{regex}");
                }
            }
        }
        IgnoreArgs::Remove { path } => {
            db.remove_ignore(&path)?;
        }
    }
    Ok(())
}

fn complete_downloads(
    dl_rx: &Receiver<DownloadResult>,
    db: &Database,
    block: bool,
) -> anyhow::Result<()> {
    let mut result = Ok(());
    while let Ok(rx) = if block {
        dl_rx.recv().map_err(|_| ())
    } else {
        dl_rx.try_recv().map_err(|_| ())
    } {
        if let Err(e) = rx.result {
            error!("download error: {e:#}");
            if result.is_ok() {
                // Save the first error.
                result = Err(e);
            }

            OUT.get().unwrap().remove_bar(&rx.path);

            // Complete any other pending finished downloads.
            continue;
        }

        db.set_file(&rx.path, rx.mtime, &rx.content_hash)?;
    }
    result
}

#[derive(Debug)]
enum ListFrom {
    Path(String),
    Cursor(String),
}

/// List Dropbox filesystem entries.
///
/// Paths are lowercase, absolute Dropbox-root-relative paths.
///
/// Starting cursor, if set, will resume listing all entries using that cursor, and the results will
/// be filtered according to the paths given. If unset, the given paths will be listed separately
/// and their results joined together.
///
/// The result is a vec of filesystem entries, and a final cursor which can be saved to the DB.
fn list_entries(
    list_from: ListFrom,
    client: Arc<UserAuthDefaultClient>,
) -> anyhow::Result<(Vec<Metadata>, String)> {
    debug!("Listing entries {list_from:?}");
    let mut page = match list_from {
        ListFrom::Cursor(cursor) => {
            files::list_folder_continue(client.as_ref(), &ListFolderContinueArg::new(cursor))
                .context("failed to list folder using cursor")?
        }
        ListFrom::Path(path) => {
            let entry = files::get_metadata(client.as_ref(), &GetMetadataArg::new(path.clone()))
                .with_context(|| format!("failed to get metadata for {path:?}"))?;
            if matches!(entry, Metadata::File(_)) {
                return Ok((vec![entry], String::new()));
            }
            files::list_folder(
                client.as_ref(),
                &ListFolderArg::new(path.clone())
                    .with_recursive(true)
                    .with_include_deleted(true),
            )
            .with_context(|| format!("failed to list folder {path:?}"))?
        }
    };

    let paths = OUT.get().unwrap().paths_progress();
    paths.inc(page.entries.len() as u64);

    let mut all_entries = page.entries;
    while page.has_more {
        page =
            files::list_folder_continue(client.as_ref(), &ListFolderContinueArg::new(page.cursor))
                .context("failed to continue listing folder")?;
        paths.inc(page.entries.len() as u64);
        all_entries.extend(page.entries);
    }

    Ok((all_entries, page.cursor))
}

fn pull(args: PullArgs, common_options: CommonOptions, db: &Database) -> anyhow::Result<()> {
    let mut remote_root = db.config("remote_path")?;
    let local_root = db.local_root();

    let ignores = db.ignores()?;

    let client = client(db)?;

    let mut cursor = if !args.full {
        db.config_opt("cursor")?
    } else {
        None
    };

    let (_downloader, dl_tx, dl_rx) = Downloader::new(
        remote_root.clone(),
        client.clone(),
        common_options.parallel_downloads,
    );
    let dl_tx = guard(dl_tx, |dl_tx| {
        error!("error occurred; waiting for in-flight downloads");
        drop(dl_tx);
        _ = complete_downloads(&dl_rx, db, true);
    });

    let mut downloaded_files = 0;
    let mut downloaded_bytes = 0;
    let mut would_download_files = vec![];

    // Root folder "/" needs to be special-cased to "" for list operations.
    if remote_root == "/" {
        remote_root = String::new();
    }

    let mut all_entries = vec![];
    let arg_paths = args
        .paths
        .iter()
        .map(|path| resolve_path(path, local_root, &remote_root))
        .collect::<anyhow::Result<Vec<_>>>()?;

    std::env::set_current_dir(local_root)
        .with_context(|| format!("failed to change working directory to {local_root:?}"))?;

    if let Some(starting_cursor) = cursor.clone() {
        let (mut entries, ending_cursor) =
            list_entries(ListFrom::Cursor(starting_cursor), client.clone())?;
        info!("{} changed paths", entries.len());

        if !arg_paths.is_empty() {
            entries.retain(|entry| {
                let path = match entry {
                    Metadata::File(f) => &f.path_lower,
                    Metadata::Folder(f) => &f.path_lower,
                    Metadata::Deleted(d) => &d.path_lower,
                }
                .as_ref()
                .unwrap();

                arg_paths
                    .iter()
                    .any(|prefix| path.starts_with_ignore_case(prefix))
            });
            info!(
                "filtered to {} entries matching paths on the command-line",
                entries.len()
            );
        }

        all_entries = entries;
        if args.paths.is_empty() {
            cursor = Some(ending_cursor);
        }
    } else {
        let root_paths = if args.paths.is_empty() {
            vec![remote_root.clone()]
        } else {
            arg_paths
        };

        for root in root_paths {
            let (entries, ending_cursor) =
                list_entries(ListFrom::Path(root.clone()), client.clone())?;
            info!("{} paths fetched for {root}", entries.len());
            all_entries.extend(entries);
            if args.paths.is_empty() {
                cursor = Some(ending_cursor);
            }
        }
    }

    OUT.get().unwrap().paths_progress().finish_and_clear();

    let mut ops = VecDeque::from(ops::list_folder_to_ops(
        all_entries,
        &(remote_root.clone() + "/"),
        &ignores,
        db,
    )?);
    info!("{} operations to perform", ops.len());

    let mut total_bytes = 0;
    let mut count_files = 0;
    for op in &ops {
        if let Op::AddedFile(_path, remote) = op {
            count_files += 1;
            total_bytes += remote.size;
        }
    }
    OUT.get().unwrap().set_total(count_files, total_bytes);

    while let Some(op) = ops.pop_front() {
        complete_downloads(&dl_rx, db, false)?;

        match op {
            Op::AddedFile(path, remote) => {
                if !remote.is_downloadable {
                    warn!("skipping non-downloadable file: {path}");
                    OUT.get().unwrap().dec_total(remote.size);
                    continue;
                }
                match open_file(&path).with_context(|| format!("checking local file {path}"))? {
                    OpenResult::File(mut local, _) => {
                        debug!("{path}: checking pre-existing local file");
                        if check_local_file(&path, &mut local, Some(&remote), false, db)
                            .with_context(|| format!("refusing to clobber local file {path}"))?
                        {
                            info!("{path}: up-to-date");
                            OUT.get().unwrap().dec_total(remote.size);
                            continue;
                        }
                    }
                    OpenResult::Dir(dirpath) => {
                        // As an edge case, an existing empty directory can be replaced by a file.
                        if let Err(e) = fs::remove_dir(dirpath) {
                            bail!("{path} exists and is a directory (and could not be removed: {e}); cannot download file");
                        }
                    }
                    OpenResult::NotFound => (),
                }

                let mtime = OffsetDateTime::parse(&remote.client_modified, &Rfc3339)
                    .with_context(|| {
                        format!("failed to parse timestamp {}", remote.client_modified)
                    })?
                    .unix_timestamp();
                let content_hash = remote
                    .content_hash
                    .ok_or_else(|| anyhow!("missing content_hash in API metadata"))?;

                match try_copy_local_file(&path, &content_hash, mtime, db, args.dry_run) {
                    Ok(true) => {
                        OUT.get().unwrap().dec_total(remote.size);
                        continue;
                    }
                    Ok(false) => (),
                    Err(e) => {
                        warn!("Would have copied {path:?} from local file, but: {e:#}");
                    }
                }

                debug!("-> {path}");
                downloaded_files += 1;
                downloaded_bytes += remote.size;
                if args.dry_run {
                    would_download_files.push(path);
                } else {
                    dl_tx
                        .send(DownloadRequest {
                            path,
                            rev: remote.rev,
                            size: remote.size,
                            mtime,
                            content_hash,
                        })
                        .unwrap();
                }
            }
            Op::DeletedFile(path) => {
                debug!("-> [delete] {path}");
                if args.dry_run {
                    info!("skipping delete of {path}");
                    continue;
                }
                match open_file(&path).with_context(|| format!("checking local file {path}"))? {
                    OpenResult::File(mut local, actual_file_path) => {
                        check_local_file(&path, &mut local, None, true, db)
                            .with_context(|| format!("refusing to delete local file {path}"))?;
                        info!("deleting {actual_file_path:?}");
                        fs::remove_file(&actual_file_path).with_context(|| {
                            format!("failed to remove local file {actual_file_path:?}")
                        })?;
                        db.remove_file(&path)?;
                    }
                    OpenResult::Dir(actual_file_path) => {
                        for entry in WalkDir::new(&actual_file_path) {
                            let entry = entry?;
                            if entry.file_type().is_dir() {
                                continue;
                            }
                            debug!("checking {:?}", entry.path());
                            let mut f = File::open(entry.path())
                                .with_context(|| entry.path().display().to_string())
                                .with_context(|| format!("failed to open {:?}", entry.path()))?;
                            check_local_file(
                                entry.path().to_string_lossy().borrow(),
                                &mut f,
                                None,
                                true,
                                db,
                            )
                            .with_context(|| {
                                format!("refusing to delete unknown local file {:?}", entry.path())
                            })?;
                        }
                        // All files in the dir checked ok; delete it.
                        // There's a toctou issue here, but without holding all the checked
                        // files open the whole time, we can't really fix it. Just don't modify
                        // the dir during this check, okay? :P
                        info!("deleting dir {actual_file_path:?}");
                        fs::remove_dir_all(&actual_file_path).with_context(|| {
                            format!("failed to remove local directory {actual_file_path:?}")
                        })?;

                        let mut files = vec![];
                        let check = path.to_owned() + "/";
                        db.for_files(|subpath| {
                            if subpath.starts_with(&check) {
                                files.push(subpath.to_owned());
                            }
                            Ok(())
                        })?;
                        for subfile in files {
                            db.remove_file(&subfile)?;
                        }
                        db.remove_file(&path)?;
                    }
                    OpenResult::NotFound => {
                        db.remove_file(&path)?;
                    }
                }
            }
            Op::MovedFile {
                old_path,
                new_path,
                remote,
            } => {
                debug!("-> move {old_path} -> {new_path}");
                if !args.dry_run {
                    if let Err(e) = fs::rename(&old_path, &new_path) {
                        warn!("failed to move {old_path:?} to {new_path:?}: {e}; downloading the file afresh instead");
                        ops.push_front(Op::DeletedFile(old_path));
                        ops.push_front(Op::AddedFile(new_path, remote));
                        continue;
                    }
                    db.rename_file(&old_path, &new_path).with_context(|| {
                        format!("failed to move {old_path:?} to {new_path:?} in the DB")
                    })?;
                }
                info!(
                    "{} {old_path:?} to {new_path:?}",
                    if args.dry_run {
                        "Would have moved"
                    } else {
                        "Moved"
                    }
                );
            }
            Op::CreateFolder(path) => {
                // We don't store folders in the DB, but we can at least create them in the FS.
                debug!("-> [folder] {path}");
                if !args.dry_run {
                    create_dir(&path)?;
                }
            }
        }
    }

    // Defuse error handler.
    drop(ScopeGuard::into_inner(dl_tx));

    if downloaded_files > 0 {
        complete_downloads(&dl_rx, db, true)?;
    }

    info!(
        "{}downloaded {downloaded_bytes} bytes across {downloaded_files} files",
        if args.dry_run { "would have " } else { "" }
    );

    if args.dry_run {
        info!("would have downloaded these files:");
        for file in would_download_files {
            info!("\t{file}");
        }
        info!("not updating cursor because dry run is enabled");
    } else if let Some(cursor) = cursor {
        db.set_config("cursor", &cursor)?;
    } else {
        info!("not updating cursor because pull was limited to sub-path");
    }

    Ok(())
}

fn check(args: CheckArgs, db: &Database) -> anyhow::Result<()> {
    eprintln!("Hashing files...");
    let mut checks = 0;
    let mut violations = vec![];

    let local_root = db.local_root();
    let paths = args
        .paths
        .iter()
        .map(|path| resolve_path(path, local_root, ""))
        .collect::<anyhow::Result<Vec<String>>>()?;

    std::env::set_current_dir(local_root)
        .with_context(|| format!("failed to change working directory to {local_root:?}"))?;

    db.for_files(|path| {
        if !args.paths.is_empty() && paths.iter().all(|prefix| !path.starts_with(&prefix[1..])) {
            return Ok(());
        }

        checks += 1;
        eprint!("{path}");
        io::stderr().flush().unwrap();
        match open_file(path) {
            Ok(OpenResult::File(mut local, _)) => {
                match check_local_file(path, &mut local, None, true, db) {
                    Ok(_) => {
                        eprint!("\r{:width$}\r", "", width = path.len());
                    }
                    Err(e) => {
                        eprintln!(": {e:#}");
                        violations.push(format!("{path}: {e:#}"));
                    }
                }
            }
            Ok(OpenResult::Dir(_)) => {
                eprintln!(": expected file; found dir");
                violations.push(format!("{path}: expected file; found dir"));
            }
            Ok(OpenResult::NotFound) => {
                eprintln!(": local file not found");
                violations.push(format!("{path}: local file not found"));
            }
            Err(e) => {
                eprintln!(": failed to open local file: {e:#}");
                violations.push(format!("{path}: failed to open local file: {e:#}"));
            }
        }
        Ok(())
    })?;
    println!("{checks} checks, {} violations", violations.len());
    for v in violations {
        println!("{v}");
    }
    Ok(())
}

/// See if the file is a duplicate of something we already have, and copy it instead of
/// downloading.
/// Returns true if this was done; false if no appropriate file was found, or an error if something
/// unexpected happened.
fn try_copy_local_file(
    path: &str,
    content_hash: &str,
    mtime: i64,
    db: &Database,
    dry_run: bool,
) -> anyhow::Result<bool> {
    let Some(source_path) = db.get_file_by_hash(content_hash, path)? else {
        // No matching file found to copy.
        return Ok(false);
    };

    // Open the file and get its actual casing on the local filesystem.
    let OpenResult::File(mut file, actual_path) =
        open_file(&source_path).with_context(|| source_path.clone())?
    else {
        bail!("local file {source_path:?} is missing");
    };

    if !check_local_file(&source_path, &mut file, None, true, db)? {
        bail!("{source_path:?} content is not what is expected");
    }
    drop(file);

    if dry_run {
        info!("Would copy {path:?} from pre-existing local file {actual_path:?}");
        // Pretend like we didn't find anything.
        return Ok(false);
    }

    debug!("Copying {path:?} from {actual_path:?}");
    fs::copy(&actual_path, path)
        .with_context(|| format!("failed to copy {actual_path:?} to {path:?}"))?;

    let OpenResult::File(dest, _) = open_file(path).with_context(|| path.to_owned())? else {
        bail!("newly-created file {path:?} could not be opened");
    };

    dest.set_modified(SystemTime::UNIX_EPOCH + Duration::seconds(mtime))
        .context("failed to set mtime")?;
    db.set_file(path, mtime, content_hash)?;
    info!("Copied {path:?} from pre-existing local file");

    Ok(true)
}

/// Check a local file against local database and optionally remote metadata.
/// Returns true if the file matches.
fn check_local_file(
    path: &str,
    local: &mut File,
    remote: Option<&FileMetadata>,
    hash_files: bool,
    db: &Database,
) -> anyhow::Result<bool> {
    let cached_hash = OnceCell::<String>::new();
    let local_content_hash = |local: &mut File| -> anyhow::Result<&str> {
        if !hash_files {
            return Ok("");
        }
        if let Some(hash) = cached_hash.get().as_ref() {
            return Ok(hash);
        }
        let h = ContentHasher::from_stream(local)
            .context("failed to hash local file")?
            .finish_str();
        cached_hash.set(h).unwrap();
        Ok(cached_hash.get().unwrap().as_str())
    };

    let local_mtime = local
        .metadata()
        .context("failed to stat")?
        .modified()
        .context("failed to stat")?
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs_f64().round() as i64)
        .unwrap_or_else(|e| -(e.duration().as_secs() as i64));

    // Check local file against existing database entry.
    let db_match = if let Some((db_mtime, db_content_hash)) = db.get_file(path)? {
        if local_mtime != db_mtime {
            bail!("local file modification time mismatch with DB");
        }
        if hash_files && local_content_hash(local)? != db_content_hash {
            bail!("local file hash mismatch with DB");
        }
        true
    } else {
        false
    };

    // Check local file against remote metadata.
    if let Some(remote) = remote {
        let remote_mtime =
            OffsetDateTime::parse(&remote.client_modified, &Rfc3339)?.unix_timestamp();
        let remote_content_hash = remote
            .content_hash
            .as_deref()
            .ok_or_else(|| anyhow!("missing content_hash"))?;

        if local_mtime == remote_mtime
            && (!hash_files || local_content_hash(local)? == remote_content_hash)
        {
            // Local file matches remote already; mark it in DB directly.
            db.set_file(path, remote_mtime, remote_content_hash)?;
            Ok(true)
        } else if db_match {
            // File matches DB, but not remote. Ok to overwrite.
            Ok(false)
        } else {
            // File doesn't have a DB entry, and does not match remote
            debug!(" local mtime: {local_mtime}");
            debug!("remote mtime: {remote_mtime}");
            if hash_files {
                debug!(" local hash: {:?}", local_content_hash(local));
                debug!("remote hash: {}", remote_content_hash);
            } else {
                debug!("hashing disabled");
            }
            Err(anyhow!("unknown local file, doesn't match remote metadata"))
        }
    } else {
        // We have no remote metadata (i.e. are deleting or checking local DB)
        if db_match {
            // Local DB is okay
            Ok(true)
        } else {
            // No local DB entry
            if local.metadata()?.file_type().is_dir() {
                return Err(anyhow!("not deleting local dir without checking it first"));
            }
            Err(anyhow!("unknown local file, no remote metadata"))
        }
    }
}

fn client(db: &Database) -> anyhow::Result<Arc<UserAuthDefaultClient>> {
    let mut client = UserAuthDefaultClient::new(
        Authorization::load(db.config("client_id")?, &db.config("auth")?)
            .ok_or_else(|| anyhow!("unable to load authorization from db"))?,
    );

    if let Some(nsid) = db.config_opt("root_nsid")? {
        client.set_path_root(&PathRoot::NamespaceId(nsid));
    }

    Ok(Arc::new(client))
}

enum OpenResult {
    File(File, PathBuf),
    Dir(PathBuf),
    NotFound,
}

fn open_file(path: &str) -> anyhow::Result<OpenResult> {
    macro_rules! try_open {
        ($path:expr) => {
            let path = Path::new($path);
            match File::open(path) {
                Ok(f) => {
                    if f.metadata()
                        .map(|m| m.file_type().is_dir())
                        .unwrap_or(false)
                    {
                        return Ok(OpenResult::Dir(path.into()));
                    }
                    return Ok(OpenResult::File(f, path.into()));
                }

                #[cfg(windows)]
                Err(e)
                    if e.kind() == io::ErrorKind::PermissionDenied
                        && fs::metadata(path)
                            .map(|m| m.file_type().is_dir())
                            .unwrap_or(false) =>
                {
                    return Ok(OpenResult::Dir(path.into()));
                }

                Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e.into()),
                _ => (), // continue to case-insentive lookup
            }
        };
    }

    try_open!(path);

    let mut cur = PathBuf::from(".");

    'component: for component in path.split('/') {
        for entry in fs::read_dir(&cur).with_context(|| format!("unable to open dir {cur:?}"))? {
            let entry = entry?;
            if entry.file_name().eq_ignore_case(component) {
                cur = entry.path();
                continue 'component;
            }
        }
        return Ok(OpenResult::NotFound);
    }

    try_open!(&cur);
    Err(anyhow!("failed to open file {cur:?}"))
}

fn create_dirs_case_insentive(path: &str) -> anyhow::Result<PathBuf> {
    let mut cur = PathBuf::from(".");

    let mut it = path.split('/').peekable();
    'component: while let Some(component) = it.next() {
        if it.peek().is_none() {
            cur.push(component);
            break;
        }

        for entry in fs::read_dir(&cur).with_context(|| format!("unable to open dir {cur:?}"))? {
            let entry = entry?;
            if entry.file_name().eq_ignore_case(component) {
                cur.push(entry.file_name());
                continue 'component;
            }
        }

        cur.push(component);
        match fs::create_dir(&cur) {
            Ok(()) => (),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => (),
            Err(e) => return Err(e).context(format!("failed to create dir at {cur:?}")),
        }
    }

    Ok(cur)
}

pub(crate) fn create_file(path: &str) -> anyhow::Result<File> {
    let adj = create_dirs_case_insentive(path)?;
    File::create(&adj).with_context(|| format!("failed to create file at {adj:?}"))
}

fn create_dir(path: &str) -> anyhow::Result<()> {
    let adj = create_dirs_case_insentive(path)?;
    match fs::create_dir(&adj) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(e).with_context(|| format!("failed to create dir at {adj:?}")),
    }
}

/// Take a local path, absolute or relative to CWD, and translate it to an absolute Dropbox path,
/// using the configured local and remote roots.
fn resolve_path(local_path: &Path, local_root: &Path, remote_root: &str) -> anyhow::Result<String> {
    let mut absolute = std::env::current_dir().expect("unable to get current working dir");
    for component in local_path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir => {
                absolute = PathBuf::from(component.as_os_str().to_owned());
            }
            Component::CurDir => (),
            Component::ParentDir => {
                absolute.pop();
            }
            Component::Normal(path) => {
                absolute.push(path);
            }
        }
    }

    let relative = absolute
        .strip_prefix(local_root)
        .map_err(|_| anyhow!("cannot operate on path outside the mirror root: {absolute:?}"))?;

    let path = relative
        .to_str()
        .with_context(|| format!("cannot operate on non-UTF8 path: {relative:?}"))?;

    Ok(format!("{remote_root}/{path}"))
}

trait StrExt {
    fn strip_prefix_ignore_case(&self, prefix: &str) -> Option<&'_ str>;
    fn eq_ignore_case(&self, other: &str) -> bool;

    fn starts_with_ignore_case(&self, prefix: &str) -> bool {
        self.strip_prefix_ignore_case(prefix).is_some()
    }
}

impl StrExt for str {
    fn strip_prefix_ignore_case(&self, prefix: &str) -> Option<&'_ str> {
        dbx_strip_prefix_ignore_case(self, prefix)
    }

    fn eq_ignore_case(&self, other: &str) -> bool {
        dbx_eq_ignore_case(self, other)
    }
}

impl StrExt for OsString {
    fn strip_prefix_ignore_case(&self, prefix: &str) -> Option<&'_ str> {
        self.to_str()
            .and_then(|s| s.strip_prefix_ignore_case(prefix))
    }

    fn eq_ignore_case(&self, other: &str) -> bool {
        self.to_str()
            .map(|s| s.eq_ignore_case(other))
            .unwrap_or(false)
    }
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    Output::init(&args.common);

    let db = if let Operation::Setup(setup_args) = args.op {
        return setup(setup_args);
    } else {
        let mut cwd = std::env::current_dir().context("failed to get working dir")?;
        loop {
            match fs::metadata(cwd.join(DATABASE_FILENAME)) {
                Ok(_) => break,
                Err(e) => {
                    if e.kind() != io::ErrorKind::NotFound {
                        return Err(e).context(format!(
                            "failed to check for database file {:?}",
                            cwd.join(DATABASE_FILENAME)
                        ));
                    }
                }
            }
            if cwd.parent().is_none() {
                return Err(anyhow!(
                    "Failed to find {DATABASE_FILENAME:?} in this or any parent directories."
                ))
                .context("Please run \"dbxmirror setup\" before running other commands.");
            }

            cwd.pop();
        }
        debug!("local root is {cwd:?}");
        Database::open(cwd.join(DATABASE_FILENAME))?
    };

    if db.config_opt("remote_path")?.is_none() {
        bail!("Sync directory not set up yet. Please run 'dbxmirror setup' first.");
    }

    match args.op {
        Operation::Setup(_) => unreachable!(),
        Operation::Pull(pull_args) => pull(pull_args, args.common, &db).or_else(|e| {
            if let Some(files::ListFolderContinueError::Reset) = e.root_cause().downcast_ref() {
                error!("Dropbox requires a directory iterator cursor reset. Try again.");
                db.unset_config("cursor")?;
            }
            Err(e)
        })?,
        Operation::Ignore(ignore_args) => ignore(ignore_args, &db)?,
        Operation::Check(check_args) => check(check_args, &db)?,
    }

    Ok(())
}
