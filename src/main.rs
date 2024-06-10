mod db;
mod downloader;
mod output;

#[macro_use]
extern crate log;

use crate::db::{Database, DatabaseOpts};
use crate::downloader::{DownloadRequest, DownloadResult, Downloader};
use crate::output::{Output, OUT};
use anyhow::{anyhow, bail, Context};
use clap::Parser;
use clap_wrapper::clap_wrapper;
use crossbeam_channel::Receiver;
use dropbox_sdk::common::PathRoot;
use dropbox_sdk::default_client::{NoauthDefaultClient, UserAuthDefaultClient};
use dropbox_sdk::files;
use dropbox_sdk::files::{
    FileMetadata, GetMetadataArg, ListFolderArg, ListFolderContinueArg, Metadata,
};
use dropbox_sdk::oauth2::Authorization;
use dropbox_toolbox::content_hash::ContentHash;
use dropbox_toolbox::ResultExt;
use scopeguard::{guard, ScopeGuard};
use std::cell::OnceCell;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use std::{fs, io};
use std::ffi::OsString;
use dbxcase::dbx_tolower;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

const DATABASE_PATH: &str = "./.dbxmirror.db";

/// dbxmirror :: Dropbox Mirror
///
/// This program allows you to efficiently maintain a mirror of a Dropbox folder structure.
#[clap_wrapper]
#[derive(Debug, Parser)]
#[command(version)]
struct Args {
    #[command(subcommand)]
    op: Operation,

    #[command(flatten)]
    common: CommonOptions,

    #[command(flatten)]
    db: DatabaseOpts,
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
}

#[derive(Debug, Clone, Parser)]
enum Operation {
    /// Check the state of the local filesystem.
    Check,

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
    #[arg()]
    remote_path: String,

    /// Override the root namespace ID to something else.
    #[arg(long)]
    root_namespace_id: Option<String>,
}

#[clap_wrapper]
#[derive(Debug, Clone, Parser)]
struct PullArgs {
    /// Don't download any files, but do check existing local files.
    #[arg(long)]
    no_download: bool,

    /// Pull the entire list of files from scratch instead of resuming from the last successful
    /// pull.
    #[arg(long)]
    full: bool,
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

fn setup(args: SetupArgs, db_opts: &DatabaseOpts) -> anyhow::Result<()> {
    let db = Database::open(DATABASE_PATH, db_opts)?;

    if db.config_opt("auth")?.is_some() {
        bail!("this directory is already configured; remove {DATABASE_PATH} to setup again");
    }

    let mut auth = dropbox_sdk::oauth2::get_auth_from_env_or_prompt();
    auth.obtain_access_token(NoauthDefaultClient::default())?;

    let client_id = auth.client_id.clone();
    let saved_auth = auth.save().unwrap();

    if args.remote_path != "/" {
        let client = UserAuthDefaultClient::new(auth);
        match files::get_metadata(&client, &GetMetadataArg::new(args.remote_path.clone())).combine()
        {
            Ok(meta) => {
                if matches!(meta, Metadata::File(_)) {
                    bail!(
                        "Remote path {:?} refers to a file, not a folder.",
                        args.remote_path
                    );
                }
            }
            Err(e) => {
                bail!("Failed to look up remote path {:?}: {e}", args.remote_path);
            }
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

            // Complete any other pending finished downloads.
            continue;
        }

        OUT.get()
            .unwrap()
            .download_progress(&rx.path, rx.size, rx.size);
        db.set_file(&rx.path, rx.mtime, &rx.content_hash)?;
    }
    result
}

fn pull(args: PullArgs, db: &Database) -> anyhow::Result<()> {
    let mut remote_root = db.config("remote_path")?;

    let ignores = db.ignores()?;

    let client = client(db)?;

    let cursor = if !args.full {
        db.config_opt("cursor")?
    } else {
        None
    };

    let (_downloader, dl_tx, dl_rx) = Downloader::new(remote_root.clone(), client.clone());
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

    let mut page = if let Some(cursor) = cursor {
        files::list_folder_continue(client.as_ref(), &ListFolderContinueArg::new(cursor))
            .combine()?
    } else {
        files::list_folder(
            client.as_ref(),
            &ListFolderArg::new(remote_root.clone())
                .with_recursive(true)
                .with_include_deleted(true),
        )
        .combine()?
    };

    loop {
        'entries: for entry in page.entries {
            complete_downloads(&dl_rx, db, false)?;

            let path = match &entry {
                Metadata::File(f) => f.path_display.as_ref(),
                Metadata::Deleted(d) => d.path_display.as_ref(),
                Metadata::Folder(f) => f.path_display.as_ref(),
            };
            if path.map(|s| s.eq_ignore_case(&remote_root)).unwrap_or(false) {
                // This is an entry for the root itself.
                continue;
            }
            let path = path
                .ok_or_else(|| anyhow!("missing path_display field from API"))?
                .strip_prefix_case_insensitive(&(remote_root.clone() + "/"))
                .ok_or_else(|| {
                    anyhow!(
                        "remote path {:?} doesn't start with root path {:?}",
                        path,
                        remote_root
                    )
                })?
                .to_owned();

            for (test, regex) in &ignores {
                if *regex {
                    todo!("regex support");
                } else if path.starts_with(test) {
                    continue 'entries;
                }
            }

            match entry {
                Metadata::File(remote) => {
                    debug!("-> {path}");
                    match open_file(&path) {
                        Ok(Some(mut local)) => {
                            debug!("checking pre-existing local file");
                            if check_local_file(&path, &mut local, Some(&remote), false, db)
                                .with_context(|| format!("refusing to clobber local file {path}"))?
                            {
                                debug!("local file is already up-to-date");
                                continue;
                            }
                        }
                        Ok(None) => (),
                        Err(e) => {
                            return Err(e).with_context(|| {
                                format!("error checking metadata of local file {path}")
                            });
                        }
                    }

                    let mtime = OffsetDateTime::parse(&remote.client_modified, &Rfc3339)
                        .with_context(|| {
                            format!("failed to parse timestamp {}", remote.client_modified)
                        })?
                        .unix_timestamp();
                    let content_hash = remote
                        .content_hash
                        .ok_or_else(|| anyhow!("missing content_hash in API metadata"))?;

                    downloaded_files += 1;
                    downloaded_bytes += remote.size;
                    if args.no_download {
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
                Metadata::Folder(_) => {
                    // We don't store folders in the DB, but we can at least create them in the FS.
                    debug!("-> [folder] {path}");
                    create_dir(&path)?;
                }
                Metadata::Deleted(_) => {
                    if args.no_download {
                        info!("skipping delete of {path}");
                        continue;
                    }
                    match open_file(&path) {
                        Ok(Some(mut local)) => {
                            check_local_file(&path, &mut local, None, true, db)
                                .with_context(|| format!("refusing to delete local file {path}"))?;
                            info!("deleting {path}");
                            if let Err(e) = fs::remove_file(&path)
                                .with_context(|| format!("failed to remove local file {path}"))
                            {
                                // Is it a directory?
                                if local
                                    .metadata()
                                    .map(|m| m.file_type().is_dir())
                                    .unwrap_or(false)
                                {
                                    fs::remove_dir(&path).with_context(|| {
                                        format!("failed to remove local directory {path}")
                                    })?;
                                } else {
                                    return Err(e);
                                }
                            }
                            db.remove_file(&path)?;
                        }
                        Ok(None) => {
                            db.remove_file(&path)?;
                        }
                        Err(e) => {
                            return Err(e).with_context(|| {
                                format!("error checking metadata of local file {path}")
                            });
                        }
                    }
                }
            }
        }

        if !page.has_more {
            break;
        }

        page =
            files::list_folder_continue(client.as_ref(), &ListFolderContinueArg::new(page.cursor))
                .combine()?;
    }

    // Defuse error handler.
    drop(ScopeGuard::into_inner(dl_tx));

    if downloaded_files > 0 {
        complete_downloads(&dl_rx, db, true)?;
    }

    info!(
        "{}downloaded {downloaded_bytes} bytes across {downloaded_files} files",
        if args.no_download { "would have " } else { "" }
    );

    if args.no_download && downloaded_files > 0 {
        info!("would have downloaded these files:");
        for file in would_download_files {
            info!("\t{file}");
        }
        info!("not updating cursor because some needed files were not downloaded");
    } else {
        db.set_config("cursor", &page.cursor)?;
    }

    Ok(())
}

fn check(db: &Database) -> anyhow::Result<()> {
    println!("Hashing files...");
    let mut checks = 0;
    let mut violations = 0;
    db.for_files(|path| {
        checks += 1;
        violations += 1;
        eprint!("{path}");
        io::stderr().flush().unwrap();
        match open_file(path) {
            Ok(Some(mut local)) => match check_local_file(path, &mut local, None, true, db) {
                Ok(_) => {
                    eprint!("\r{:width$}\r", "", width = path.len());
                    violations -= 1;
                }
                Err(e) => {
                    eprintln!(": {e}");
                }
            },
            Ok(None) => {
                eprintln!(": local file not found");
            }
            Err(e) => {
                eprintln!(": failed to open local file: {e}");
            }
        }
        Ok(())
    })?;
    println!("{checks} checks, {violations} violations");
    Ok(())
}

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
        let mut h = ContentHash::new();
        h.read_stream(local).context("failed to hash local file")?;
        cached_hash.set(h.finish_hex()).unwrap();
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
            Err(anyhow!("unknown local file, doesn't match remote metadata"))
        }
    } else {
        // We have no remote metadata (i.e. are deleting or checking local DB)
        if db_match {
            // Local DB is okay
            Ok(true)
        } else {
            // No local DB entry
            if local
                .metadata()
                .map(|m| m.file_type().is_dir())
                .unwrap_or(false)
            {
                Ok(false)
            } else {
                Err(anyhow!("unknown local file, no remote metadata"))
            }
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

fn open_file(path: &str) -> anyhow::Result<Option<File>> {
    match File::open(path) {
        Ok(f) => return Ok(Some(f)),
        Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e.into()),
        _ => (), // continue to case-insentive lookup
    }

    let mut cur = PathBuf::from(".");

    'component: for component in path.split('/') {
        for entry in fs::read_dir(&cur).with_context(|| format!("unable to open dir {cur:?}"))? {
            let entry = entry?;
            if entry.file_name().eq_ignore_case(component) {
                cur = entry.path();
                continue 'component;
            }
        }
        return Ok(None);
    }

    Ok(Some(
        File::open(&cur).with_context(|| format!("failed to open file {cur:?}"))?,
    ))
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
        fs::create_dir(&cur).with_context(|| format!("failed to create dir at {cur:?}"))?;
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

trait StrExt {
    fn strip_prefix_case_insensitive(&self, prefix: &str) -> Option<&'_ str>;
    fn eq_ignore_case(&self, other: &str) -> bool;
}

impl StrExt for str {
    fn strip_prefix_case_insensitive(&self, prefix: &str) -> Option<&'_ str> {
        let mut pfx_it = prefix.chars().map(dbx_tolower);
        let mut last = None;
        let t = self.trim_start_matches(|c: char| {
            last = pfx_it.next();
            last == Some(dbx_tolower(c))
        });
        if last.is_some() {
            None
        } else {
            Some(t)
        }
    }

    fn eq_ignore_case(&self, other: &str) -> bool {
        self.chars().map(dbx_tolower).eq(other.chars().map(dbx_tolower))
    }
}

impl StrExt for OsString {
    fn strip_prefix_case_insensitive(&self, prefix: &str) -> Option<&'_ str> {
        self.to_str().and_then(|s| s.strip_prefix_case_insensitive(prefix))
    }

    fn eq_ignore_case(&self, other: &str) -> bool {
        self.to_str().map(|s| s.eq_ignore_case(other)).unwrap_or(false)
    }
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    Output::init(&args.common);

    let db = if let Operation::Setup(setup_args) = args.op {
        return setup(setup_args, &args.db);
    } else {
        Database::open(DATABASE_PATH, &args.db)?
    };

    if db.config_opt("remote_path")?.is_none() {
        bail!("Sync directory not set up yet. Please run 'dbxmirror setup' first.");
    }

    match args.op {
        Operation::Setup(_) => unreachable!(),
        Operation::Pull(pull_args) => pull(pull_args, &db)?,
        Operation::Ignore(ignore_args) => ignore(ignore_args, &db)?,
        Operation::Check => check(&db)?,
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use crate::StrExt;

    #[test]
    fn test_strext() {
        assert_eq!(Some("_SUFFIX"), "SİX_SUFFIX".strip_prefix_case_insensitive("six"));
        assert_eq!(None, "ABC".strip_prefix_case_insensitive("abcd"));
        assert_eq!(Some("ABC"), "ABC".strip_prefix_case_insensitive(""));
        assert!("Ⓗİ THÉRE".eq_ignore_case("ⓗi thére"));
        assert!(!"ABCD".eq_ignore_case("abcde"));
        assert!("".eq_ignore_case(""));
        assert!(!"x".eq_ignore_case(""));
        assert!(!"".eq_ignore_case("x"));
    }
}
