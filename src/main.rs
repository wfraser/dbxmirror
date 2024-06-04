mod db;

use std::{fs, io};
use std::fs::File;
use std::time::SystemTime;
use anyhow::{anyhow, bail, Context};
use clap::Parser;
use clap_wrapper::clap_wrapper;
use dropbox_sdk::default_client::{NoauthDefaultClient, UserAuthDefaultClient};
use dropbox_sdk::files;
use dropbox_sdk::files::{DownloadArg, FileMetadata, ListFolderArg, ListFolderContinueArg, Metadata};
use dropbox_sdk::oauth2::Authorization;
use dropbox_toolbox::ResultExt;
use time::Duration;
use crate::db::Database;

const DATABASE_PATH: &str = "./.dbxcli.db";

/// DBX CLI
#[clap_wrapper]
#[derive(Debug, Parser)]
#[command(version)]
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
    #[arg(short, long)]
    verbose: bool,
}

#[derive(Debug, Clone, Parser)]
enum Operation {
    /// Check the state of the local filesystem.
    Check,

    /// Pull updates from the server and update the local filesystem.
    Pull,

    /// Perform initial setup for a synced directory.
    ///
    /// Prompts for authentication interactively.
    Setup(SetupArgs),
}

#[derive(Debug, Clone, Parser)]
struct SetupArgs {
    #[arg()]
    remote_path: String,

    /// Override the root namespace ID to something else.
    #[arg(long)]
    root_namespace_id: Option<String>,
}

fn setup(args: SetupArgs) -> anyhow::Result<()> {
    let db = Database::open(DATABASE_PATH)?;

    if db.config_opt("auth")?.is_some() {
        bail!("this directory is already configured");
    }

    let mut auth = dropbox_sdk::oauth2::get_auth_from_env_or_prompt();
    auth.obtain_access_token(NoauthDefaultClient::default())?;
    db.set_config("client_id", &auth.client_id)?;
    db.set_config("auth", auth.save().unwrap().as_str())?;

    db.set_config("remote_path", &args.remote_path)?;
    if let Some(nsid) = args.root_namespace_id {
        db.set_config("root_nsid", &nsid)?;
    }

    Ok(())
}

fn pull(db: &Database) -> anyhow::Result<()> {
    let remote_root = db.config("remote_path")?;
    let client = client(db)?;

    let mut page = if let Some(cursor) = db.config_opt("cursor")? {
        files::list_folder_continue(
            &client,
            &ListFolderContinueArg::new(cursor))
            .combine()?
    } else {
        files::list_folder(
            &client,
            &ListFolderArg::new(remote_root.clone())
                .with_recursive(true)
                .with_include_deleted(true))
            .combine()?
    };

    loop {
        for entry in page.entries {
            let path = match &entry {
                Metadata::File(f) => f.path_display.as_ref(),
                Metadata::Deleted(d) => d.path_display.as_ref(),
                Metadata::Folder(_) => continue,
            };
            let path = path
                .ok_or_else(|| anyhow!("missing path_display field from API"))?
                .strip_prefix(&(remote_root.clone() + "/"))
                .ok_or_else(|| anyhow!("remote path {:?} doesn't start with root path {:?}", path, remote_root))?
                .to_owned();

            match entry {
                Metadata::File(remote) => {
                    eprintln!("-> {path}");
                    match fs::metadata(&path) {
                        Ok(local) => {
                            if check_local_file(&path, &local, Some(&remote), db)? {
                                continue;
                            }
                        }
                        Err(e) if e.kind() == io::ErrorKind::NotFound => (),
                        Err(e) => {
                            return Err(e).with_context(|| format!("error checking metadata of local file {path}"));
                        }
                    }
                    eprintln!("downloading");
                    download(remote, &path, db, &client)
                        .with_context(|| path.clone())?;
                }
                Metadata::Deleted(_) => {
                    match fs::metadata(&path) {
                        Ok(local) => {
                            check_local_file(&path, &local, None, db)?;
                            fs::remove_file(&path)
                                .with_context(|| format!("failed to remove local file {path}"))?;
                            db.remove_file(&path)?;
                        }
                        Err(e) if e.kind() == io::ErrorKind::NotFound => {
                            db.remove_file(&path)?;
                        }
                        Err(e) => {
                            return Err(e).with_context(|| format!("error checking metadata of local file {path}"));
                        }
                    }
                }
                Metadata::Folder(_) => (),
            }
        }

        if !page.has_more {
            break;
        }

        page = files::list_folder_continue(
            &client,
            &ListFolderContinueArg::new(page.cursor))
            .combine()?;
    }

    db.set_config("cursor", &page.cursor)?;

    Ok(())
}

fn download(remote: FileMetadata, path: &str, db: &Database, client: &UserAuthDefaultClient) -> anyhow::Result<()> {
    let dl = files::download(
        client,
        &DownloadArg::new(remote.path_lower.unwrap())
            .with_rev(remote.rev),
        None,
        None,
    ).combine()?;

    let content_length = dl.content_length.ok_or_else(|| anyhow!("missing content length"))?;
    let content_hash = dl.result.content_hash.ok_or_else(|| anyhow!("missing content hash"))?;
    let mtime = time::OffsetDateTime::parse(
        &dl.result.client_modified,
        &time::format_description::well_known::Rfc3339
    ).with_context(|| format!("failed to parse timestamp {}", dl.result.client_modified))?;

    if path.contains('/') {
        let dir = path.rsplitn(2, '/').skip(1).next().unwrap();
        println!("make dir {dir}");
        fs::create_dir_all(dir)
            .context("failed to create dirs")?;
    }

    let mut dest = File::create(path)
        .context("failed to create file")?;

    let written = io::copy(
        &mut dl.body.ok_or_else(|| anyhow!("missing body for download"))?,
        &mut dest,
    )
        .context("failed to save file")?;

    if content_length != written {
        bail!("mismatch between file content length ({content_length}) and written ({written})");
    }

    // TODO: verify content hash?

    dest.set_modified(SystemTime::UNIX_EPOCH + Duration::seconds(mtime.unix_timestamp()))
        .context("failed to set modified time on file")?;

    db.set_file(path, mtime.unix_timestamp(), &content_hash)?;

    Ok(())
}

fn check_local_file(path: &str, local: &fs::Metadata, remote: Option<&FileMetadata>, db: &Database) -> anyhow::Result<bool> {
    eprintln!("checking pre-existing local file");
    let (mtime, content_hash) = db.get_file(path)?
        .ok_or_else(|| anyhow!("missing database entry for existing local file {path}"))?;
    if local.modified().with_context(|| format!("failed to stat {path}"))? != mtime {
        bail!("local file modification time mismatch: {path}");
    }
    if let Some(remote) = remote {
        if &content_hash == remote.content_hash.as_ref().unwrap() {
            eprintln!("local file is identical");
            db.set_file(
                path,
                mtime.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as i64,
                &content_hash)?;
            return Ok(true);
        }
    }
    Ok(false)
}

fn client(db: &Database) -> anyhow::Result<UserAuthDefaultClient> {
    Ok(UserAuthDefaultClient::new(
        Authorization::load(
            db.config("client_id")?,
            &db.config("auth")?,
        ).ok_or_else(|| anyhow!("unable to load authorization from db"))?
    ))
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let db = if let Operation::Setup(setup_args) = args.op {
        return setup(setup_args);
    } else {
        Database::open(DATABASE_PATH)?
    };

    if db.config_opt("remote_path")?.is_none() {
        bail!("Sync directory not set up yet. Please run 'dbxcli setup' first.");
    }

    match args.op {
        Operation::Setup(_) => unreachable!(),
        Operation::Pull => pull(&db)?,
        _ => todo!("operation {:?}", args.op),
    }

    Ok(())
}
