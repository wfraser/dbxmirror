mod db;
mod downloader;

use std::{fs, io};
use std::sync::Arc;
use std::time::SystemTime;
use anyhow::{anyhow, bail, Context};
use clap::Parser;
use clap_wrapper::clap_wrapper;
use crossbeam_channel::Receiver;
use dropbox_sdk::common::PathRoot;
use dropbox_sdk::default_client::{NoauthDefaultClient, UserAuthDefaultClient};
use dropbox_sdk::files;
use dropbox_sdk::files::{FileMetadata, GetMetadataArg, ListFolderArg, ListFolderContinueArg, Metadata};
use dropbox_sdk::oauth2::Authorization;
use dropbox_toolbox::ResultExt;
use scopeguard::{guard, ScopeGuard};
use crate::db::Database;
use crate::downloader::{Downloader, DownloadRequest, DownloadResult};

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
        bail!("this directory is already configured; remove {DATABASE_PATH} to setup again");
    }

    let mut auth = dropbox_sdk::oauth2::get_auth_from_env_or_prompt();
    auth.obtain_access_token(NoauthDefaultClient::default())?;

    let client_id = auth.client_id.clone();
    let saved_auth = auth.save().unwrap();

    if args.remote_path != "/" {
        let client = UserAuthDefaultClient::new(auth);
        match files::get_metadata(&client, &GetMetadataArg::new(args.remote_path.clone()))
            .combine()
        {
            Ok(meta) => {
                if matches!(meta, Metadata::File(_)) {
                    bail!("Remote path {:?} refers to a file, not a folder.", args.remote_path);
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

fn complete_downloads(dl_rx: &Receiver<DownloadResult>, db: &Database, block: bool) -> anyhow::Result<()> {
    let mut result = Ok(());
    while let Ok(rx) = if block {
        dl_rx.recv().map_err(|_| ())
    } else {
        dl_rx.try_recv().map_err(|_| ())
    } {
        if let Err(e) = rx.result {
            eprintln!("download error: {e:#}");
            if result.is_ok() {

                // Save the first error.
                result = Err(e);
            }

            // Complete any other pending finished downloads.
            continue;
        }

        eprintln!("completed {}", rx.path);
        db.set_file(&rx.path, rx.mtime, &rx.content_hash)?;
    }
    result
}

fn pull(db: &Database) -> anyhow::Result<()> {
    let remote_root = db.config("remote_path")?;
    let client = client(db)?;

    let mut page = if let Some(cursor) = db.config_opt("cursor")? {
        files::list_folder_continue(
            client.as_ref(),
            &ListFolderContinueArg::new(cursor))
            .combine()?
    } else {
        files::list_folder(
            client.as_ref(),
            &ListFolderArg::new(remote_root.clone())
                .with_recursive(true)
                .with_include_deleted(true))
            .combine()?
    };

    let (_downloader, dl_tx, dl_rx) = Downloader::new(remote_root.clone(), client.clone());
    let dl_tx = guard(dl_tx, |dl_tx| {
        eprintln!("error occurred; waiting for existing downloads");
        drop(dl_tx);
        _ = complete_downloads(&dl_rx, db, true);
    });

    loop {
        for entry in page.entries {
            complete_downloads(&dl_rx, db, false)?;

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

                    let mtime = time::OffsetDateTime::parse(
                        &remote.client_modified,
                        &time::format_description::well_known::Rfc3339
                    )
                        .with_context(|| format!("failed to parse timestamp {}", remote.client_modified))?
                        .unix_timestamp();
                    let content_hash = remote.content_hash.ok_or_else(|| anyhow!("missing content_hash in API metadata"))?;

                    dl_tx.send(DownloadRequest {
                        path,
                        rev: remote.rev,
                        size: remote.size,
                        mtime,
                        content_hash,
                    }).unwrap()
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
            client.as_ref(),
            &ListFolderContinueArg::new(page.cursor))
            .combine()?;
    }

    eprintln!("waiting for downloads");
    drop(ScopeGuard::into_inner(dl_tx));
    complete_downloads(&dl_rx, db, true)?;
    db.set_config("cursor", &page.cursor)?;

    Ok(())
}

fn check_local_file(path: &str, local: &fs::Metadata, remote: Option<&FileMetadata>, db: &Database) -> anyhow::Result<bool> {
    eprintln!("checking pre-existing local file");
    let (mtime, content_hash) = db.get_file(path)?
        .ok_or_else(|| anyhow!("refusing to clobber existing local file {path}"))?;
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

fn client(db: &Database) -> anyhow::Result<Arc<UserAuthDefaultClient>> {
    let mut client = UserAuthDefaultClient::new(
        Authorization::load(
            db.config("client_id")?,
            &db.config("auth")?,
        ).ok_or_else(|| anyhow!("unable to load authorization from db"))?
    );

    if let Some(nsid) = db.config_opt("root_nsid")? {
        client.set_path_root(&PathRoot::NamespaceId(nsid));
    }

    Ok(Arc::new(client))
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
