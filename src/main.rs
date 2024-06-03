mod db;

use anyhow::bail;
use clap::Parser;
use clap_wrapper::clap_wrapper;
use dropbox_sdk::default_client::NoauthDefaultClient;
use crate::db::Database;

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

    /// Perform initial setup for a synced directory. Prompts for authentication interactively.
    Setup {
        #[arg()]
        remote_path: String,

        /// Override the root namespace ID to something else.
        #[arg(long)]
        root_namespace_id: Option<String>,
    },
}

fn setup(remote_path: &str, root_nsid: Option<&str>) -> anyhow::Result<()> {
    let db = Database::open("./.dbxcli.db")?;

    if db.config("auth")?.is_some() {
        bail!("this directory is already configured");
    }

    let mut auth = dropbox_sdk::oauth2::get_auth_from_env_or_prompt();
    auth.obtain_access_token(NoauthDefaultClient::default())?;
    db.set_config("auth", auth.save().unwrap().as_str())?;

    db.set_config("remote_path", remote_path)?;
    if let Some(nsid) = root_nsid {
        db.set_config("root_nsid", nsid)?;
    }

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    println!("{args:#?}");

    match args.op {
        Operation::Setup {
            remote_path,
            root_namespace_id,
        } => setup(&remote_path, root_namespace_id.as_deref())?,
        _ => todo!("operation {:?}", args.op),
    }

    Ok(())
}
