use clap::Parser;
use clap_wrapper::clap_wrapper;

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
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    println!("{args:#?}");
    Ok(())
}
