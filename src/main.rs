use clap::Parser;
use clap_wrapper::clap_wrapper;

/// DBX CLI
#[clap_wrapper]
#[derive(Debug, Parser)]
#[clap(version)]
struct Args {
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    println!("{args:#?}");
    Ok(())
}
