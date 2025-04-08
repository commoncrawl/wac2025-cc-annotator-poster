use clap::Parser;

mod annotators;
mod cli;
mod parquet;

#[tokio::main]
async fn main() {
    let cli = cli::Cli::parse();
    annotators::annotate(&cli.src, &cli.dst, cli.threads).await;
}
