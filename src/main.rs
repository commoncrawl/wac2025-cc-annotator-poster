use clap::Parser;
use nanohtml2text::html2text;
use walkdir::{DirEntry, WalkDir};

// mod annotators;
mod cli;
//mod parquet;
mod errors;
mod reader;

#[tokio::main]
async fn main() {
    let cli = cli::Cli::parse();
    //annotators::annotate(&cli.src, &cli.dst, cli.threads).await;
    let file_paths: Vec<DirEntry> = WalkDir::new(&cli.src)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
        .filter(|e| e.file_name().to_str().unwrap().ends_with(".warc.gz"))
        .collect();
    for file in file_paths {
        let warc = reader::Warc::from_path_gzip(file.path()).unwrap();
        let mut i = 0;
        for record in warc.iter {
            let record = record.unwrap();
            let raw_body = record.body();
            let (headers, body) = match reader::parse_http_response(raw_body) {
                Some((h, b)) => (h, b),
                None => {
                    println!("Error parsing record");
                    continue;
                }
            };
            println!("{:?}", headers);
            println!("{}", html2text(body));
            i += 1;
            if i > 10 {
                break;
            }
        }
        break;
    }
}
