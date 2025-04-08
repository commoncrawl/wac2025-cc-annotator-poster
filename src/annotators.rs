use std::path::PathBuf;

use tokio::{sync::Semaphore, task::JoinSet};
use walkdir::{DirEntry, WalkDir};

async fn process_file(file: DirEntry, dst: PathBuf) {
    // Create the output folder if it does not exist
    let mut folder_path = PathBuf::new();
    folder_path.push(dst);
    let lang = file
        .file_name()
        .to_str()
        .unwrap()
        .strip_suffix("_meta.jsonl")
        .unwrap();
    folder_path.push(lang);

    if !folder_path.exists() {
        std::fs::create_dir_all(&folder_path).unwrap();
    }

    let mut record_builder = OscarBuilder::default();

    let jsonl = {
        let file = File::open(file.path()).unwrap();
        BufReader::new(file)
    };

    println!("Processing file: {}", file.file_name().to_str().unwrap());

    let mut part = 1;

    for line in jsonl.lines() {
        let line = line.unwrap();
        let document: Document = serde_json::from_str(&line).unwrap();
        record_builder.append(&document);
        if record_builder.warc_record_id.len() >= 90_000 {
            write_to_parquet(
                RecordBatch::from(record_builder.finish()),
                &folder_path,
                lang,
                part,
            );
            record_builder = OscarBuilder::default();
            part += 1;
        }
    }
    write_to_parquet(
        RecordBatch::from(record_builder.finish()),
        &folder_path,
        lang,
        part,
    );
    println!(
        "Finished processing file: {}",
        file.file_name().to_str().unwrap(),
    );
}

pub async fn annotate(src: &PathBuf, dst: &PathBuf, threads: usize) {
    let file_paths: Vec<DirEntry> = WalkDir::new(src)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
        .filter(|e| e.file_name().to_str().unwrap().ends_with(".warc.gz"))
        .collect();

    let semaphore = Arc::new(Semaphore::new(threads));
    let mut set = JoinSet::new();

    for file in file_paths {
        let dst = dst.clone();
        let semaphore = semaphore.clone();
        set.spawn(async move {
            let _permit = semaphore.acquire().await;
            process_file(file, dst).await;
        });
    }

    while let Some(result) = set.join_next().await {
        result.unwrap();
    }
}
