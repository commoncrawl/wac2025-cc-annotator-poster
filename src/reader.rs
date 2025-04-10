use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
    str,
};

use crate::errors::AnnotatetorError;
use flate2::read::MultiGzDecoder;
use httparse::{EMPTY_HEADER, Response};
use warc::{RecordIter, WarcReader};

/// WARC/Shard instance, generic over reader type.
///
///
/// Be aware that CommonCrawl files are gzipped and need
/// a multi gz decoder (such as [MultiGzDecoder]).
pub struct Warc<T> {
    pub iter: RecordIter<T>,
}

// pub struct RecordIter<T: Iterator<Item = BufReader<MultiGzDecoder<File>>>> {
// pub struct RecordIter<T: Iterator<Item = Result<(RawRecordHeader, std::vec::Vec<u8>), warc::Error>>>
// {
//     iter: T,
// }

/// Wet reader using [MultiGzDecoder] over a [File].
impl Warc<BufReader<MultiGzDecoder<File>>> {
    /// Create a new reader from a gzipped WET file.
    pub fn from_path_gzip<P: AsRef<Path>>(path: P) -> Result<Self, AnnotatetorError> {
        // TODO: ensure that path is dir.
        let gzip_file = File::open(path)?;
        let gzip_stream = MultiGzDecoder::new(gzip_file);

        // we use a different reader from the default one in the warc crate to
        // manage multipart gzipped content.
        let bufreader = BufReader::new(gzip_stream);

        let reader = WarcReader::new(bufreader);

        let x = reader.iter_records();
        Ok(Self { iter: x })
    }
}

#[allow(dead_code)]
impl<T: BufRead> Warc<T> {
    pub fn new(reader: T) -> Self {
        let reader = WarcReader::new(reader);
        let iter = reader.iter_records();
        Self { iter }
    }
}

pub fn parse_http_response(raw: &[u8]) -> Option<(Vec<(String, String)>, &str)> {
    let mut headers = [EMPTY_HEADER; 64];
    let mut res = Response::new(&mut headers);

    let status = res.parse(raw).ok()?;
    let header_len = match status {
        httparse::Status::Complete(len) => len,
        _ => return None,
    };

    let header_map = res
        .headers
        .iter()
        .map(|h| {
            (
                h.name.to_string(),
                String::from_utf8_lossy(h.value).into_owned(),
            )
        })
        .collect();

    let body = str::from_utf8(&raw[header_len..]).ok()?;
    Some((header_map, body))
}
