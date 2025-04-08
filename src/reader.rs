/// WARC reader using [MultiGzDecoder] over a [File].
impl Warc<BufReader<MultiGzDecoder<File>>> {
    /// Create a new reader from a gzipped WET file.
    pub fn from_path_gzip<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
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
