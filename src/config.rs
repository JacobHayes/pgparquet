use crate::GcsSource;

#[derive(Debug, Clone)]
pub struct Config {
    pub batch_size: usize,
    pub create_table: bool,
    pub gcs_source: GcsSource,
    pub table: String,
    pub truncate: bool,
}
