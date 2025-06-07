use crate::GcsSource;

#[derive(Debug, Clone)]
pub struct Config {
    pub gcs_source: GcsSource,
    pub table: String,
    pub schema: Option<String>,
    pub batch_size: usize,
    pub create_table: bool,
    pub truncate: bool,
}

impl Config {
    pub fn table_name(&self) -> String {
        if let Some(schema) = &self.schema {
            format!("{}.{}", schema, self.table)
        } else {
            self.table.clone()
        }
    }
}
