use anyhow::{Context, Result, anyhow};
use futures::StreamExt;
use object_store::{ObjectStore, path::Path};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use std::sync::Arc;
use tokio_postgres::Client as PgClient;
use tracing::{debug, info, warn};

pub mod config;
pub mod postgres_writer;
pub mod schema_mapper;

pub use crate::config::Config;
pub use crate::postgres_writer::PostgresWriter;

#[derive(Debug, Clone)]
pub enum GcsSource {
    File { bucket: String, path: String },
    Prefix { bucket: String, prefix: String },
}

impl GcsSource {
    pub fn bucket(&self) -> &str {
        match self {
            GcsSource::File { bucket, .. } => bucket,
            GcsSource::Prefix { bucket, .. } => bucket,
        }
    }

    pub fn parse_url(url: &str) -> Result<GcsSource> {
        if !url.starts_with("gs://") {
            return Err(anyhow!("GCS URL must start with 'gs://'. Got: {}", url));
        }

        let parts: Vec<&str> = url[5..].splitn(2, '/').collect();
        if parts.len() != 2 {
            return Err(anyhow!(
                "Invalid GCS URL format. Expected 'gs://bucket/path'. Got: {}",
                url
            ));
        }

        let bucket = parts[0].to_string();
        let path_or_prefix = parts[1].to_string();

        if bucket.is_empty() {
            return Err(anyhow!("Bucket name cannot be empty in GCS URL: {}", url));
        }

        // Determine if this is a file or prefix based on the URL pattern
        if path_or_prefix.ends_with(".parquet") {
            Ok(GcsSource::File {
                bucket,
                path: path_or_prefix,
            })
        } else if path_or_prefix.ends_with('/') {
            Ok(GcsSource::Prefix {
                bucket,
                prefix: path_or_prefix,
            })
        } else {
            Err(anyhow!(
                "GCS URL must end with '.parquet' for files or '/' for prefixes. Got: {}",
                url
            ))
        }
    }
}

pub async fn process_data(
    gcs_client: Arc<dyn ObjectStore>,
    pg_client: PgClient,
    config: Config,
) -> Result<()> {
    let parquet_files = match &config.gcs_source {
        GcsSource::File { path, .. } => vec![path.clone()],
        GcsSource::Prefix { prefix, .. } => list_parquet_files(&gcs_client, prefix).await?,
    };
    if parquet_files.is_empty() {
        warn!("No parquet files found at {:?}", config.gcs_source);
        return Ok(());
    };

    info!("Importing data into table: {}", config.table);
    let mut postgres_writer = PostgresWriter::new(pg_client, config.clone()).await?;
    let mut schema_initialized = false;
    for file_path in parquet_files {
        info!("Processing file: {}", file_path);

        let path = Path::from(file_path.as_str());
        let object = gcs_client
            .get(&path)
            .await
            .context("Failed to get object from GCS")?;

        let bytes = object
            .bytes()
            .await
            .context("Failed to read object bytes")?;
        let cursor = std::io::Cursor::new(bytes);

        let builder = ParquetRecordBatchStreamBuilder::new(cursor)
            .await
            .context("Failed to create parquet stream builder")?;

        // Initialize schema on first file
        if !schema_initialized {
            let arrow_schema = builder.schema();
            postgres_writer
                .initialize_schema(arrow_schema.clone())
                .await?;
            schema_initialized = true;
        }

        let stream = builder
            .with_batch_size(config.batch_size)
            .build()
            .context("Failed to build parquet stream")?;

        tokio::pin!(stream);
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.context("Failed to read record batch")?;
            postgres_writer.write_batch(&batch).await?;
        }
    }

    postgres_writer.finalize().await?;
    info!(
        "Data load completed. Total rows written: {}",
        postgres_writer.rows_written
    );
    Ok(())
}

pub async fn list_parquet_files(
    gcs_client: &Arc<dyn ObjectStore>,
    prefix: &str,
) -> Result<Vec<String>> {
    let mut files = Vec::new();

    info!("Listing objects in GCS with prefix: {}", prefix);
    let prefix_path = Path::from(prefix);
    let mut list_stream = gcs_client.list(Some(&prefix_path));

    let mut total_objects = 0;
    while let Some(meta) = list_stream.next().await {
        let meta = meta.context("Failed to get object metadata")?;
        let path = meta.location.as_ref();
        total_objects += 1;

        if path.ends_with(".parquet") {
            debug!("Found parquet file: {}", path);
            files.push(path.to_string());
        } else {
            debug!("Skipping non-parquet file: {}", path);
        }
    }

    info!(
        "Scanned {} total objects, found {} parquet files",
        total_objects,
        files.len()
    );
    files.sort();
    Ok(files)
}
