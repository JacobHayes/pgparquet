use anyhow::{Context, Result, anyhow};
use clap::Parser;
use futures::StreamExt;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::{ObjectStore, path::Path};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use std::sync::Arc;
use tokio_postgres::{Client as PgClient, NoTls};
use tracing::{error, info, warn};

mod config;
mod postgres_writer;
mod schema_mapper;

use crate::config::Config;
use crate::postgres_writer::PostgresWriter;

#[derive(Debug, Clone)]
enum GcsSource {
    File { bucket: String, path: String },
    Prefix { bucket: String, prefix: String },
}

impl GcsSource {
    fn bucket(&self) -> &str {
        match self {
            GcsSource::File { bucket, .. } => bucket,
            GcsSource::Prefix { bucket, .. } => bucket,
        }
    }

    fn parse_url(url: &str) -> Result<GcsSource> {
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
            return Err(anyhow!(
                "GCS URL must end with '.parquet' for files or '/' for prefixes. Got: {}",
                url
            ));
        }
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// GCS path - end with '.parquet' for a single file (eg: gs://bucket/file.parquet) or '/' for a folder (eg: gs://bucket/folder/)
    #[arg(short, long)]
    path: String,

    /// PostgreSQL connection string
    #[arg(short, long)]
    database_url: String,

    /// Target table name in PostgreSQL (can include schema: schema.table)
    #[arg(short, long)]
    table: String,

    /// Batch size for processing records
    #[arg(long, default_value = "1000")]
    batch_size: usize,

    /// Whether to create the table if it doesn't exist
    #[arg(long)]
    create_table: bool,

    /// Whether to truncate the table before loading
    #[arg(long)]
    truncate: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing with default INFO level if RUST_LOG is not set
    let env_filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(env_filter))
        .init();

    let args = Args::parse();
    let gcs_source = GcsSource::parse_url(&args.path)?;

    info!("Starting data load from GCS to PostgreSQL");
    info!("Data Source: {:?}", gcs_source);
    info!("Database: {}, Table: {}", args.database_url, args.table);

    info!("Initializing GCS client...");
    let gcs_client = GoogleCloudStorageBuilder::from_env()
        .with_bucket_name(gcs_source.bucket())
        .build()
        .context("Failed to create GCS client - make sure Google Cloud credentials are set up (try 'gcloud auth application-default login')")?;

    info!("Connecting to PostgreSQL...");
    let (pg_client, connection) = tokio_postgres::connect(&args.database_url, NoTls)
        .await
        .context("Failed to connect to PostgreSQL")?;
    // Spawn the connection task
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("PostgreSQL connection error: {}", e);
        }
    });

    let config = Config {
        batch_size: args.batch_size,
        create_table: args.create_table,
        gcs_source,
        table: args.table,
        truncate: args.truncate,
    };

    let gcs_client = Arc::new(gcs_client);
    process_data(gcs_client, pg_client, config).await?;

    info!("Data load completed successfully");
    Ok(())
}

async fn process_data(
    gcs_client: Arc<dyn ObjectStore>,
    pg_client: PgClient,
    config: Config,
) -> Result<()> {
    // Get list of parquet files to process
    let parquet_files = match &config.gcs_source {
        GcsSource::File { path, .. } => {
            info!("Processing single file: {}", path);
            vec![path.clone()]
        }
        GcsSource::Prefix { prefix, .. } => {
            info!("Listing parquet files with prefix: {}", prefix);
            list_parquet_files(&gcs_client, prefix).await?
        }
    };
    if parquet_files.is_empty() {
        warn!("No parquet files found at {:?}", config.gcs_source);
        return Ok(());
    }

    info!("Found {} parquet files to process", parquet_files.len());

    let mut postgres_writer = PostgresWriter::new(pg_client, config.clone()).await?;
    let mut schema_initialized = false;

    // Process each parquet file
    for file_path in parquet_files {
        info!("Processing file: {}", file_path);

        let path = Path::from(file_path.as_str());
        let object = gcs_client
            .get(&path)
            .await
            .context("Failed to get object from GCS")?;

        // Create parquet reader from the object stream
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

        // Stream and process record batches
        tokio::pin!(stream);
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.context("Failed to read record batch")?;
            postgres_writer.write_batch(&batch).await?;
        }

        info!("Completed processing file: {}", file_path);
    }

    postgres_writer.finalize().await?;
    Ok(())
}

async fn list_parquet_files(
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
            info!("Found parquet file: {}", path);
            files.push(path.to_string());
        } else {
            info!("Skipping non-parquet file: {}", path);
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
