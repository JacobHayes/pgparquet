use anyhow::{Context, Result};
use clap::Parser;
use native_tls::TlsConnector;
use object_store::gcp::GoogleCloudStorageBuilder;
use postgres_native_tls::MakeTlsConnector;
use std::sync::Arc;
use tokio_postgres::NoTls;
use tracing::{debug, error};

use pgparquet::{Config, GcsSource, process_data};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// GCS path - end with '.parquet' for a single file (eg: gs://bucket/file.parquet) or '/' for a folder (eg: gs://bucket/folder/)
    #[arg(short, long)]
    path: String,

    /// PostgreSQL connection string
    #[arg(short, long, env)]
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

    /// Enable TLS for database connection
    #[arg(long)]
    tls: bool,

    /// Enable TLS for database connection without certificate validation
    #[arg(long)]
    tls_insecure: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing with default INFO level if RUST_LOG is not set
    let env_filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(env_filter))
        .init();

    let args = Args::parse();

    debug!("Initializing GCS client...");
    let gcs_source = GcsSource::parse_url(&args.path)?;
    let gcs_client = GoogleCloudStorageBuilder::from_env()
        .with_bucket_name(gcs_source.bucket())
        .build()
        .context("Failed to create GCS client - make sure Google Cloud credentials are set up (try 'gcloud auth application-default login')")?;

    debug!("Connecting to PostgreSQL...");
    let pg_client = if args.tls || args.tls_insecure {
        let mut builder = TlsConnector::builder();
        if args.tls_insecure {
            builder.danger_accept_invalid_certs(true);
            builder.danger_accept_invalid_hostnames(true);
        }
        let connector =
            MakeTlsConnector::new(builder.build().context("Failed to build TLS connector")?);
        let (client, connection) = tokio_postgres::connect(&args.database_url, connector)
            .await
            .context("Failed to connect to PostgreSQL with TLS")?;

        // Spawn the TLS connection task
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("PostgreSQL TLS connection error: {}", e);
            }
        });

        client
    } else {
        let (client, connection) = tokio_postgres::connect(&args.database_url, NoTls)
            .await
            .context("Failed to connect to PostgreSQL")?;

        // Spawn the connection task
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("PostgreSQL connection error: {}", e);
            }
        });

        client
    };

    let config = Config {
        batch_size: args.batch_size,
        create_table: args.create_table,
        gcs_source,
        table: args.table,
        truncate: args.truncate,
    };

    let gcs_client = Arc::new(gcs_client);
    process_data(gcs_client, pg_client, config).await?;
    Ok(())
}
