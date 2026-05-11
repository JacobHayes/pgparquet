use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use arrow::{
    array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use object_store::local::LocalFileSystem;
use parquet::arrow::ArrowWriter;
use pgparquet::{Config, GcsSource, process_data};
use testcontainers_modules::{
    postgres,
    testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
};
use tokio_postgres::{Client, NoTls};

static FIXTURES: std::sync::OnceLock<Result<PathBuf, String>> = std::sync::OnceLock::new();

#[tokio::test]
async fn loads_single_parquet_file_into_postgres() -> Result<()> {
    let fixture_root = fixture_root()?;
    let Some((_container, database_url)) = start_postgres_or_skip().await? else {
        return Ok(());
    };
    let writer_client = connect(&database_url).await?;
    let query_client = connect(&database_url).await?;

    process_data(
        Arc::new(LocalFileSystem::new_with_prefix(&fixture_root)?),
        writer_client,
        Config {
            batch_size: 2,
            create_table: true,
            gcs_source: GcsSource::File {
                bucket: "local".to_string(),
                path: "single.parquet".to_string(),
            },
            table: "public.single_load".to_string(),
            truncate: false,
        },
    )
    .await?;

    let rows = query_client
        .query(
            "SELECT id, name, active, score FROM public.single_load ORDER BY id",
            &[],
        )
        .await?;

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    assert_eq!(
        rows[0].get::<_, Option<String>>(1),
        Some("alpha".to_string())
    );
    assert!(rows[0].get::<_, bool>(2));
    assert_eq!(rows[0].get::<_, Option<f64>>(3), Some(1.5));

    assert_eq!(rows[1].get::<_, i64>(0), 2);
    assert_eq!(rows[1].get::<_, Option<String>>(1), None);
    assert!(!rows[1].get::<_, bool>(2));
    assert_eq!(rows[1].get::<_, Option<f64>>(3), None);

    assert_eq!(rows[2].get::<_, i64>(0), 3);
    assert_eq!(
        rows[2].get::<_, Option<String>>(1),
        Some("tab\tnewline\nslash\\".to_string())
    );
    assert!(rows[2].get::<_, bool>(2));
    assert_eq!(rows[2].get::<_, Option<f64>>(3), Some(-2.25));

    Ok(())
}

#[tokio::test]
async fn loads_prefix_parquet_files_and_truncates_existing_rows() -> Result<()> {
    let fixture_root = fixture_root()?;
    let Some((_container, database_url)) = start_postgres_or_skip().await? else {
        return Ok(());
    };
    let writer_client = connect(&database_url).await?;
    let query_client = connect(&database_url).await?;

    query_client
        .batch_execute(
            "CREATE TABLE public.prefix_load (id BIGINT NOT NULL, label TEXT);\
             INSERT INTO public.prefix_load (id, label) VALUES (999, 'remove me');",
        )
        .await?;

    process_data(
        Arc::new(LocalFileSystem::new_with_prefix(&fixture_root)?),
        writer_client,
        Config {
            batch_size: 1,
            create_table: false,
            gcs_source: GcsSource::Prefix {
                bucket: "local".to_string(),
                prefix: "prefix/".to_string(),
            },
            table: "public.prefix_load".to_string(),
            truncate: true,
        },
    )
    .await?;

    let rows = query_client
        .query("SELECT id, label FROM public.prefix_load ORDER BY id", &[])
        .await?;

    let loaded: Vec<(i64, Option<String>)> =
        rows.iter().map(|row| (row.get(0), row.get(1))).collect();

    assert_eq!(
        loaded,
        vec![
            (10, Some("first file row one".to_string())),
            (11, Some("first file row two".to_string())),
            (12, Some("second file row".to_string())),
        ]
    );

    Ok(())
}

fn fixture_root() -> Result<PathBuf> {
    FIXTURES
        .get_or_init(|| create_fixtures().map_err(|error| format!("{error:#}")))
        .clone()
        .map_err(anyhow::Error::msg)
}

fn create_fixtures() -> Result<PathBuf> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(".testdata");
    fs::create_dir_all(root.join("prefix"))?;
    fs::write(root.join(".gitignore"), "*\n!.gitignore\n")?;
    fs::write(root.join("prefix/ignored.txt"), "not parquet")?;

    let single_path = root.join("single.parquet");
    if !single_path.exists() {
        write_single_fixture(&single_path)?;
    }

    let prefix_one_path = root.join("prefix/01.parquet");
    if !prefix_one_path.exists() {
        write_prefix_fixture(
            &prefix_one_path,
            vec![
                (10, Some("first file row one")),
                (11, Some("first file row two")),
            ],
        )?;
    }

    let prefix_two_path = root.join("prefix/02.parquet");
    if !prefix_two_path.exists() {
        write_prefix_fixture(&prefix_two_path, vec![(12, Some("second file row"))])?;
    }

    Ok(root)
}

fn write_single_fixture(path: &Path) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("active", DataType::Boolean, false),
        Field::new("score", DataType::Float64, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("alpha"),
                None,
                Some("tab\tnewline\nslash\\"),
            ])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![true, false, true])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(1.5), None, Some(-2.25)])) as ArrayRef,
        ],
    )?;

    write_parquet(path, schema, &[batch])
}

fn write_prefix_fixture(path: &Path, rows: Vec<(i64, Option<&str>)>) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
    ]));

    let ids = rows.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    let labels = rows.iter().map(|(_, label)| *label).collect::<Vec<_>>();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(labels)) as ArrayRef,
        ],
    )?;

    write_parquet(path, schema, &[batch])
}

fn write_parquet(path: &Path, schema: Arc<Schema>, batches: &[RecordBatch]) -> Result<()> {
    let file = File::create(path).with_context(|| format!("creating {}", path.display()))?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.close()?;
    Ok(())
}

async fn start_postgres_or_skip() -> Result<Option<(ContainerAsync<postgres::Postgres>, String)>> {
    if cfg!(target_os = "macos") && std::env::var_os("CI").is_some() {
        eprintln!("skipping testcontainers e2e test on macOS CI because Docker is unavailable");
        return Ok(None);
    }

    let container = postgres::Postgres::default()
        .with_tag("17-alpine")
        .start()
        .await?;

    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let database_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    Ok(Some((container, database_url)))
}

async fn connect(database_url: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("PostgreSQL connection error: {error}");
        }
    });
    Ok(client)
}
