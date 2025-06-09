# pgparquet

A quick and dirty CLI to read Parquet files from Google Cloud Storage (GCS) and stream into PostgreSQL. Other storage backends may be added in the future.

The [`pg_parquet`](https://github.com/CrunchyData/pg_parquet) extension is great, but cannot be installed on hosted PostgreSQL providers (eg: GCP). DuckDB can read from parquet and write to PostgreSQL, but it doesn't support Google Application Default Credentials (ADC) for authentication, which makes authentication more challenging.

> [!NOTE]
> This project is a prototype as I learn Rust - there may be bugs or inefficiencies. Feel free to contribute!

## Features

- **Streaming Processing**: Efficiently streams large Parquet files without loading them entirely into memory
- **High-Performance COPY**: Uses PostgreSQL's COPY command for optimal bulk loading performance
- **Automatic Schema Mapping**: Converts Arrow/Parquet schemas to PostgreSQL table schemas
- **Batch Processing**: Configurable batch size for optimal performance
- **Table Management**: Can create tables automatically and optionally truncate before loading
- **Error Handling**: Comprehensive error handling with detailed logging
- **Authentication**: Uses Google Application Default Credentials (ADC)

## Prerequisites

1. **Rust**: Install Rust from [rustup.rs](https://rustup.rs/)
2. **PostgreSQL**: Access to a PostgreSQL database
3. **Google Cloud Authentication**: Set up [Application Default Credentials](https://cloud.google.com/docs/authentication/provide-credentials-adc) (automatic when running in GCP)

## Installation

```bash
cd pgparquet
cargo build --release
```

## Usage

### Basic Usage

Create a new table and load a single parquet file:
```bash
pgparquet \
  --path gs://my-bucket/data/single-file.parquet \
  --database-url "postgresql://user:password@localhost:5432/mydb" \
  --table analytics.my_table \
  --create-table
```

Wipe a table and load all parquet files from a folder:
```bash
pgparquet \
  --path gs://my-bucket/data/parquet-files/ \
  --database-url "postgresql://user:password@localhost:5432/mydb" \
  --table analytics.my_table \
  --truncate
```

### Command Line Options

- `--path, -p`: GCS path - end with '.parquet' for a single file or '/' for a folder
  - Examples: `gs://bucket/file.parquet` or `gs://bucket/folder/`
- `--database-url, -d`: PostgreSQL connection string (required)
- `--table, -t`: Target table name in PostgreSQL (can include schema: schema.table)
- `--batch-size`: Number of records to process in each batch (default: 1000)
- `--create-table`: Create the table if it doesn't exist
- `--truncate`: Truncate the table before loading data

### Environment Variables

You can also use environment variables for sensitive information or the log level:

```bash
export DATABASE_URL="postgresql://user:password@localhost:5432/mydb"
export RUST_LOG=info  # Set logging level
```

## Data Type Mapping

The tool automatically maps Arrow/Parquet data types to PostgreSQL types:

| Arrow/Parquet Type | PostgreSQL Type |
|-------------------|-----------------|
| Boolean | BOOLEAN |
| Int8, Int16 | SMALLINT |
| Int32 | INTEGER |
| Int64 | BIGINT |
| UInt64 | NUMERIC |
| Float32 | REAL |
| Float64 | DOUBLE PRECISION |
| Utf8, LargeUtf8 | TEXT |
| Binary, LargeBinary | BYTEA |
| Date32, Date64 | DATE |
| Time32, Time64 | TIME |
| Timestamp | TIMESTAMP |
| Decimal128, Decimal256 | NUMERIC |
| List, Struct, Map | JSONB |

## Performance Considerations

- **COPY Command**: The tool uses PostgreSQL's COPY command which is significantly faster than INSERT statements for bulk loading
- **Batch Size**: Larger batch sizes can improve performance but use more memory
- **Buffer Size**: Data is buffered in 5MiB chunks before being sent via COPY
- **Network**: Ensure good network connectivity between your application and both GCS and PostgreSQL
- **PostgreSQL Configuration**: Consider adjusting PostgreSQL settings for bulk loading:
  - `shared_buffers`
  - `maintenance_work_mem`
  - `checkpoint_segments`

## Troubleshooting

### Authentication Issues

1. Verify your Google Cloud credentials:
   ```bash
   gcloud auth application-default print-access-token
   ```

2. Check your user or service account has the necessary permissions:
   - `storage.objects.get`
   - `storage.objects.list`

### PostgreSQL Connection Issues

1. Verify connection string format:
   ```
   postgresql://[user[:password]@][host][:port][/dbname][?param1=value1&...]
   ```

2. Test connection manually:
   ```bash
   psql "postgresql://user:password@host:5432/dbname"
   ```

### Performance Issues

1. Monitor memory usage with larger batch sizes
2. Check network latency to both GCS and PostgreSQL
3. Consider running closer to your data (same region)

## License

This project is licensed under the MIT License.
