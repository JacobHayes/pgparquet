# pgparquet

A quick and dirty CLI to read Parquet files from Google Cloud Storage (GCS) and
stream into PostgreSQL.

The [`pg_parquet`](https://github.com/CrunchyData/pg_parquet) extension is
great, but cannot be installed to all hosted PostgreSQL providers (eg: GCP).
DuckDB can read from parquet and write to PostgreSQL, but it doesn't support
Google Application Default Credentials (ADC) for authentication, which makes
authentication more challenging.

> [!NOTE]
> This project is a prototype as I learn Rust - don't expect production quality code yet, but feel free to contribute!

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
3. **Google Cloud Authentication**: Set up Application Default Credentials

### Setting up Google Cloud Authentication

You can authenticate in several ways:

1. **Service Account Key** (recommended for production):
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
   ```

2. **gcloud CLI** (for development):
   ```bash
   gcloud auth application-default login
   ```

3. **Compute Engine/Cloud Run**: Uses the instance's default service account automatically

## Installation

```bash
cd pgparquet
cargo build --release
```

## Usage

### Basic Usage

Load all parquet files from a folder:
```bash
pgparquet \
  --path gs://my-bucket/data/parquet-files/ \
  --database-url "postgresql://user:password@localhost:5432/mydb" \
  --table my_table \
  --create-table \
  --truncate
```

Load a single parquet file with schema qualification:
```bash
pgparquet \
  --path gs://my-bucket/data/single-file.parquet \
  --database-url "postgresql://user:password@localhost:5432/mydb" \
  --table analytics.my_table \
  --create-table
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

You can also use environment variables for sensitive information:

```bash
export DATABASE_URL="postgresql://user:password@localhost:5432/mydb"
export RUST_LOG=info  # Set logging level
```

Then run:
```bash
pgparquet \
  --path gs://my-bucket/data/parquet-files/ \
  --database-url "$DATABASE_URL" \
  --table my_table \
  --create-table
```

### Examples

#### Load all parquet files from a GCS folder into a new table

```bash
pgparquet \
  --path gs://analytics-data/exports/user-events/ \
  --database-url "postgresql://analytics:secret@db.example.com:5432/warehouse" \
  --table analytics.user_events \
  --create-table \
  --truncate \
  --batch-size 5000
```

#### Load a single file into an existing table

```bash
pgparquet \
  --path gs://ml-datasets/processed/features/features_2024.parquet \
  --database-url "$DATABASE_URL" \
  --table ml_features \
  --batch-size 2000
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
- **Buffer Size**: Data is buffered in 1MB chunks before being sent via COPY for optimal performance
- **Network**: Ensure good network connectivity between your application and both GCS and PostgreSQL
- **PostgreSQL Configuration**: Consider adjusting PostgreSQL settings for bulk loading:
  - `shared_buffers`
  - `maintenance_work_mem`
  - `checkpoint_segments`

## Logging

The application uses structured logging. Set the `RUST_LOG` environment variable to control log levels:

```bash
export RUST_LOG=debug  # For detailed debugging
export RUST_LOG=info   # For general information
export RUST_LOG=warn   # For warnings only
export RUST_LOG=error  # For errors only
```

## Error Handling

The application will:
- Stop processing if it encounters an authentication error
- Skip files that cannot be read and continue with others
- Provide detailed error messages for troubleshooting
- Log progress information for monitoring

## Security

- Uses Google Application Default Credentials - no credentials stored in code
- Supports PostgreSQL SSL connections through connection string parameters
- Column names are sanitized to prevent SQL injection

## Development

### Building

```bash
cargo build
```

### Running Tests

```bash
cargo test
```

### Formatting

```bash
cargo fmt
```

### Linting

```bash
cargo clippy
```

## Troubleshooting

### Authentication Issues

1. Verify your Google Cloud credentials:
   ```bash
   gcloud auth application-default print-access-token
   ```

2. Check your service account has the necessary permissions:
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
