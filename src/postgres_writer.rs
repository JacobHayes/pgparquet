use anyhow::{Context, Result, anyhow};
use arrow::array::*;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use futures::SinkExt;
use std::sync::Arc;
use tokio_postgres::Client as PgClient;
use tracing::{debug, info};

use crate::config::Config;
use crate::schema_mapper::SchemaMapper;

pub struct PostgresWriter {
    pub rows_written: u64,

    client: PgClient,
    config: Config,
    copy_data: String,
    schema: Option<Arc<Schema>>,
}

impl PostgresWriter {
    pub async fn new(client: PgClient, config: Config) -> Result<Self> {
        Ok(Self {
            client,
            config,
            schema: None,
            rows_written: 0,
            copy_data: String::new(),
        })
    }

    pub async fn initialize_schema(&mut self, schema: Arc<Schema>) -> Result<()> {
        debug!("Initializing schema for table: {}", self.config.table);

        // Store the schema
        self.schema = Some(schema.clone());

        // Create table if requested
        if self.config.create_table {
            let ddl = SchemaMapper::arrow_to_postgres_ddl(&schema, &self.config.table)?;
            debug!("Creating table with DDL: {}", ddl);

            self.client
                .execute(&ddl, &[])
                .await
                .context("Failed to create table")?;
            info!("Created table: {}", self.config.table);
        }

        // Truncate table if requested
        if self.config.truncate {
            let truncate_sql = format!("TRUNCATE TABLE {} RESTART IDENTITY", self.config.table);
            self.client
                .execute(&truncate_sql, &[])
                .await
                .context("Failed to truncate table")?;
            info!("Truncated table: {}", self.config.table);
        }

        Ok(())
    }

    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let _schema = self.schema.as_ref().context("Schema not initialized")?;

        let num_rows = batch.num_rows();
        debug!("Writing batch with {} rows", num_rows);

        // Convert batch to tab-separated format for COPY
        for row_idx in 0..num_rows {
            let mut row_values = Vec::new();

            for column in batch.columns().iter() {
                let value = self.arrow_value_to_copy_string(column, row_idx)?;
                row_values.push(value);
            }

            self.copy_data.push_str(&row_values.join("\t"));
            self.copy_data.push('\n');
        }

        self.rows_written += num_rows as u64;
        debug!(
            "Successfully buffered {} rows (total: {})",
            num_rows, self.rows_written
        );

        // Flush once we have 5MiB+ of data
        if self.copy_data.len() > 1024 * 1024 * 5 {
            self.flush_copy_data().await?;
        }

        Ok(())
    }

    pub async fn finalize(&mut self) -> Result<()> {
        // Flush any remaining data
        if !self.copy_data.is_empty() {
            self.flush_copy_data().await?;
        }
        Ok(())
    }

    async fn flush_copy_data(&mut self) -> Result<()> {
        if self.copy_data.is_empty() {
            return Ok(());
        }

        let schema = self.schema.as_ref().context("Schema not initialized")?;
        let copy_sql = SchemaMapper::create_copy_statement(schema, &self.config.table);

        debug!("Flushing {} bytes of COPY data", self.copy_data.len());

        let copy_sink = self
            .client
            .copy_in(&copy_sql)
            .await
            .context("Failed to start COPY operation")?;

        let mut pinned_sink = std::pin::pin!(copy_sink);

        pinned_sink
            .send(bytes::Bytes::from(self.copy_data.clone()))
            .await
            .context("Failed to send data to COPY")?;

        pinned_sink
            .close()
            .await
            .context("Failed to complete COPY operation")?;

        debug!("Successfully flushed COPY data");
        self.copy_data.clear();
        Ok(())
    }

    fn arrow_value_to_copy_string(&self, array: &dyn Array, row_idx: usize) -> Result<String> {
        if array.is_null(row_idx) {
            return Ok("\\N".to_string()); // PostgreSQL NULL representation in COPY format
        }

        match array.data_type() {
            DataType::Boolean => {
                let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                Ok(if array.value(row_idx) { "t" } else { "f" }.to_string())
            }
            DataType::Int8 => {
                let array = array.as_any().downcast_ref::<Int8Array>().unwrap();
                Ok(array.value(row_idx).to_string())
            }
            DataType::Int16 => {
                let array = array.as_any().downcast_ref::<Int16Array>().unwrap();
                Ok(array.value(row_idx).to_string())
            }
            DataType::Int32 => {
                let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                Ok(array.value(row_idx).to_string())
            }
            DataType::Int64 => {
                let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(array.value(row_idx).to_string())
            }
            DataType::UInt8 => {
                let array = array.as_any().downcast_ref::<UInt8Array>().unwrap();
                Ok(array.value(row_idx).to_string())
            }
            DataType::UInt16 => {
                let array = array.as_any().downcast_ref::<UInt16Array>().unwrap();
                Ok(array.value(row_idx).to_string())
            }
            DataType::UInt32 => {
                let array = array.as_any().downcast_ref::<UInt32Array>().unwrap();
                Ok(array.value(row_idx).to_string())
            }
            DataType::UInt64 => {
                let array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                Ok(array.value(row_idx).to_string())
            }
            DataType::Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                Ok(array.value(row_idx).to_string())
            }
            DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(array.value(row_idx).to_string())
            }
            DataType::Utf8 => {
                let array = array.as_any().downcast_ref::<StringArray>().unwrap();
                Ok(self.escape_copy_string(array.value(row_idx)))
            }
            DataType::LargeUtf8 => {
                let array = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                Ok(self.escape_copy_string(array.value(row_idx)))
            }
            DataType::Binary => {
                let array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                Ok(format!("\\\\x{}", hex::encode(array.value(row_idx))))
            }
            DataType::LargeBinary => {
                let array = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                Ok(format!("\\\\x{}", hex::encode(array.value(row_idx))))
            }
            DataType::Date32 => {
                let array = array.as_any().downcast_ref::<Date32Array>().unwrap();
                let days = array.value(row_idx);
                let date = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                    .unwrap()
                    .checked_add_days(chrono::Days::new(days as u64))
                    .context("Invalid date32 value")?;
                Ok(date.format("%Y-%m-%d").to_string())
            }
            DataType::Date64 => {
                let array = array.as_any().downcast_ref::<Date64Array>().unwrap();
                let millis = array.value(row_idx);
                let timestamp = chrono::DateTime::from_timestamp_millis(millis)
                    .context("Invalid date64 value")?;
                Ok(timestamp.date_naive().format("%Y-%m-%d").to_string())
            }
            DataType::Timestamp(_unit, _) => {
                let array = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                let nanos = array.value(row_idx);
                let timestamp = chrono::DateTime::from_timestamp_nanos(nanos);
                Ok(timestamp.format("%Y-%m-%d %H:%M:%S%.f").to_string())
            }
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::Struct(_)
            | DataType::Map(_, _) => {
                // For complex types, convert to JSON string and escape
                let json_str = format!("{:?}", array.slice(row_idx, 1));
                Ok(self.escape_copy_string(&json_str))
            }
            _ => Err(anyhow!(
                "Unsupported Arrow data type for COPY conversion: {:?}",
                array.data_type()
            )),
        }
    }

    fn escape_copy_string(&self, s: &str) -> String {
        s.replace('\\', "\\\\")
            .replace('\n', "\\n")
            .replace('\r', "\\r")
            .replace('\t', "\\t")
    }
}
