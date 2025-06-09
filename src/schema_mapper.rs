use anyhow::{Result, anyhow};
use arrow::datatypes::{DataType, Schema};

pub struct SchemaMapper;

impl SchemaMapper {
    pub fn arrow_to_postgres_ddl(schema: &Schema, table_name: &str) -> Result<String> {
        let mut ddl = format!("CREATE TABLE IF NOT EXISTS {} (\n", table_name);

        let mut columns = Vec::new();

        for field in schema.fields() {
            let column_name = Self::sanitize_column_name(field.name());
            let postgres_type = Self::arrow_to_postgres_type(field.data_type())?;
            let nullable = if field.is_nullable() { "" } else { " NOT NULL" };

            columns.push(format!("  {} {}{}", column_name, postgres_type, nullable));
        }

        ddl.push_str(&columns.join(",\n"));
        ddl.push_str("\n);");

        Ok(ddl)
    }

    pub fn create_copy_statement(schema: &Schema, table_name: &str) -> String {
        let column_names: Vec<String> = schema
            .fields()
            .iter()
            .map(|field| Self::sanitize_column_name(field.name()))
            .collect();

        format!(
            "COPY {} ({}) FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t', NULL '\\N')",
            table_name,
            column_names.join(", ")
        )
    }

    fn arrow_to_postgres_type(data_type: &DataType) -> Result<&'static str> {
        match data_type {
            DataType::Boolean => Ok("BOOLEAN"),
            DataType::Int8 => Ok("SMALLINT"),
            DataType::Int16 => Ok("SMALLINT"),
            DataType::Int32 => Ok("INTEGER"),
            DataType::Int64 => Ok("BIGINT"),
            DataType::UInt8 => Ok("SMALLINT"),
            DataType::UInt16 => Ok("INTEGER"),
            DataType::UInt32 => Ok("BIGINT"),
            DataType::UInt64 => Ok("NUMERIC"),
            DataType::Float16 => Ok("REAL"),
            DataType::Float32 => Ok("REAL"),
            DataType::Float64 => Ok("DOUBLE PRECISION"),
            DataType::Utf8 => Ok("TEXT"),
            DataType::LargeUtf8 => Ok("TEXT"),
            DataType::Binary => Ok("BYTEA"),
            DataType::LargeBinary => Ok("BYTEA"),
            DataType::Date32 => Ok("DATE"),
            DataType::Date64 => Ok("DATE"),
            DataType::Time32(_) => Ok("TIME"),
            DataType::Time64(_) => Ok("TIME"),
            DataType::Timestamp(_, _) => Ok("TIMESTAMP"),
            DataType::Decimal128(_precision, _scale) => Ok("NUMERIC"),
            DataType::Decimal256(_precision, _scale) => Ok("NUMERIC"),
            DataType::List(_) => Ok("JSONB"),
            DataType::LargeList(_) => Ok("JSONB"),
            DataType::Struct(_) => Ok("JSONB"),
            DataType::Map(_, _) => Ok("JSONB"),
            _ => Err(anyhow!("Unsupported Arrow data type: {:?}", data_type)),
        }
    }

    fn sanitize_column_name(name: &str) -> String {
        // Replace invalid characters and ensure it starts with a letter or underscore
        let mut sanitized = name
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect::<String>();

        // Ensure it starts with a letter or underscore
        if !sanitized.starts_with(|c: char| c.is_alphabetic() || c == '_') {
            sanitized = format!("col_{}", sanitized);
        }

        // Convert to lowercase for PostgreSQL convention
        sanitized.to_lowercase()
    }
}
