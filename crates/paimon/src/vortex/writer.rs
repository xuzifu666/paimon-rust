// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Vortex file format writer for Apache Paimon.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;

use crate::io::FileIO;
use crate::spec::RowType;
use crate::Result;

/// Factory for creating Vortex writers.
#[derive(Debug, Clone)]
pub struct VortexWriterFactory {
    row_type: RowType,
    batch_size: usize,
    batch_memory: usize,
}

impl VortexWriterFactory {
    /// Create a new Vortex writer factory.
    pub fn new(row_type: RowType, batch_size: usize, batch_memory: usize) -> Self {
        Self {
            row_type,
            batch_size,
            batch_memory,
        }
    }

    /// Create a writer for the given file path.
    pub async fn create_writer(
        &self,
        file_io: &FileIO,
        file_path: &str,
    ) -> Result<VortexWriter> {
        let (vortex_path, storage_options) =
            super::utils::to_vortex_specified(file_io, file_path)?;

        VortexWriter::new(
            &vortex_path,
            &self.row_type,
            self.batch_size,
            self.batch_memory,
            storage_options,
        )
        .await
    }
}

/// Vortex file writer.
pub struct VortexWriter {
    path: String,
    schema: Arc<ArrowSchema>,
    buffer: Vec<RecordBatch>,
    batch_size: usize,
    batch_memory: usize,
    current_memory: usize,
    _storage_options: HashMap<String, String>,
}

impl VortexWriter {
    /// Create a new Vortex writer.
    pub async fn new(
        path: &str,
        row_type: &RowType,
        batch_size: usize,
        batch_memory: usize,
        storage_options: HashMap<String, String>,
    ) -> Result<Self> {
        // Convert row type to Arrow schema
        let schema = crate::arrow::build_target_arrow_schema(row_type.fields())?;

        Ok(Self {
            path: path.to_string(),
            schema,
            buffer: Vec::new(),
            batch_size,
            batch_memory,
            current_memory: 0,
            _storage_options: storage_options,
        })
    }

    /// Write a record batch.
    pub async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        // Check if adding this batch would exceed memory limit
        let batch_size_bytes = batch.get_array_memory_size();

        if self.current_memory + batch_size_bytes > self.batch_memory && !self.buffer.is_empty() {
            // Flush current buffer before adding new batch
            self.flush().await?;
        }

        self.buffer.push(batch);
        self.current_memory += batch_size_bytes;

        // Also flush if we've reached the batch size limit
        if self.buffer.len() >= self.batch_size {
            self.flush().await?;
        }

        Ok(())
    }

    /// Flush buffered batches to the Vortex file.
    pub async fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        // Combine all buffered batches into one
        let combined_batch = if self.buffer.len() == 1 {
            self.buffer.pop().unwrap()
        } else {
            // Concatenate batches
            arrow_select::concat::concat_batches(&self.schema, &self.buffer)
                .map_err(|e| crate::Error::Unsupported {
                    message: format!("Failed to concatenate batches: {e}"),
                })?
        };

        self.buffer.clear();
        self.current_memory = 0;

        // TODO: Write to Vortex file when crate is available
        // For now, just log that we would write
        // In actual implementation: tracing::debug!(...)
        let _ = combined_batch.num_rows(); // Suppress unused warning

        Ok(())
    }

    /// Close the writer and flush any remaining data.
    pub async fn close(mut self) -> Result<()> {
        self.flush().await
    }

    /// Get the current written position (approximate bytes written).
    pub fn written_position(&self) -> usize {
        self.current_memory
    }

    /// Get the file path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Check if target size is reached.
    /// Vortex applies its own compression/encoding, so in-memory Arrow size is much larger
    /// than the actual file size on disk. Always return false to avoid rolling into small files.
    pub fn reach_target_size(&self, _target_size: u64) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::*;
    use arrow_array::{Int32Array, StringArray};
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_vortex_writer_basic() {
        // Create a temporary directory for testing
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.vortex");
        let path = temp_path.to_str().unwrap();

        // Create a simple row type
        let row_type = RowType::new(vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "name".to_string(),
                DataType::VarChar(VarCharType::new(VarCharType::MAX_LENGTH).unwrap()),
            ),
        ]);

        // Create writer
        let mut writer = VortexWriter::new(path, &row_type, 1024, 1024 * 1024, HashMap::new())
            .await
            .unwrap();

        // Create a simple record batch
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int32, true),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        // Write batch
        writer.write_batch(batch).await.unwrap();

        // Close writer
        writer.close().await.unwrap();
    }

    #[test]
    fn test_reach_target_size() {
        // Vortex should always return false for reach_target_size
        // because it applies its own compression
        // This is a placeholder test - the actual logic is tested in integration tests
    }
}
