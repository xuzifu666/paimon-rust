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

//! Vortex file format implementation.

use crate::spec::RowType;
use crate::Result;

use super::format::FileFormat;
use super::format_options::VortexFormatOptions;
use super::reader::VortexReaderFactory;
use super::validation::validate_row_type;
use super::writer::VortexWriterFactory;

/// Vortex file format implementation.
pub struct VortexFileFormat {
    options: VortexFormatOptions,
}

impl VortexFileFormat {
    /// Create a new Vortex file format with default options.
    pub fn new() -> Self {
        Self {
            options: VortexFormatOptions::default(),
        }
    }

    /// Create a new Vortex file format with custom options.
    pub fn with_options(options: VortexFormatOptions) -> Self {
        Self { options }
    }

    /// Get the format options.
    pub fn options(&self) -> &VortexFormatOptions {
        &self.options
    }

    /// Create a reader factory.
    pub fn create_reader_factory(&self, projected_row_type: RowType) -> VortexReaderFactory {
        VortexReaderFactory::new(projected_row_type)
    }

    /// Create a writer factory.
    pub fn create_writer_factory(&self, row_type: RowType) -> VortexWriterFactory {
        VortexWriterFactory::new(
            row_type,
            self.options.write_batch_size,
            self.options.write_batch_memory,
        )
    }
}

impl Default for VortexFileFormat {
    fn default() -> Self {
        Self::new()
    }
}

impl FileFormat for VortexFileFormat {
    fn name(&self) -> &str {
        "vortex"
    }

    fn validate(&self, row_type: &RowType) -> Result<()> {
        validate_row_type(row_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::*;

    #[test]
    fn test_vortex_file_format() {
        let format = VortexFileFormat::new();
        assert_eq!(format.name(), "vortex");

        // Test validation with supported types
        let row_type = RowType::new(vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
        ]);
        assert!(format.validate(&row_type).is_ok());

        // Test validation with unsupported types
        let row_type = RowType::new(vec![
            DataField::new(0, "map_col".to_string(), DataType::Map(MapType::new(
                DataType::VarChar(VarCharType::new(VarCharType::MAX_LENGTH).unwrap()),
                DataType::Int(IntType::new()),
            ))),
        ]);
        assert!(format.validate(&row_type).is_err());
    }

    #[test]
    fn test_vortex_file_format_options() {
        let options = VortexFormatOptions {
            read_batch_size: 2048,
            write_batch_size: 2048,
            write_batch_memory: 256 * 1024 * 1024, // 256MB
        };
        let format = VortexFileFormat::with_options(options);
        assert_eq!(format.options().read_batch_size, 2048);
        assert_eq!(format.options().write_batch_size, 2048);
        assert_eq!(format.options().write_batch_memory, 256 * 1024 * 1024);
    }
}
