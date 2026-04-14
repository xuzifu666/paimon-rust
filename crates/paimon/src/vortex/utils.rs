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

//! Utilities for Vortex file format support.

use std::collections::HashMap;

use crate::io::FileIO;
use crate::Result;

/// Converts FileIO and path to Vortex-specific path and storage options.
pub fn to_vortex_specified(
    _file_io: &FileIO,
    path: &str,
) -> Result<(String, HashMap<String, String>)> {
    // Parse the path to extract scheme
    let url = url::Url::parse(path).map_err(|e| crate::Error::ConfigInvalid {
        message: format!("Invalid URL: {path}, error: {e}"),
    })?;

    let scheme = url.scheme();
    let storage_options = HashMap::new();
    let mut converted_path = path.to_string();

    // Handle different storage schemes
    match scheme {
        "oss" => {
            // OSS storage: convert oss:// to s3:// and extract options
            // In a full implementation, this would extract credentials from FileIO
            converted_path = path.replace("oss://", "s3://");
        }
        _ => {
            // Default: return path as-is with empty storage options
        }
    }

    Ok((converted_path, storage_options))
}

/// Extracts field names from a row type for projection.
pub fn extract_field_names(row_type: &crate::spec::RowType) -> Vec<String> {
    row_type
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect()
}

/// Converts row indices from RoaringBitmap to a vector of indices.
pub fn row_indices_to_vec(selection: &roaring::RoaringBitmap) -> Vec<u64> {
    selection.iter().map(|i| i as u64).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_vortex_specified_file_path() {
        let file_io = FileIO::from_path("file:///tmp/test").unwrap().build().unwrap();
        let (path, options) = to_vortex_specified(&file_io, "file:///tmp/test.vortex").unwrap();
        assert_eq!(path, "file:///tmp/test.vortex");
        assert!(options.is_empty());
    }

    #[test]
    fn test_to_vortex_specified_oss_path() {
        let file_io = FileIO::from_path("file:///tmp/test").unwrap().build().unwrap();
        let (path, _options) =
            to_vortex_specified(&file_io, "oss://bucket/test.vortex").unwrap();
        assert_eq!(path, "s3://bucket/test.vortex");
    }

    #[test]
    fn test_extract_field_names() {
        use crate::spec::*;

        let row_type = RowType::new(vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "name".to_string(),
                DataType::VarChar(VarCharType::new(VarCharType::MAX_LENGTH).unwrap()),
            ),
        ]);

        let names = extract_field_names(&row_type);
        assert_eq!(names, vec!["id", "name"]);
    }

    #[test]
    fn test_row_indices_to_vec() {
        let mut bitmap = roaring::RoaringBitmap::new();
        bitmap.insert(1);
        bitmap.insert(3);
        bitmap.insert(5);

        let indices = row_indices_to_vec(&bitmap);
        assert_eq!(indices, vec![1, 3, 5]);
    }
}
