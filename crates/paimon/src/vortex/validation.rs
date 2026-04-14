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

//! Type validation for Vortex format.

use crate::spec::{DataType, RowType};
use crate::Result;

/// Validates if the given row type is supported by Vortex format.
pub fn validate_row_type(row_type: &RowType) -> Result<()> {
    for field in row_type.fields() {
        validate_data_type(field.data_type())?;
    }
    Ok(())
}

fn validate_data_type(data_type: &DataType) -> Result<()> {
    match data_type {
        // Note: Variant and Blob types are not supported in this Paimon implementation
        DataType::Multiset(_) => Err(crate::Error::Unsupported {
            message: "Vortex file format does not support type MULTISET".to_string(),
        }),
        DataType::Map(_) => Err(crate::Error::Unsupported {
            message: "Vortex file format does not support type MAP".to_string(),
        }),
        DataType::Array(a) => validate_data_type(a.element_type()),
        DataType::Row(r) => validate_row_type(r),
        _ => Ok(()), // All other types are supported
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::*;

    #[test]
    fn test_validate_supported_types() {
        // Test primitive types
        let row_type = RowType::new(vec![
            DataField::new(0, "int_col".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "string_col".to_string(), DataType::VarChar(VarCharType::new(VarCharType::MAX_LENGTH).unwrap())),
        ]);
        assert!(validate_row_type(&row_type).is_ok());

        // Test array type
        let row_type = RowType::new(vec![
            DataField::new(0, "array_col".to_string(), DataType::Array(ArrayType::new(DataType::Int(IntType::new())))),
        ]);
        assert!(validate_row_type(&row_type).is_ok());
    }

    #[test]
    fn test_validate_unsupported_types() {
        // Test multiset type
        let row_type = RowType::new(vec![
            DataField::new(0, "multiset_col".to_string(), DataType::Multiset(MultisetType::new(
                DataType::Int(IntType::new()),
            ))),
        ]);
        assert!(validate_row_type(&row_type).is_err());

        // Test map type
        let row_type = RowType::new(vec![
            DataField::new(0, "map_col".to_string(), DataType::Map(MapType::new(
                DataType::VarChar(VarCharType::new(VarCharType::MAX_LENGTH).unwrap()),
                DataType::Int(IntType::new()),
            ))),
        ]);
        assert!(validate_row_type(&row_type).is_err());
    }
}
