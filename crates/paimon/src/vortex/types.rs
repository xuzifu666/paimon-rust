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

//! Type conversion utilities for Vortex format.
//!
//! This module provides conversion between Paimon types and Vortex DType.

use crate::spec::{DataType, RowType};
use crate::Result;

/// Converts a Paimon RowType to Vortex DType representation.
///
/// This is a placeholder implementation. In a full implementation,
/// this would convert to Vortex's DType type.
pub fn to_dtype(row_type: &RowType) -> Result<String> {
    // For now, we return a JSON-like representation of the schema
    // In the actual implementation with vortex crate, this would return vortex::DType
    let fields: Vec<String> = row_type
        .fields()
        .iter()
        .map(|f| format!("{}:{}", f.name(), data_type_to_string(f.data_type())))
        .collect();
    Ok(format!("struct{{{}}}", fields.join(",")))
}

fn data_type_to_string(data_type: &DataType) -> String {
    match data_type {
        DataType::Boolean(_) => "bool".to_string(),
        DataType::TinyInt(_) => "i8".to_string(),
        DataType::SmallInt(_) => "i16".to_string(),
        DataType::Int(_) => "i32".to_string(),
        DataType::BigInt(_) => "i64".to_string(),
        DataType::Float(_) => "f32".to_string(),
        DataType::Double(_) => "f64".to_string(),
        DataType::Decimal(d) => format!("decimal({},{})", d.precision(), d.scale()),
        DataType::Char(_) | DataType::VarChar(_) => "utf8".to_string(),
        DataType::Binary(_) | DataType::VarBinary(_) => "binary".to_string(),
        DataType::Date(_) => "date".to_string(),
        DataType::Time(_) => "time".to_string(),
        DataType::Timestamp(t) => format!("timestamp({})", t.precision()),
        DataType::LocalZonedTimestamp(t) => format!("timestamp_tz({})", t.precision()),
        DataType::Array(a) => format!("list<{}>", data_type_to_string(a.element_type())),
        // Note: Vector type is not supported in this Paimon implementation
        DataType::Row(r) => {
            let fields: Vec<String> = r
                .fields()
                .iter()
                .map(|f| format!("{}:{}", f.name(), data_type_to_string(f.data_type())))
                .collect();
            format!("struct{{{}}}", fields.join(","))
        }
        _ => "unsupported".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::*;

    #[test]
    fn test_primitive_types() {
        let row_type = RowType::new(vec![
            DataField::new(0, "int_col".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "long_col".to_string(), DataType::BigInt(BigIntType::new())),
            DataField::new(2, "float_col".to_string(), DataType::Float(FloatType::new())),
            DataField::new(3, "double_col".to_string(), DataType::Double(DoubleType::new())),
        ]);

        let dtype = to_dtype(&row_type).unwrap();
        assert!(dtype.contains("int_col:i32"));
        assert!(dtype.contains("long_col:i64"));
        assert!(dtype.contains("float_col:f32"));
        assert!(dtype.contains("double_col:f64"));
    }

    #[test]
    fn test_string_and_binary_types() {
        let row_type = RowType::new(vec![
            DataField::new(
                0,
                "string_col".to_string(),
                DataType::VarChar(VarCharType::new(VarCharType::MAX_LENGTH).unwrap()),
            ),
            DataField::new(
                1,
                "binary_col".to_string(),
                DataType::VarBinary(VarBinaryType::try_new(true, VarBinaryType::MAX_LENGTH).unwrap()),
            ),
        ]);

        let dtype = to_dtype(&row_type).unwrap();
        assert!(dtype.contains("string_col:utf8"));
        assert!(dtype.contains("binary_col:binary"));
    }

    #[test]
    fn test_decimal_type() {
        let row_type = RowType::new(vec![
            DataField::new(
                0,
                "decimal_col".to_string(),
                DataType::Decimal(DecimalType::new(10, 2).unwrap()),
            ),
        ]);

        let dtype = to_dtype(&row_type).unwrap();
        assert!(dtype.contains("decimal_col:decimal(10,2)"));
    }

    #[test]
    fn test_array_type() {
        let row_type = RowType::new(vec![
            DataField::new(
                0,
                "array_col".to_string(),
                DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
            ),
        ]);

        let dtype = to_dtype(&row_type).unwrap();
        assert!(dtype.contains("array_col:list<i32>"));
    }
}
