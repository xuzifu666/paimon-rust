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

use crate::error::Result;
use crate::io::FileIO;
use serde::{Deserialize, Serialize};

/// Consumer which contains next snapshot.
///
/// Reference: <https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/consumer/Consumer.java>
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Consumer {
    #[serde(rename = "nextSnapshot")]
    next_snapshot: i64,
}

impl Consumer {
    /// Create a new consumer with the given next snapshot ID.
    pub fn new(next_snapshot: i64) -> Self {
        Self { next_snapshot }
    }

    /// Get the next snapshot ID.
    pub fn next_snapshot(&self) -> i64 {
        self.next_snapshot
    }

    /// Serialize consumer to JSON string.
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|e| crate::error::Error::DataInvalid {
            message: format!("Failed to serialize consumer: {}", e),
            source: Some(Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
        })
    }

    /// Deserialize consumer from JSON string.
    pub fn from_json(json: &str) -> Result<Self> {
        serde_json::from_str(json).map_err(|e| crate::error::Error::DataInvalid {
            message: format!("Failed to deserialize consumer: {}", e),
            source: Some(Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
        })
    }

    /// Read consumer from a file path with retry mechanism.
    ///
    /// This implements a retry mechanism similar to the Java version to handle
    /// concurrent write scenarios.
    pub async fn from_path(file_io: &FileIO, path: &str) -> Result<Option<Self>> {
        let input_file = file_io.new_input(path)?;

        let mut last_error = None;
        for _ in 0..10 {
            match input_file.read().await {
                Ok(bytes) => {
                    let json = String::from_utf8(bytes.to_vec()).map_err(|e| {
                        crate::error::Error::DataInvalid {
                            message: format!("Consumer file is not valid UTF-8: {}", e),
                            source: Some(Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
                        }
                    })?;
                    return Ok(Some(Self::from_json(&json)?));
                }
                Err(e) => {
                    // Check if file doesn't exist
                    // Handle different error messages across platforms (Windows/Linux/macOS)
                    let error_str = e.to_string().to_lowercase();
                    if error_str.contains("not found")
                        || error_str.contains("no such file")
                        || error_str.contains("is not found")
                        || error_str.contains("notexist")
                        || error_str.contains("does not exist")
                        || error_str.contains("invalid")
                    // Windows may return "invalid filename"
                    {
                        return Ok(None);
                    }
                    last_error = Some(e);
                    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                }
            }
        }

        Err(
            last_error.unwrap_or_else(|| crate::error::Error::DataInvalid {
                message: format!("Failed to read consumer from {} after 10 attempts", path),
                source: Some(Box::new(std::io::Error::other("All retry attempts failed"))
                    as Box<dyn std::error::Error + Send + Sync>),
            }),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consumer_serialization() {
        let consumer = Consumer::new(5);
        let json = consumer.to_json().unwrap();
        assert!(json.contains("nextSnapshot"));
        assert!(json.contains("5"));

        let deserialized = Consumer::from_json(&json).unwrap();
        assert_eq!(consumer, deserialized);
    }

    #[test]
    fn test_consumer_next_snapshot() {
        let consumer = Consumer::new(10);
        assert_eq!(consumer.next_snapshot(), 10);
    }

    #[test]
    fn test_consumer_invalid_json() {
        let result = Consumer::from_json("invalid json");
        assert!(result.is_err());
    }
}
