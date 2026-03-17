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
use crate::io::{FileIO, InputFile};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

/// Consumer represents the consumption progress of a consumer.
///
/// It tracks the next snapshot ID that the consumer should read.
///
/// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/consumer/Consumer.java>
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Consumer {
    /// The next snapshot ID to be consumed.
    next_snapshot: i64,
}

impl Consumer {
    const FIELD_NEXT_SNAPSHOT: &'static str = "nextSnapshot";
    const MAX_RETRY_ATTEMPTS: u32 = 10;
    const RETRY_DELAY_MS: u64 = 200;

    /// Create a new consumer with the given next snapshot ID.
    pub fn new(next_snapshot: i64) -> Self {
        Self { next_snapshot }
    }

    /// Get the next snapshot ID to be consumed.
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
    pub async fn from_path(file_io: &FileIO, path: &str) -> Result<Self> {
        let input_file = file_io.new_input(path)?;

        let mut last_error = None;
        for attempt in 1..=Self::MAX_RETRY_ATTEMPTS {
            match input_file.read().await {
                Ok(bytes) => {
                    let json = String::from_utf8(bytes).map_err(|e| {
                        crate::error::Error::DataInvalid {
                            message: format!("Consumer file is not valid UTF-8: {}", e),
                            source: Some(Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
                        }
                    })?;
                    return Self::from_json(&json);
                }
                Err(e) => {
                    last_error = Some(e);
                    if attempt < Self::MAX_RETRY_ATTEMPTS {
                        tokio::time::sleep(tokio::time::Duration::from_millis(
                            Self::RETRY_DELAY_MS,
                        ))
                        .await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| crate::error::Error::IoUnexpected {
            message: format!(
                "Failed to read consumer from {} after {} attempts",
                path, Self::MAX_RETRY_ATTEMPTS
            ),
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "All retry attempts failed",
            )),
        }))
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
