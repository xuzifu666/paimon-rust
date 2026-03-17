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

//! Consumer related types for Apache Paimon.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;
use std::time::Duration;
use tokio::time::sleep;

use crate::io::FileIO;

/// Consumer which contains next snapshot.
///
/// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/consumer/Consumer.java>.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Consumer {
    /// The next snapshot id to consume.
    #[serde(rename = "nextSnapshot")]
    pub next_snapshot: i64,
}

impl Consumer {
    /// Create a new Consumer with the given next snapshot id.
    pub fn new(next_snapshot: i64) -> Self {
        Self { next_snapshot }
    }

    /// Get the next snapshot id.
    #[inline]
    pub fn next_snapshot(&self) -> i64 {
        self.next_snapshot
    }

    /// Serialize the consumer to JSON string.
    pub fn to_json(&self) -> crate::Result<String> {
        serde_json::to_string(self).map_err(|e| crate::Error::DataInvalid {
            message: format!("Failed to serialize Consumer: {}", e),
            source: Some(Box::new(e)),
        })
    }

    /// Deserialize a Consumer from JSON string.
    pub fn from_json(json: &str) -> crate::Result<Consumer> {
        serde_json::from_str(json).map_err(|e| crate::Error::DataInvalid {
            message: format!("Failed to deserialize Consumer from JSON: {}", e),
            source: Some(Box::new(e)),
        })
    }

    /// Read a Consumer from the given path with retry mechanism.
    ///
    /// This method will retry up to 10 times if deserialization fails,
    /// waiting 200ms between retries.
    pub async fn from_path(file_io: &FileIO, path: &str) -> crate::Result<Option<Consumer>> {
        let mut retry_number = 0;
        let mut last_error: Option<crate::Error> = None;

        while retry_number < 10 {
            retry_number += 1;

            let input = match file_io.new_input(path) {
                Ok(input) => input,
                Err(_) => {
                    // File does not exist - return None
                    return Ok(None);
                }
            };

            let exists = match input.exists().await {
                Ok(exists) => exists,
                Err(_) => {
                    // File does not exist
                    return Ok(None);
                }
            };

            if !exists {
                return Ok(None);
            }

            match input.read().await {
                Ok(content) => {
                    match str::from_utf8(&content) {
                        Ok(json_str) => {
                            match Self::from_json(json_str) {
                                Ok(consumer) => return Ok(Some(consumer)),
                                Err(e) => {
                                    // Retry on deserialization failure
                                    last_error = Some(e);
                                    sleep(Duration::from_millis(200)).await;
                                }
                            }
                        }
                        Err(e) => {
                            last_error = Some(crate::Error::DataInvalid {
                                message: "Invalid UTF-8 in consumer file".to_string(),
                                source: Some(Box::new(e)),
                            });
                            sleep(Duration::from_millis(200)).await;
                        }
                    }
                }
                Err(e) => {
                    last_error = Some(e);
                    sleep(Duration::from_millis(200)).await;
                }
            }
        }

        Err(last_error.unwrap_or_else(|| crate::Error::DataInvalid {
            message: "Retry failed after 10 times".to_string(),
            source: None,
        }))
    }
}

impl fmt::Display for Consumer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Consumer {{ nextSnapshot: {} }}", self.next_snapshot)
    }
}

/// Consumer info entry representing a consumer with id and next snapshot.
///
/// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-api/src/main/java/org/apache/paimon/consumer/ConsumerInfo.java>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerInfo {
    /// The consumer id.
    pub consumer_id: String,
    /// The next snapshot id.
    pub next_snapshot: Option<i64>,
}

impl ConsumerInfo {
    /// Create a new ConsumerInfo.
    pub fn new(consumer_id: String, next_snapshot: Option<i64>) -> Self {
        Self {
            consumer_id,
            next_snapshot,
        }
    }
}

/// File status for consumer files.
#[derive(Debug, Clone)]
pub struct ConsumerFileStatus {
    /// The path to the consumer file.
    pub path: String,
    /// The last modified time.
    pub last_modified: Option<DateTime<Utc>>,
}

impl ConsumerFileStatus {
    /// Create a new ConsumerFileStatus.
    pub fn new(path: String, last_modified: Option<DateTime<Utc>>) -> Self {
        Self {
            path,
            last_modified,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consumer_serialization() {
        let consumer = Consumer::new(12345);
        let json = consumer.to_json().unwrap();
        assert_eq!(json, r#"{"nextSnapshot":12345}"#);

        let deserialized = Consumer::from_json(&json).unwrap();
        assert_eq!(deserialized, consumer);
    }

    #[test]
    fn test_consumer_deserialization() {
        let json = r#"{"nextSnapshot":67890}"#;
        let consumer = Consumer::from_json(json).unwrap();
        assert_eq!(consumer.next_snapshot(), 67890);
    }

    #[test]
    fn test_consumer_display() {
        let consumer = Consumer::new(12345);
        let display = format!("{}", consumer);
        assert_eq!(display, "Consumer { nextSnapshot: 12345 }");
    }
}
