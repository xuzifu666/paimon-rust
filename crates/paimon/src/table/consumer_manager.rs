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

//! Consumer manager for Apache Paimon.

use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::io::FileIO;
use crate::spec::Consumer;

const CONSUMER_DIR: &str = "consumer";
const CONSUMER_PREFIX: &str = "consumer-";

/// Default main branch name.
pub const DEFAULT_MAIN_BRANCH: &str = "main";

/// Manager for consumer groups.
///
/// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/consumer/ConsumerManager.java>.
#[derive(Debug, Clone)]
pub struct ConsumerManager {
    file_io: FileIO,
    table_path: String,
    branch: String,
}

impl ConsumerManager {
    /// Create a new ConsumerManager with the default main branch.
    pub fn new(file_io: FileIO, table_path: String) -> Self {
        Self::new_with_branch(file_io, table_path, DEFAULT_MAIN_BRANCH.to_string())
    }

    /// Create a new ConsumerManager with the specified branch.
    pub fn new_with_branch(file_io: FileIO, table_path: String, branch: String) -> Self {
        let branch = if branch.is_empty() {
            DEFAULT_MAIN_BRANCH.to_string()
        } else {
            branch
        };

        Self {
            file_io,
            table_path,
            branch,
        }
    }

    /// Get the consumer directory path.
    fn consumer_dir(&self) -> String {
        format!("{}/{}/{}", self.table_path, self.branch, CONSUMER_DIR)
    }

    /// Get the consumer file path for the given consumer id.
    fn consumer_path(&self, consumer_id: &str) -> String {
        format!("{}/{}{}", self.consumer_dir(), CONSUMER_PREFIX, consumer_id)
    }

    /// Get the consumer with the given id.
    ///
    /// Returns `None` if the consumer does not exist.
    pub async fn consumer(&self, consumer_id: &str) -> crate::Result<Option<Consumer>> {
        let path = self.consumer_path(consumer_id);
        Consumer::from_path(&self.file_io, &path).await
    }

    /// Reset (create or update) a consumer with the given id and next snapshot.
    pub async fn reset_consumer(&self, consumer_id: &str, consumer: Consumer) -> crate::Result<()> {
        let path = self.consumer_path(consumer_id);
        let json = consumer.to_json()?;

        // Ensure the consumer directory exists
        self.file_io.mkdirs(&self.consumer_dir()).await?;

        let output = self.file_io.new_output(&path)?;
        output.write(bytes::Bytes::from(json)).await?;

        Ok(())
    }

    /// Delete the consumer with the given id.
    pub async fn delete_consumer(&self, consumer_id: &str) -> crate::Result<()> {
        let path = self.consumer_path(consumer_id);
        self.file_io.delete_file(&path).await?;
        Ok(())
    }

    /// Get the minimum next snapshot id among all consumers.
    ///
    /// Returns `None` if no consumers exist.
    pub async fn min_next_snapshot(&self) -> crate::Result<Option<i64>> {
        let consumer_dir = self.consumer_dir();

        // Check if directory exists
        if !self.file_io.exists(&consumer_dir).await? {
            return Ok(None);
        }

        let statuses = self.file_io.list_status(&consumer_dir).await?;
        let mut min_snapshot: Option<i64> = None;

        for status in statuses {
            let file_name = status.path.rsplit('/').next().unwrap_or(&status.path);

            // Only process consumer files (with prefix)
            if let Some(id) = file_name.strip_prefix(CONSUMER_PREFIX) {
                if let Some(consumer) = self.consumer(id).await? {
                    let next = consumer.next_snapshot();
                    match min_snapshot {
                        None => min_snapshot = Some(next),
                        Some(curr) => {
                            if next < curr {
                                min_snapshot = Some(next);
                            }
                        }
                    }
                }
            }
        }

        Ok(min_snapshot)
    }

    /// Expire consumers that were modified before the given expiration time.
    pub async fn expire(&self, expire_time: DateTime<Utc>) -> crate::Result<()> {
        let consumer_dir = self.consumer_dir();

        // Check if directory exists
        if !self.file_io.exists(&consumer_dir).await? {
            return Ok(());
        }

        let statuses = self.file_io.list_status(&consumer_dir).await?;

        for status in statuses {
            let file_name = status.path.rsplit('/').next().unwrap_or(&status.path);

            // Only process consumer files
            if file_name.starts_with(CONSUMER_PREFIX) {
                if let Some(last_modified) = status.last_modified {
                    if expire_time > last_modified {
                        self.file_io.delete_file(&status.path).await?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Clear consumers based on include and exclude patterns.
    ///
    /// - `including_pattern`: Only consumers matching this pattern will be cleared.
    /// - `excluding_pattern`: Consumers matching this pattern will NOT be cleared (optional).
    pub async fn clear_consumers(
        &self,
        including_pattern: &str,
        excluding_pattern: Option<&str>,
    ) -> crate::Result<()> {
        let consumer_dir = self.consumer_dir();

        // Check if directory exists
        if !self.file_io.exists(&consumer_dir).await? {
            return Ok(());
        }

        let statuses = self.file_io.list_status(&consumer_dir).await?;

        for status in statuses {
            let file_name = status.path.rsplit('/').next().unwrap_or(&status.path);

            // Only process consumer files
            if let Some(consumer_name) = file_name.strip_prefix(CONSUMER_PREFIX) {
                let mut should_clear = glob_match(including_pattern, consumer_name);

                if should_clear {
                    if let Some(exclude_pattern) = excluding_pattern {
                        should_clear = should_clear && !glob_match(exclude_pattern, consumer_name);
                    }
                }

                if should_clear {
                    self.file_io.delete_file(&status.path).await?;
                }
            }
        }

        Ok(())
    }

    /// Get all consumers as a map from consumer id to next snapshot.
    pub async fn consumers(&self) -> crate::Result<HashMap<String, i64>> {
        let consumer_dir = self.consumer_dir();
        let mut consumers = HashMap::new();

        // Check if directory exists
        if !self.file_io.exists(&consumer_dir).await? {
            return Ok(consumers);
        }

        let statuses = self.file_io.list_status(&consumer_dir).await?;

        for status in statuses {
            let file_name = status.path.rsplit('/').next().unwrap_or(&status.path);

            // Only process consumer files
            if let Some(id) = file_name.strip_prefix(CONSUMER_PREFIX) {
                if let Some(consumer) = self.consumer(id).await? {
                    consumers.insert(id.to_string(), consumer.next_snapshot());
                }
            }
        }

        Ok(consumers)
    }

    /// List all consumer ids.
    pub async fn list_all_ids(&self) -> crate::Result<Vec<String>> {
        let consumer_dir = self.consumer_dir();
        let mut ids = Vec::new();

        // Check if directory exists
        if !self.file_io.exists(&consumer_dir).await? {
            return Ok(ids);
        }

        let statuses = self.file_io.list_status(&consumer_dir).await?;

        for status in statuses {
            let file_name = status.path.rsplit('/').next().unwrap_or(&status.path);

            // Only process consumer files
            if let Some(id) = file_name.strip_prefix(CONSUMER_PREFIX) {
                ids.push(id.to_string());
            }
        }

        Ok(ids)
    }

    /// Get the table path.
    pub fn table_path(&self) -> &str {
        &self.table_path
    }

    /// Get the branch name.
    pub fn branch(&self) -> &str {
        &self.branch
    }
}

/// Simple glob pattern matching function.
///
/// Supports:
/// - `*` matches any sequence of characters (except path separator)
/// - `?` matches any single character
/// - `**` matches any sequence of characters including path separators
fn glob_match(pattern: &str, name: &str) -> bool {
    // Handle simple cases
    if pattern == "*" || pattern == "**" {
        return true;
    }

    // Convert pattern to regex for matching
    let mut regex_pattern = String::from("^");
    let mut chars = pattern.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '*' => {
                if chars.peek() == Some(&'*') {
                    // ** matches everything
                    chars.next();
                    regex_pattern.push_str(".*");
                } else {
                    // * matches anything except /
                    regex_pattern.push_str("[^/]*");
                }
            }
            '?' => {
                // ? matches any single character except /
                regex_pattern.push_str("[^/]");
            }
            '.' => {
                regex_pattern.push_str("\\.");
            }
            '[' => {
                regex_pattern.push('[');
            }
            ']' => {
                regex_pattern.push(']');
            }
            '(' | ')' | '+' | '^' | '$' | '|' | '\\' => {
                regex_pattern.push('\\');
                regex_pattern.push(c);
            }
            _ => {
                regex_pattern.push(c);
            }
        }
    }

    regex_pattern.push('$');

    // Use regex for matching
    if let Ok(re) = regex::Regex::new(&regex_pattern) {
        re.is_match(name)
    } else {
        // Fallback: if regex is invalid, do exact match
        pattern == name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_match() {
        // Exact match
        assert!(glob_match("consumer-1", "consumer-1"));
        assert!(!glob_match("consumer-1", "consumer-2"));

        // Single wildcard
        assert!(glob_match("consumer-*", "consumer-1"));
        assert!(glob_match("consumer-*", "consumer-abc"));
        assert!(!glob_match("consumer-*", "consumer-1/2"));

        // Question mark
        assert!(glob_match("consumer-?", "consumer-1"));
        assert!(glob_match("consumer-?", "consumer-a"));
        assert!(!glob_match("consumer-?", "consumer-ab"));

        // Asterisk
        assert!(glob_match("*", "anything"));
    }

    #[tokio::test]
    async fn test_consumer_manager_consumer_path() {
        // Create a memory-based FileIO for testing
        let file_io = FileIO::from_path("memory:/").unwrap().build().unwrap();

        let manager = ConsumerManager::new(file_io, "/test/table".to_string());

        assert_eq!(manager.consumer_dir(), "/test/table/main/consumer");
        assert_eq!(
            manager.consumer_path("consumer-1"),
            "/test/table/main/consumer/consumer-consumer-1"
        );
    }

    #[tokio::test]
    async fn test_consumer_manager_with_branch() {
        let file_io = FileIO::from_path("memory:/").unwrap().build().unwrap();

        let manager = ConsumerManager::new_with_branch(
            file_io,
            "/test/table".to_string(),
            "feature-branch".to_string(),
        );

        assert_eq!(
            manager.consumer_dir(),
            "/test/table/feature-branch/consumer"
        );
        assert_eq!(manager.branch(), "feature-branch");
    }

    #[tokio::test]
    async fn test_consumer_manager_empty_branch() {
        let file_io = FileIO::from_path("memory:/").unwrap().build().unwrap();

        let manager =
            ConsumerManager::new_with_branch(file_io, "/test/table".to_string(), "".to_string());

        assert_eq!(manager.branch(), "main");
    }
}
