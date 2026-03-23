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
use crate::spec::Consumer;
use chrono::{DateTime, Utc};
use regex::Regex;
use std::collections::HashMap;

/// Manage consumer groups.
///
/// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/consumer/ConsumerManager.java>
#[derive(Debug, Clone)]
pub struct ConsumerManager {
    file_io: FileIO,
    table_path: String,
    branch: String,
}

const CONSUMER_PREFIX: &str = "consumer-";
const DEFAULT_MAIN_BRANCH: &str = "main";

impl ConsumerManager {
    /// Create a new ConsumerManager with default "main" branch.
    pub fn new(file_io: FileIO, table_path: String) -> Self {
        Self {
            file_io,
            table_path,
            branch: DEFAULT_MAIN_BRANCH.to_string(),
        }
    }

    /// Create a new ConsumerManager with custom branch name.
    ///
    /// If branch name is null or whitespace, it will default to "main".
    pub fn with_branch(file_io: FileIO, table_path: String, branch_name: Option<String>) -> Self {
        let branch = if let Some(name) = branch_name {
            if name.trim().is_empty() {
                DEFAULT_MAIN_BRANCH.to_string()
            } else {
                name
            }
        } else {
            DEFAULT_MAIN_BRANCH.to_string()
        };

        Self {
            file_io,
            table_path,
            branch,
        }
    }

    /// Get consumer by ID.
    pub async fn consumer(&self, consumer_id: &str) -> Result<Option<Consumer>> {
        let path = self.consumer_path(consumer_id);
        Consumer::from_path(&self.file_io, &path).await
    }

    /// Reset (update) consumer with given ID and consumer data.
    pub async fn reset_consumer(&self, consumer_id: &str, consumer: &Consumer) -> Result<()> {
        let path = self.consumer_path(consumer_id);
        let output_file = self.file_io.new_output(&path)?;
        let json = consumer.to_json()?;
        output_file.write(bytes::Bytes::from(json)).await
    }

    /// Delete consumer with given ID.
    pub async fn delete_consumer(&self, consumer_id: &str) -> Result<()> {
        let path = self.consumer_path(consumer_id);
        self.file_io.delete_file(&path).await
    }

    /// Find the minimum next snapshot ID among all consumers.
    ///
    /// Returns None if there are no consumers.
    pub async fn min_next_snapshot(&self) -> Result<Option<i64>> {
        let consumer_ids = self.list_all_ids().await?;
        let mut min_snapshot: Option<i64> = None;

        for consumer_id in &consumer_ids {
            if let Some(consumer) = self.consumer(consumer_id).await? {
                let snapshot_id = consumer.next_snapshot();
                match min_snapshot {
                    None => min_snapshot = Some(snapshot_id),
                    Some(current_min) if snapshot_id < current_min => {
                        min_snapshot = Some(snapshot_id)
                    }
                    _ => {}
                }
            }
        }

        Ok(min_snapshot)
    }

    /// Expire consumers whose modification time is before given datetime.
    pub async fn expire(&self, expire_datetime: DateTime<Utc>) -> Result<()> {
        let consumer_ids = self.list_all_ids().await?;

        for consumer_id in &consumer_ids {
            let path = self.consumer_path(consumer_id);

            // Get file modification time
            if let Some(modification_time) = self.get_file_modification_time(&path).await? {
                if expire_datetime > modification_time {
                    // Delete the consumer file
                    self.file_io.delete_file(&path).await?;
                }
            }
        }

        Ok(())
    }

    /// Clear consumers matching the given patterns.
    ///
    /// # Arguments
    /// * `including_pattern` - Regex pattern for consumers to include (if None, match all)
    /// * `excluding_pattern` - Regex pattern for consumers to exclude (if None, exclude none)
    pub async fn clear_consumers(
        &self,
        including_pattern: Option<&Regex>,
        excluding_pattern: Option<&Regex>,
    ) -> Result<()> {
        let consumer_ids = self.list_all_ids().await?;

        for consumer_id in &consumer_ids {
            let mut should_clear = match including_pattern {
                Some(pattern) => pattern.is_match(consumer_id),
                None => true,
            };

            if should_clear {
                should_clear = match excluding_pattern {
                    Some(pattern) => !pattern.is_match(consumer_id),
                    None => true,
                };
            }

            if should_clear {
                let path = self.consumer_path(consumer_id);
                self.file_io.delete_file(&path).await?;
            }
        }

        Ok(())
    }

    /// Get all consumers as a map of consumer ID to next snapshot ID.
    pub async fn consumers(&self) -> Result<HashMap<String, i64>> {
        let consumer_ids = self.list_all_ids().await?;
        let mut consumers_map = HashMap::new();

        for consumer_id in &consumer_ids {
            if let Some(consumer) = self.consumer(consumer_id).await? {
                consumers_map.insert(consumer_id.clone(), consumer.next_snapshot());
            }
        }

        Ok(consumers_map)
    }

    /// List all consumer IDs.
    pub async fn list_all_ids(&self) -> Result<Vec<String>> {
        let consumer_dir = self.consumer_directory();
        let mut consumer_ids = Vec::new();

        // Try to list the consumer directory
        match self.file_io.list_status(&consumer_dir).await {
            Ok(statuses) => {
                for status in &statuses {
                    if !status.is_dir {
                        let filename = status.path.rsplit('/').next().unwrap_or(&status.path);

                        if let Some(consumer_id) = self.extract_consumer_id_from_filename(filename)
                        {
                            consumer_ids.push(consumer_id);
                        }
                    }
                }
            }
            Err(e) => {
                // If directory doesn't exist, return empty list
                if e.to_string().contains("not found")
                    || e.to_string().contains("No such file")
                    || e.to_string().contains("is not a directory")
                {
                    return Ok(Vec::new());
                }
                return Err(e);
            }
        }

        Ok(consumer_ids)
    }

    // Helper methods

    fn consumer_directory(&self) -> String {
        format!("{}/{}/consumer", self.table_path, self.branch)
    }

    fn consumer_path(&self, consumer_id: &str) -> String {
        format!(
            "{}/{}{}",
            self.consumer_directory(),
            CONSUMER_PREFIX,
            consumer_id
        )
    }

    fn extract_consumer_id_from_filename(&self, filename: &str) -> Option<String> {
        filename
            .strip_prefix(CONSUMER_PREFIX)
            .map(|s| s.to_string())
    }

    async fn get_file_modification_time(&self, path: &str) -> Result<Option<DateTime<Utc>>> {
        match self.file_io.get_status(path).await {
            Ok(status) => Ok(status.last_modified),
            Err(e) => {
                if e.to_string().contains("not found") || e.to_string().contains("No such file") {
                    Ok(None)
                } else {
                    Err(e)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;

    async fn create_test_manager() -> (ConsumerManager, tempfile::TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let manager = ConsumerManager::new(file_io, temp_dir.path().to_string_lossy().to_string());
        (manager, temp_dir)
    }

    #[tokio::test]
    async fn test_reset_and_get_consumer() {
        let (manager, _temp) = create_test_manager().await;
        let consumer = Consumer::new(5);

        manager.reset_consumer("test-id", &consumer).await.unwrap();
        let retrieved = manager.consumer("test-id").await.unwrap();

        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().next_snapshot(), 5);
    }

    #[tokio::test]
    async fn test_nonexistent_consumer() {
        let (manager, _temp) = create_test_manager().await;
        let result = manager.consumer("nonexistent").await.unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_delete_consumer() {
        let (manager, _temp) = create_test_manager().await;
        let consumer = Consumer::new(5);

        manager.reset_consumer("test-id", &consumer).await.unwrap();
        manager.delete_consumer("test-id").await.unwrap();
        let result = manager.consumer("test-id").await.unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_min_next_snapshot() {
        let (manager, _temp) = create_test_manager().await;

        manager
            .reset_consumer("id1", &Consumer::new(10))
            .await
            .unwrap();
        manager
            .reset_consumer("id2", &Consumer::new(5))
            .await
            .unwrap();
        manager
            .reset_consumer("id3", &Consumer::new(15))
            .await
            .unwrap();

        let min_snapshot = manager.min_next_snapshot().await.unwrap();
        assert_eq!(min_snapshot, Some(5));
    }

    #[tokio::test]
    async fn test_empty_min_next_snapshot() {
        let (manager, _temp) = create_test_manager().await;
        let min_snapshot = manager.min_next_snapshot().await.unwrap();

        assert_eq!(min_snapshot, None);
    }

    #[tokio::test]
    async fn test_list_all_ids() {
        let (manager, _temp) = create_test_manager().await;

        manager
            .reset_consumer("id1", &Consumer::new(1))
            .await
            .unwrap();
        manager
            .reset_consumer("id2", &Consumer::new(2))
            .await
            .unwrap();
        manager
            .reset_consumer("id3", &Consumer::new(3))
            .await
            .unwrap();

        let ids = manager.list_all_ids().await.unwrap();
        assert_eq!(ids.len(), 3);
        assert!(ids.contains(&"id1".to_string()));
        assert!(ids.contains(&"id2".to_string()));
        assert!(ids.contains(&"id3".to_string()));
    }

    #[tokio::test]
    async fn test_clear_consumers_with_patterns() {
        let (manager, _temp) = create_test_manager().await;

        manager
            .reset_consumer("test-1", &Consumer::new(1))
            .await
            .unwrap();
        manager
            .reset_consumer("test-2", &Consumer::new(2))
            .await
            .unwrap();
        manager
            .reset_consumer("prod-1", &Consumer::new(3))
            .await
            .unwrap();
        manager
            .reset_consumer("prod-2", &Consumer::new(4))
            .await
            .unwrap();

        let including = Regex::new(r"^test-").unwrap();
        manager
            .clear_consumers(Some(&including), None)
            .await
            .unwrap();

        let ids = manager.list_all_ids().await.unwrap();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&"prod-1".to_string()));
        assert!(ids.contains(&"prod-2".to_string()));
    }
}
