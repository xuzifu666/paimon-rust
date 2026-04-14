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

//! Vortex file format configuration.

/// Vortex file format configuration.
#[derive(Debug, Clone)]
pub struct VortexFormatOptions {
    /// Batch size for reading.
    pub read_batch_size: usize,
    /// Batch size for writing.
    pub write_batch_size: usize,
    /// Memory limit for writing in bytes.
    pub write_batch_memory: usize,
}

impl Default for VortexFormatOptions {
    fn default() -> Self {
        Self {
            read_batch_size: 1024,
            write_batch_size: 1024,
            write_batch_memory: 1024 * 1024 * 128, // 128MB
        }
    }
}
