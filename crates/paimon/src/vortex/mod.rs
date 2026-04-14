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

//! Vortex file format support for Apache Paimon.
//!
//! Vortex is a next-generation columnar file format designed for high-performance
//! data processing with excellent compression and fast query performance.
//!
//! Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-vortex>

mod format;
pub use format::FileFormat;

mod format_options;
pub use format_options::VortexFormatOptions;

mod format_impl;
pub use format_impl::VortexFileFormat;

mod validation;
pub use validation::validate_row_type;

pub mod reader;
pub use reader::{VortexReader, VortexReaderFactory};

pub mod writer;
pub use writer::{VortexWriter, VortexWriterFactory};

pub mod utils;
pub mod types;
