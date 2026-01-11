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

use crate::client::connection::FlussConnection;
use crate::client::metadata::Metadata;
use crate::metadata::{TableInfo, TablePath};
use std::sync::Arc;

use crate::error::Result;

pub const EARLIEST_OFFSET: i64 = -2;

mod append;

mod log_fetch_buffer;
mod remote_log;
mod scanner;
mod writer;

pub use append::{AppendWriter, TableAppend};
pub use scanner::{LogScanner, RecordBatchLogScanner, TableScan};

#[allow(dead_code)]
pub struct FlussTable<'a> {
    conn: &'a FlussConnection,
    metadata: Arc<Metadata>,
    table_info: TableInfo,
    table_path: TablePath,
    has_primary_key: bool,
}

impl<'a> FlussTable<'a> {
    pub fn new(conn: &'a FlussConnection, metadata: Arc<Metadata>, table_info: TableInfo) -> Self {
        FlussTable {
            conn,
            table_path: table_info.table_path.clone(),
            has_primary_key: table_info.has_primary_key(),
            table_info,
            metadata,
        }
    }

    pub fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    pub fn new_append(&self) -> Result<TableAppend> {
        Ok(TableAppend::new(
            self.table_path.clone(),
            self.table_info.clone(),
            self.conn.get_or_create_writer_client()?,
        ))
    }

    pub fn new_scan(&self) -> TableScan<'_> {
        TableScan::new(self.conn, self.table_info.clone(), self.metadata.clone())
    }

    pub fn metadata(&self) -> &Arc<Metadata> {
        &self.metadata
    }

    pub fn table_info(&self) -> &TableInfo {
        &self.table_info
    }

    pub fn table_path(&self) -> &TablePath {
        &self.table_path
    }

    pub fn has_primary_key(&self) -> bool {
        self.has_primary_key
    }

    /// Lookup a key in the kv storage for this table. Returns Ok(Some(value)) if found,
    /// Ok(None) if not found. Only supported for primary-key tables.
    pub async fn lookup_kv(&self, key: &[u8]) -> Result<Option<String>> {
        use crate::bucketing::BucketingFunction;
        use crate::error::Error;
        use crate::rpc::message::LookupKvRequest;

        if !self.has_primary_key() {
            return Err(Error::UnexpectedError {
                message: "The table is not primary key table.".to_string(),
                source: None,
            });
        }

        // Ensure metadata is up-to-date for this table
        self.metadata
            .check_and_update_table_metadata(std::slice::from_ref(&self.table_path))
            .await?;

        let cluster = self.metadata.get_cluster();
        let num_buckets = cluster.get_bucket_count(&self.table_path);

        // Use default bucketing function for now (no special data lake format)
        let bucketing = <dyn BucketingFunction>::of(None);
        let bucket_id = bucketing.bucketing(key, num_buckets)?;

        let table_bucket = cluster.get_table_bucket(&self.table_path, bucket_id);

        let leader = match self.metadata.leader_for(&table_bucket) {
            Some(s) => s,
            None => {
                return Err(Error::UnexpectedError {
                    message: format!("No leader found for table bucket {table_bucket}"),
                    source: None,
                });
            }
        };

        let connection = self.metadata.get_connection(&leader).await?;
        let request = LookupKvRequest::new(&self.table_path, key);
        let response = connection.request(request).await?;

        if response.found {
            Ok(response.value)
        } else {
            Ok(None)
        }
    }
}

impl<'a> Drop for FlussTable<'a> {
    fn drop(&mut self) {
        // do-nothing now
    }
}
