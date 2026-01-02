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

//! ORC reader adapters.
//!
//! Provides an [`AsyncChunkReader`] implementation backed by `object_store`
//! so `orc-rust` can read from local or remote object stores.

use bytes::Bytes;
use futures_util::future::BoxFuture;
use object_store::path::Path;
use object_store::ObjectStore;
use orc_rust::reader::AsyncChunkReader;
use std::sync::Arc;

/// Adapter to convert ObjectStore to AsyncChunkReader for orc-rust
pub struct ObjectStoreChunkReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
    file_size: Option<u64>,
}

impl ObjectStoreChunkReader {
    /// Create a new ObjectStoreChunkReader
    pub fn new(store: Arc<dyn ObjectStore>, path: Path) -> Self {
        Self {
            store,
            path,
            file_size: None,
        }
    }

    /// Create with known file size (for optimization)
    pub fn with_size(store: Arc<dyn ObjectStore>, path: Path, size: u64) -> Self {
        Self {
            store,
            path,
            file_size: Some(size),
        }
    }
}

impl AsyncChunkReader for ObjectStoreChunkReader {
    fn len(&mut self) -> BoxFuture<'_, std::io::Result<u64>> {
        Box::pin(async move {
            if let Some(size) = self.file_size {
                Ok(size)
            } else {
                // Fetch metadata to get file size
                let meta =
                    self.store.head(&self.path).await.map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
                    })?;
                Ok(meta.size as u64)
            }
        })
    }

    fn get_bytes(
        &mut self,
        offset_from_start: u64,
        length: u64,
    ) -> BoxFuture<'_, std::io::Result<Bytes>> {
        let store = Arc::clone(&self.store);
        let path = self.path.clone();

        Box::pin(async move {
            let range = offset_from_start..(offset_from_start + length);
            let bytes = store
                .get_range(&path, range)
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
            Ok(bytes)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use orc_rust::reader::AsyncChunkReader;

    #[tokio::test]
    async fn test_object_store_chunk_reader_new() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("test.orc");
        let reader = ObjectStoreChunkReader::new(Arc::clone(&store), path.clone());

        assert!(reader.file_size.is_none());
        assert_eq!(reader.path, path);
    }

    #[tokio::test]
    async fn test_object_store_chunk_reader_with_size() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("test.orc");
        let reader = ObjectStoreChunkReader::with_size(Arc::clone(&store), path.clone(), 1024);

        assert_eq!(reader.file_size, Some(1024));
        assert_eq!(reader.path, path);
    }

    #[tokio::test]
    async fn test_object_store_chunk_reader_len_with_known_size() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("test.orc");
        let mut reader = ObjectStoreChunkReader::with_size(store, path, 2048);

        let len = reader.len().await.unwrap();
        assert_eq!(len, 2048);
    }

    #[tokio::test]
    async fn test_object_store_chunk_reader_get_bytes() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("test_file.bin");

        // Upload test data
        let test_data = bytes::Bytes::from(vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        store
            .put(&path, test_data.clone().into())
            .await
            .expect("Failed to upload test data");

        let mut reader =
            ObjectStoreChunkReader::with_size(Arc::clone(&store), path, test_data.len() as u64);

        // Read a portion of the data
        let bytes = reader.get_bytes(2, 4).await.unwrap();
        assert_eq!(bytes.as_ref(), &[2, 3, 4, 5]);

        // Read from the beginning
        let bytes = reader.get_bytes(0, 3).await.unwrap();
        assert_eq!(bytes.as_ref(), &[0, 1, 2]);
    }

    #[tokio::test]
    async fn test_object_store_chunk_reader_len_fetch_metadata() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("metadata_test.bin");

        // Upload test data
        let test_data = bytes::Bytes::from(vec![0u8; 512]);
        store
            .put(&path, test_data.into())
            .await
            .expect("Failed to upload test data");

        // Create reader without known size
        let mut reader = ObjectStoreChunkReader::new(Arc::clone(&store), path);

        // Should fetch metadata to get size
        let len = reader.len().await.unwrap();
        assert_eq!(len, 512);
    }

    #[tokio::test]
    async fn test_object_store_chunk_reader_file_not_found() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("nonexistent.orc");

        let mut reader = ObjectStoreChunkReader::new(store, path);

        // Should return error when file doesn't exist
        let result = reader.len().await;
        assert!(result.is_err());
    }
}
