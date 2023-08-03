use std::{collections::HashMap, sync::Arc, task::Poll, io::ErrorKind};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::stream::BoxStream;
use parking_lot::RwLock;
use ring::digest::{Context, SHA256};
use tokio::io::AsyncWrite;

use crate::{
    content::{Id, Description, Slot}, 
    content_provider::{ContentProvider, ByteStream, ContentStore, SlotHolder}, 
    result::Result
};

#[derive(Clone)]
pub struct MemoryProvider {
    table: Arc<RwLock<HashMap<Id, Vec<Bytes>>>>, 
    slots: Arc<RwLock<HashMap<Slot, Description>>>,
}

impl MemoryProvider {
    #[allow(unused)]
    pub fn new() -> MemoryProvider {
        Self {
            table: Arc::new(RwLock::new(HashMap::new())),
            slots: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl ContentProvider for MemoryProvider {
    async fn get(&self, id: Id) -> Result<ByteStream> {
        let table = self.table.read();
        let vector = table.get(&id).expect("Id not found");
        let bytes: Vec<_> = vector.iter().map(|b| { Result::Ok(b.clone()) }).collect();
        let stream = tokio_stream::iter(bytes);
        let result: BoxStream<'static, Result<Bytes>> = Box::pin(stream);
        Ok(result)
    }

    async fn has(&self, id: Id) -> Result<bool> {
        Ok(self.table.read().get(&id).is_some())
    }
}

struct Uploader {
    table: Arc<RwLock<HashMap<Id, Vec<Bytes>>>>,
    vector: Vec<Bytes>,
    id: Id,
    hasher: Context,
}

impl AsyncWrite for Uploader {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::result::Result<usize, std::io::Error>> {
        let len = buf.len();
        self.hasher.update(buf);
        let mut bytes = BytesMut::with_capacity(len);
        bytes.extend_from_slice(buf);
        self.vector.push(bytes.freeze());
        Poll::Ready(Ok(len))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>, 
        _cx: &mut std::task::Context<'_>
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>, 
        _cx: &mut std::task::Context<'_>
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        let uploaded_sha = self.hasher.clone().finish();
        let uploaded_id = Id::from_sha256(uploaded_sha.as_ref());
        if uploaded_id == self.id {
            self.table.write().insert(self.id, self.vector.clone());
            Poll::Ready(Ok(()))
        } else {
            Poll::Ready(Err(std::io::Error::new(ErrorKind::InvalidData, "Id does not match input")))
        }
    }
}

#[async_trait]
impl ContentStore for MemoryProvider {
    async fn put(&self, id: Id) -> Result<Box<dyn AsyncWrite + Unpin + Send>> {
        Ok(
            Box::new(
                Uploader {
                    table: self.table.clone(),
                    hasher: Context::new(&SHA256),
                    id,
                    vector:  Vec::new(),
                }
            )
        )
    } 
}

#[async_trait]
impl SlotHolder for MemoryProvider {
    async fn current(&self, slot: Slot) -> Result<Description> {
        Ok(*self.slots.read().get(&slot).expect("Unknown slot"))
    }

    async fn update(&self, slot: Slot, description: Description) -> Result<()> {
        self.slots.write().insert(slot, description);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use ring::digest::{SHA256, Context};
    use tokio::io::AsyncWriteExt;
    use tokio_test::assert_ok;
    use futures::stream::StreamExt;

    use crate::{content::{Id, Slot, Description}, content_provider::{ContentStore, ContentProvider, SlotHolder}};

    use super::MemoryProvider;

    #[test]
    fn can_create_provider() {
        let _ = MemoryProvider::new();
    }

    #[test]
    fn can_upload_value() {
        let value = "Some value to add".as_bytes();
        let id = id_of(value);
        let store = MemoryProvider::new();

        tokio_test::block_on(async {
            let mut writer = assert_ok!(store.put(id).await);
            assert_ok!(writer.write_all(value).await);
            assert_ok!(writer.shutdown().await);
        });
    }

    #[test]
    fn can_get_value() {
        let value = "Some value to add".as_bytes();
        let id = id_of(value);
        let store = MemoryProvider::new();

        tokio_test::block_on(async {
            let mut writer = assert_ok!(store.put(id).await);
            assert_ok!(writer.write_all(value).await);
            assert_ok!(writer.shutdown().await);
        });

        tokio_test::block_on(async {
            let mut stream = assert_ok!(store.get(id).await);
            let mut buffer = BytesMut::with_capacity(value.len());
            while let Some(bytes) = stream.next().await {
                let bytes = assert_ok!(bytes);
                buffer.extend_from_slice(bytes.as_ref());
            }
            let read = buffer.freeze();
            assert_eq!(read.as_ref(), value);
        });
    }

    #[test]
    fn can_update_a_slot() {
        let slot_bytes: [u8; 32] = [0; 32];
        let slot = Slot::ed25519(&slot_bytes);
        let value = "Some value to add".as_bytes();
        let size: i64 = value.len().try_into().unwrap();
        let id = id_of(value);
        let store = MemoryProvider::new();
        tokio_test::block_on(async {
            let description = Description {
                id,
                size
            };
            assert_ok!(store.update(slot, description).await);
        });
    }

    #[test]
    fn can_get_current_of_a_slot() {
        let slot_bytes: [u8; 32] = [0; 32];
        let slot = Slot::ed25519(&slot_bytes);
        let value = "Some value to add".as_bytes();
        let size: i64 = value.len().try_into().unwrap();
        let id = id_of(value);
        let store = MemoryProvider::new();
        tokio_test::block_on(async {
            let description = Description {
                id,
                size
            };
            assert_ok!(store.update(slot, description).await);
        });

        tokio_test::block_on(async {
            let description = assert_ok!(store.current(slot).await);
            assert_eq!(description.id, id);
            assert_eq!(description.size, size);
        });
    }

    fn id_of(buf: &[u8]) -> Id {
        let mut context = Context::new(&SHA256);
        context.update(buf);
        let digest = context.finish();
        Id::from_sha256(digest.as_ref())
    }
}