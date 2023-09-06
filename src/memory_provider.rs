use std::{collections::HashMap, sync::Arc, task::Poll, io::ErrorKind};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::stream::BoxStream;
use parking_lot::RwLock;
use ring::digest::{Context, SHA256};
use tokio::io::AsyncWrite;

use crate::{
    content::{Id, Slot, SlotOwner, SlotEntry},
    content_provider::{ContentProvider, ByteStream, ContentStore, SlotHolder, SlotOwnerStore},
    result::Result
};

#[derive(Clone)]
pub struct MemoryProvider {
    table: Arc<RwLock<HashMap<Id, Vec<Bytes>>>>,
    slots: Arc<RwLock<HashMap<Slot, SlotEntry>>>,
    owners: Arc<RwLock<HashMap<Slot, SlotOwner>>>,
}

impl MemoryProvider {
    #[allow(unused)]
    pub fn new() -> MemoryProvider {
        Self {
            table: Arc::new(RwLock::new(HashMap::new())),
            slots: Arc::new(RwLock::new(HashMap::new())),
            owners: Arc::new(RwLock::new(HashMap::new())),
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
    async fn current(&self, slot: Slot) -> Result<SlotEntry> {
        Ok(self.slots.read().get(&slot).ok_or("Unknown slot")?.clone())
    }

    async fn create(&self, slot: Slot, entry: SlotEntry) -> Result<()> {
        entry.validate(slot)?;
        let mut slots = self.slots.write();
        if slots.get(&slot).is_some() {
            return Err("Slot already exists".into());
        }
        slots.insert(slot, entry);
        Ok(())
    }

    async fn update(&self, slot: Slot, entry: SlotEntry) -> Result<()> {
        entry.validate(slot)?;
        let previous = entry.previous.ok_or("Previous value is required to update slot")?;
        let mut slots = self.slots.write();
        let current = slots.get(&slot).ok_or("Unknown slot")?;
        if current.description != previous {
            return Err("Slot entry is out of date".into())
        }
        slots.insert(slot, entry);
        Ok(())
    }
}

#[async_trait]
impl SlotOwnerStore for MemoryProvider {
    async fn get_slot_owner(&self, slot: Slot) -> Result<SlotOwner> {
        let owners = self.owners.read();
        if let Some(owner) = owners.get(&slot) {
            Ok(*owner)
        } else {
            Err("No owner for slot".into())
        }
    }

    async fn save_slot_owner(&self, slot: Slot, owner: SlotOwner) -> Result<()> {
        let mut owners = self.owners.write();
        owners.insert(slot, owner);
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

    use crate::{content::{Id, Description, SlotEntry}, content_provider::{ContentStore, ContentProvider, SlotHolder}, content_loader::new_slot_pair};

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
    fn can_create_a_slot() {
        let (slot, owner) = new_slot_pair();
        let value = "Some value to add".as_bytes();
        let size: i64 = value.len().try_into().unwrap();
        let id = id_of(value);
        let store = MemoryProvider::new();
        tokio_test::block_on(async {
            let description = Description {
                id,
                size
            };
            let entry = assert_ok!(SlotEntry::signed(description, None, slot, owner));
            assert_ok!(store.create(slot, entry).await);
        });
    }

    #[test]
    fn can_get_current_of_a_slot() {
        let (slot, owner) = new_slot_pair();
        let value = "Some value to add".as_bytes();
        let size: i64 = value.len().try_into().unwrap();
        let id = id_of(value);
        let store = MemoryProvider::new();
        tokio_test::block_on(async {
            let description = Description {
                id,
                size
            };
            let entry = assert_ok!(SlotEntry::signed(description, None, slot, owner));
            assert_ok!(store.create(slot, entry).await);
        });

        tokio_test::block_on(async {
            let entry = assert_ok!(store.current(slot).await);
            assert_eq!(entry.description.id, id);
            assert_eq!(entry.description.size, size);
        });
    }

    fn id_of(buf: &[u8]) -> Id {
        let mut context = Context::new(&SHA256);
        context.update(buf);
        let digest = context.finish();
        Id::from_sha256(digest.as_ref())
    }
}