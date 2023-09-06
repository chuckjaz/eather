use crate::content::{Id, Slot, SlotOwner, SlotEntry};
use crate::result::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use tokio::io::AsyncWrite;

#[allow(unused)] // Used by a trait_async
pub type ByteStream = BoxStream<'static, Result<Bytes>>;

#[async_trait]
pub trait ContentProvider {
    async fn get(&self, id: Id) -> Result<ByteStream>;
    async fn has(&self, id: Id) -> Result<bool>;
}

#[async_trait]
pub trait ContentStore {
    async fn put(&self, id: Id) -> Result<Box<dyn AsyncWrite + Unpin + Send>>;
}

#[async_trait]
pub trait SlotHolder {
    async fn current(&self, slot: Slot) -> Result<SlotEntry>;
    async fn create(&self, slot: Slot, entry: SlotEntry) -> Result<()>;
    async fn update(&self, slot: Slot, entry: SlotEntry) -> Result<()>;
}

#[async_trait]
pub trait SlotOwnerStore {
    async fn get_slot_owner(&self, slot: Slot) -> Result<SlotOwner>;
    async fn save_slot_owner(&self, slot: Slot, owner: SlotOwner) -> Result<()>;
}
