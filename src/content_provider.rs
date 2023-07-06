use crate::content::{Id, Slot, Description};
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
    async fn current(&self, slot: Slot) -> Result<Description>;
    async fn update(&self, slot: Slot, description: Description) -> Result<()>;
}
