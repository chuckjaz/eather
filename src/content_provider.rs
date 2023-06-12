use crate::content::Id;
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
    async fn current(&self, id: Id) -> Result<Id>;
}

#[async_trait]
pub trait ContentStore {
    async fn put(&self, id: Id) -> Result<Box<dyn AsyncWrite + Unpin + Send>>;
}
