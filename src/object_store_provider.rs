use crate::{
    content::{Id, Slot, Description},
    content_provider::{ByteStream, ContentProvider, ContentStore, SlotHolder},
    result::Result,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{
    stream::{BoxStream, Stream},
    StreamExt,
};
use object_store::{path::Path, ObjectStore};
use tokio::io::AsyncWrite;

#[derive(Debug)]
pub struct ObjectStoreProvider<Store: ObjectStore> {
    store: Store,
}

impl<Store: ObjectStore> ObjectStoreProvider<Store> {
    pub fn new(store: Store) -> Self {
        return Self { store };
    }
}

#[async_trait]
impl<Store: ObjectStore> ContentProvider for ObjectStoreProvider<Store> {
    async fn get(&self, id: Id) -> Result<ByteStream> {
        let path = content_path(id);
        let result = self.store.get(&path).await?;
        let stream = result.into_stream();
        let my_stream = convert_to_stream(stream);
        Ok(my_stream.boxed())
    }

    async fn has(&self, id: Id) -> Result<bool> {
        let path = content_path(id);
        let result = self.store.head(&path).await;
        match result {
            Ok(_) => Ok(true),
            _ => Ok(false),
        }
    }
}

#[async_trait]
impl<Store: ObjectStore> ContentStore for ObjectStoreProvider<Store> {
    async fn put(&self, id: Id) -> Result<Box<dyn AsyncWrite + Unpin + Send>> {
        let path = content_path(id);
        let (_, parts) = self.store.put_multipart(&path).await?;
        Ok(parts)
    }
}

#[async_trait]
impl<Store: ObjectStore> SlotHolder for ObjectStoreProvider<Store> {
    async fn current(&self, slot: Slot) -> Result<Description> {
        log::info!("SlotHolder::current({slot:?}");
        let path = slot_path(slot);
        let result = self.store.get(&path).await?;
        let bytes = result.bytes().await?;
        Ok(rmp_serde::decode::from_slice(&bytes[..])?)
    }

    async fn update(&self, slot: Slot, description: Description) -> Result<()> {
        let path = slot_path(slot);
        let bytes = rmp_serde::encode::to_vec(&description)?;
        let bytes = Bytes::from(bytes);
        self.store.put(&path, bytes).await?;
        Ok(())
    }
}

fn content_path(id: Id) -> Path {
    let name = id.id_string();
    let first: String = name.chars().take(2).collect();
    let second: String = name.chars().skip(2).take(2).collect();
    let last: String = name.chars().skip(4).collect();

    Path::from(first).child(second).child(last)
}

fn slot_path(slot: Slot) -> Path {
    let name: String = slot.slot_string();
    let first: String = name.chars().take(2).collect();
    let second: String = name.chars().skip(2).take(2).collect();
    let last: String = name.chars().skip(4).collect();
    
    Path::from(first).child(second).child(last)
}

async fn convert_error<T>(e: object_store::Result<T>) -> Result<T> {
    Ok(e?)
}

fn convert_to_stream(
    stream: BoxStream<object_store::Result<Bytes>>,
) -> impl Stream<Item = Result<Bytes>> + '_ {
    stream.then(|e| convert_error(e))
}
