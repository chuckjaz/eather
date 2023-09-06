use crate::{
    content::{Id, Slot, SlotEntry, SlotOwner},
    content_provider::{ByteStream, ContentProvider, ContentStore, SlotHolder, SlotOwnerStore},
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

    async fn store_has(&self, path: &Path) -> Result<bool> {
        Ok(
            match self.store.head(path).await {
                Ok(_) => true,
                Err(err) => match err {
                    object_store::Error::NotFound { path: _, source: _} => false,
                    _ => return Err(err.into())
                }
            }
        )
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
        self.store_has(&path).await
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
    async fn current(&self, slot: Slot) -> Result<SlotEntry> {
        log::info!("SlotHolder::current({slot:?}");
        let path = slot_path(slot);
        let result = self.store.get(&path).await?;
        let bytes = result.bytes().await?;
        Ok(rmp_serde::decode::from_slice(&bytes[..])?)
    }

    async fn create(&self, slot: Slot, entry: SlotEntry) -> Result<()> {
        log::info!("SlotHolder::create({slot:?}");
        if !entry.is_valid(slot) {
            return Err("Invalid entry for slot".into())
        }
        let path = slot_path(slot);
        if self.store_has(&path).await? {
            return Err("Slot already exists".into())
        }

        let bytes = rmp_serde::encode::to_vec(&entry)?;
        let bytes = Bytes::from(bytes);
        self.store.put(&path, bytes).await?;
        Ok(())
    }

    async fn update(&self, slot: Slot, entry: SlotEntry) -> Result<()> {
        let path = slot_path(slot);
        if !entry.is_valid(slot) {
            return Err("Invalid entry for slot".into())
        }
        let previous = entry.previous.ok_or("Previous Required to update an entry")?;
        let current = self.current(slot).await?;

        if previous != current.description {
            return Err("Previous is out of date".into())
        }

        let bytes = rmp_serde::encode::to_vec(&entry)?;
        let bytes = Bytes::from(bytes);
        self.store.put(&path, bytes).await?;
        Ok(())
    }
}

#[async_trait]
impl<Store: ObjectStore> SlotOwnerStore for ObjectStoreProvider<Store> {
    async fn get_slot_owner(&self, slot: Slot) -> Result<SlotOwner> {
        log::info!("SlotOwnerStore::get_slot_owner({slot:?}");
        let path = slot_path(slot);
        let result = match self.store.get(&path).await {
            Ok(result) => result,
            Err(err) => {
                match err {
                    object_store::Error::NotFound { path: _, source: _} => {
                        return Err("Not found".into())
                    }
                    _ => return Err(err.into())
                }
            }
        };
        let bytes = result.bytes().await?;
        Ok(rmp_serde::decode::from_slice(&bytes[..])?)
    }

    async fn save_slot_owner(&self, slot: Slot, owner: SlotOwner) -> Result<()> {
        let path = slot_path(slot);
        let bytes = rmp_serde::encode::to_vec(&owner)?;
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
