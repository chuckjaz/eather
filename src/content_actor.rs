use std::sync::Arc;

use bytes::{BytesMut, BufMut};
use futures::StreamExt;
use log::info;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, oneshot};

use crate::content::{Directory, Description};
use crate::content_provider::ByteStream;
use crate::{
    content::{Id, Slot},
    content_provider::{ContentProvider, ContentStore, SlotHolder},
    result::Result,
};

#[derive(Debug)]
struct ContentActor<Provider: ContentProvider, Store: ContentStore, Slots: SlotHolder> {
    receiver: mpsc::Receiver<ContentActorMessage>,
    provider: Provider,
    store: Store,
    slots: Slots,
}

enum ContentActorMessage {
    GetSlot {
        slot: Slot,
        response: oneshot::Sender<Result<Description>>,
    },
    UpdateSlot {
        slot: Slot,
        description: Description,
        response: oneshot::Sender<Result<()>>,
    },
    GetDirectory {
        id: Id,
        response: oneshot::Sender<Result<Directory>>,
    },
    GetStream {
        id: Id,
        response: oneshot::Sender<Result<ByteStream>>,
    }
}

impl std::fmt::Debug for ContentActorMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GetSlot { slot , response : _} => f.write_fmt(format_args!("GetSlot(slot: {slot:?})"))?,
            Self::UpdateSlot { slot, description , response: _ } => f.write_fmt(format_args!("UpdateSlot(slot: {slot:?}, description: {description:?})"))?,
            Self::GetDirectory { id, response: _ } => f.write_fmt(format_args!("GetDirectory(id: {id:?})"))?,
            Self::GetStream { id, response: _ } => f.write_fmt(format_args!("GetStream(id: {id:?})"))?
        };
        Ok(())
    }
}

impl<Provider: ContentProvider, Store: ContentStore, Slots: SlotHolder>
    ContentActor<Provider, Store, Slots>
{
    fn new(
        provider: Provider,
        store: Store,
        slots: Slots,
        receiver: mpsc::Receiver<ContentActorMessage>,
    ) -> Self {
        Self {
            receiver,
            provider,
            store,
            slots,
        }
    }

    async fn handle_message(&mut self, message: ContentActorMessage) {
        log::info!("Actor::handle_message: {:?}", message);
        match message {
            ContentActorMessage::GetSlot { slot, response } => {
                let result = self.slots.current(slot).await;
                let _ = response.send(result);
            }
            ContentActorMessage::UpdateSlot {
                slot,
                description,
                response,
            } => {
                let result = self.slots.update(slot, description).await;
                let _ = response.send(result);
            }
            ContentActorMessage::GetDirectory { id, response } => {
                let result = self.get_directory(id).await;
                let _ = response.send(result);
            },
            ContentActorMessage::GetStream { id, response } => {
                let result = self.get_stream(id).await;
                let _ = response.send(result);
            }
        }
    }

    async fn get_directory(&mut self, id: Id) -> Result<Directory> {
        let mut content = self.provider.get(id).await?;
        let mut directory_bytes = BytesMut::new();
        while let Some(bytes) = content.next().await {
            let bytes = bytes?;
            directory_bytes.put(bytes);
        }
        Ok(rmp_serde::decode::from_slice(&directory_bytes)?)
    }

    async fn get_stream(&mut self, id: Id) -> Result<ByteStream> {
        info!("ContentActor::get_stream: id: {id:?}");
        Ok(self.provider.get(id).await?)
    }
}

async fn run_actor<Provider: ContentProvider, Store: ContentStore, Slots: SlotHolder>(
    mut actor: ContentActor<Provider, Store, Slots>,
) {
    while let Some(message) = actor.receiver.recv().await {
        actor.handle_message(message).await;
    }
}

#[derive(Clone, Debug)]
pub struct ContentActorHandle {
    sender: mpsc::Sender<ContentActorMessage>,
    rt: Arc<Runtime>,
}

impl ContentActorHandle {
    pub fn new<
        Provider: ContentProvider + Send + Sync + 'static,
        Store: ContentStore + Send + Sync + 'static,
        Slots: SlotHolder + Send + Sync + 'static,
    >(
        provider: Provider,
        store: Store,
        slots: Slots,
    ) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(8);
        let actor = ContentActor::new(provider, store, slots, receiver);
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        rt.spawn(run_actor(actor));
        Ok(Self { sender, rt: Arc::new(rt) })
    }

    pub fn runtime(&self) -> Arc<Runtime> {
        self.rt.clone()
    }

    pub async fn get_slot(&self, slot: Slot) -> Result<Description> {
        let (send, receive) = oneshot::channel();
        let message = ContentActorMessage::GetSlot {
            slot,
            response: send,
        };
        let _ = self.sender.send(message).await;
        receive.await.expect("Actore has been killed")
    }

    pub async fn update_slot(
        &self,
        slot: Slot,
        description: Description,
    ) -> Result<()> {
        let (send, receive) = oneshot::channel();
        let message = ContentActorMessage::UpdateSlot {
            slot,
            description,
            response: send,
        };
        let _ = self.sender.send(message).await;
        receive.await.expect("Actor has been killed")
    }

    pub async fn get_directory(&self, id: Id) -> Result<Directory> {
        let (send, receive) = oneshot::channel();
        let message = ContentActorMessage::GetDirectory { id,  response: send };
        let _ = self.sender.send(message).await;
        receive.await.expect("Actor has been killed")
    }

    pub async fn get_stream(&self, id: Id) -> Result<ByteStream> {
        let (send, receive) = oneshot::channel();
        let message = ContentActorMessage::GetStream { id, response: send };
        let _ = self.sender.send(message).await;
        receive.await.expect("Actor has been killed")   
    }
}
