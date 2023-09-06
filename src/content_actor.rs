use std::sync::Arc;

use log::info;
use tokio::io::AsyncWrite;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, oneshot};

use crate::content::SlotEntry;
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
        response: oneshot::Sender<Result<SlotEntry>>,
    },
    UpdateSlot {
        slot: Slot,
        entry: SlotEntry,
        response: oneshot::Sender<Result<()>>,
    },
    GetStream {
        id: Id,
        response: oneshot::Sender<Result<ByteStream>>,
    },
    Put {
        id: Id,
        response: oneshot::Sender<Result<Box<dyn AsyncWrite + Unpin + Send>>>,
    }
}

impl std::fmt::Debug for ContentActorMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GetSlot { slot , response : _} => f.write_fmt(format_args!("GetSlot(slot: {slot:?})"))?,
            Self::UpdateSlot { slot, entry , response: _ } => f.write_fmt(format_args!("UpdateSlot(slot: {slot:?}, entry: {entry:?})"))?,
            Self::GetStream { id, response: _ } => f.write_fmt(format_args!("GetStream(id: {id:?})"))?,
            Self::Put { id, response: _ } => f.write_fmt(format_args!("Put(id: {id:?})"))?,
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
                entry,
                response,
            } => {
                let result = self.slots.update(slot, entry).await;
                let _ = response.send(result);
            }
            ContentActorMessage::GetStream { id, response } => {
                let result = self.get_stream(id).await;
                let _ = response.send(result);
            },
            ContentActorMessage::Put { id, response } => {
                let result = self.put(id).await;
                let _ = response.send(result);
            }
        }
    }

    async fn get_stream(&mut self, id: Id) -> Result<ByteStream> {
        info!("ContentActor::get_stream: id: {id:?}");
        Ok(self.provider.get(id).await?)
    }

    async fn put(&mut self, id: Id) -> Result<Box<dyn AsyncWrite + Unpin + Send>> {
        info!("ContentActor::put: id: {id:?}");
        Ok(self.store.put(id).await?)
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
        runtime: Arc<Runtime>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let actor = ContentActor::new(provider, store, slots, receiver);
        runtime.spawn(run_actor(actor));
        Self { sender }
    }

    pub async fn get_slot(&self, slot: Slot) -> Result<SlotEntry> {
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
        entry: SlotEntry,
    ) -> Result<()> {
        let (send, receive) = oneshot::channel();
        let message = ContentActorMessage::UpdateSlot {
            slot,
            entry,
            response: send,
        };
        let _ = self.sender.send(message).await;
        receive.await.expect("Actor has been killed")
    }

    pub async fn get_stream(&self, id: Id) -> Result<ByteStream> {
        let (send, receive) = oneshot::channel();
        let message = ContentActorMessage::GetStream { id, response: send };
        let _ = self.sender.send(message).await;
        receive.await.expect("Actor has been killed")
    }

    pub async fn put(&self, id: Id) -> Result<Box<dyn AsyncWrite + Unpin + Send>> {
        let (send, receive) = oneshot::channel();
        let message = ContentActorMessage::Put { id, response: send };
        let _ = self.sender.send(message).await;
        receive.await.expect("Actore has been killed")
    }
}
