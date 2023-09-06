use std::sync::Arc;

use log::{error, info};
use tokio::{runtime::Runtime, sync::{Mutex, mpsc, oneshot}};

use crate::{content_file_layer::{FileContent, Node}, result::Result};

struct ContentTasksActor {
    receiver: mpsc::Receiver<ContentTaskMessage>,
    file_content: Arc<Mutex<dyn FileContent + Send>>,
}

impl ContentTasksActor {
    fn new(
        receiver: mpsc::Receiver<ContentTaskMessage>,
        file_content: Arc<Mutex<dyn FileContent + Send>>
    ) -> Self {
        Self { receiver, file_content, }
    }

    async fn run_task(&mut self, task: ContentTaskMessage) {
        match task {
            ContentTaskMessage::SyncNode { node, response } => {
                // Allow the notifier to return immediately.
                let _ = response.send(Ok(()));
                info!("ContentTaskActor::sync({node})");
                let result = self.file_content.lock().await.sync(node).await;
                if let  Err(err) = result {
                    error!("ContentTaskActor sync failed: {err:?}");
                }
            }
        };
    }
}

enum ContentTaskMessage {
    SyncNode {
        node: Node,
        response: oneshot::Sender<Result<()>>,
    },
}

#[derive(Clone, Debug)]
pub struct ContentTaskHandle {
    sender: mpsc::Sender<ContentTaskMessage>,
}

impl ContentTaskHandle {
    pub fn new(runtime: Arc<Runtime>, file_content: Arc<Mutex<dyn FileContent + Send>>) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let task_actor = ContentTasksActor::new(receiver, file_content);
        runtime.spawn(run_actor(task_actor));
        Self { sender }
    }

    pub async fn sync(&self, node: Node) -> Result<()> {
        let (send, receive) =  oneshot::channel();
        let task = ContentTaskMessage::SyncNode { node, response: send };
        let _ = self.sender.send(task).await;
        receive.await.expect("Task actor cancelled")
    }
}

async fn run_actor(mut actor: ContentTasksActor) {
    while let Some(task) = actor.receiver.recv().await {
        actor.run_task(task).await;
    }
}
