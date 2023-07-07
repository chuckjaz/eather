use std::{
    collections::HashMap,
    ffi::{OsString, OsStr}, sync::Arc, time::{SystemTime, Duration}, vec,
};

use bytes::{Bytes, BytesMut, BufMut};
use fuser::{Filesystem, FileAttr, FileType};
use futures::StreamExt;
use libc::ENOENT;
use log::info;

use crate::{
    content::{Id, Slot, Content, DirectoryEntry, Description},
    content_actor::ContentActorHandle,
    content_provider::{ContentProvider, ContentStore, SlotHolder},
};

const DIRECTORY_CHUNK_SIZE: usize = 1024;
         

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct INode {
    id: u64,
}

impl From<u64> for INode {
    fn from(value: u64) -> Self {
        Self { id: value }
    }
}

#[derive(Clone, Debug)]
enum NodeContentKey {
    Id(Id),
    Slot(Slot),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NodeContentKind {
    Directory,
    File,
}

impl Into<fuser::FileType> for NodeContentKind {
    fn into(self) -> fuser::FileType {
        match self {
            NodeContentKind::File => fuser::FileType::RegularFile,
            NodeContentKind::Directory => fuser::FileType::Directory,
        }
    }
}

#[derive(Clone)]
struct NodeContentInformation {
    inode: INode,
    key: NodeContentKey,
    kind: NodeContentKind,
    description: Option<Description>, 
    mtime: SystemTime,
    ttl: SystemTime,
    executable: bool,
}

#[derive(Copy, Clone, Debug)]
struct NodeDirectoryEntry {
    inode: INode,
    kind: NodeContentKind,
}

#[derive(Clone, Debug)]
struct NodeDirectoryNamedEntry {
    inode: INode,
    kind: NodeContentKind,
    name: OsString,
}

struct NodeDirectory {
    id: Id,
    entries: HashMap<OsString, NodeDirectoryEntry>
}

pub struct ContentFuse {
    actor: ContentActorHandle,
    content: HashMap<INode, NodeContentInformation>,
    entries: HashMap<INode, Arc<NodeDirectory>>,
    slot_to_inode: HashMap<Slot, INode>,
    id_to_inode: HashMap<Id, INode>,
    next_inode: u64,
    root_slot: Slot,
}

const SLOT_TTL: std::time::Duration = std::time::Duration::from_secs(30);
const SHA_TTL: std::time::Duration = std::time::Duration::from_secs(60 *  60 * 24 * 365);
const ZERO: std::time::Duration = std::time::Duration::new(0u64, 0u32);

fn now() -> SystemTime {
    SystemTime::now()
}

fn end_of_time() -> SystemTime {
    now() + SHA_TTL
}

fn slot_time_to_live() -> SystemTime {
    now() + SLOT_TTL
}

impl ContentFuse {
    pub fn new<
        Provider: ContentProvider + Send + Sync + 'static,
        Store: ContentStore + Send + Sync + 'static,
        Slots: SlotHolder + Send + Sync + 'static,
    >(
        provider: Provider,
        store: Store,
        slots: Slots,
        root_slot: Slot
    ) -> crate::result::Result<Self> {
        let root_inode = INode{ id: 1 };
        let root_node_info = NodeContentInformation {
            inode: root_inode,
            key: NodeContentKey::Slot(root_slot),
            kind: NodeContentKind::Directory,
            description: None,
            mtime: now(),
            ttl: now(),
            executable: false,
        };
        let mut content: HashMap<INode, NodeContentInformation> = HashMap::new();
        content.insert(root_inode, root_node_info);
        let actor = ContentActorHandle::new(provider, store, slots)?;
        Ok(Self {
            actor,
            content,
            entries: HashMap::new(),
            slot_to_inode: HashMap::new(),
            id_to_inode: HashMap::new(),
            next_inode: 2,
            root_slot,
        })
    }

    fn inode_of_id(&mut self, id: Id) -> INode {
        if let Some(inode) = self.id_to_inode.get(&id) {
            *inode
        } else {
            let inode = INode { id: self.next_inode };
            self.next_inode += 1;
            self.id_to_inode.insert(id, inode);
            inode
        }
    }

    fn inode_of_slot(&mut self, slot: Slot) -> INode {
        if let Some(inode) = self.slot_to_inode.get(&slot) {
            *inode
        } else {
            let inode = INode { id: self.next_inode };
            self.next_inode += 1;
            self.slot_to_inode.insert(slot, inode);
            inode
        }
    }

    async fn updated_info(&mut self, inode: INode) -> crate::result::Result<NodeContentInformation> {
        info!("ContentFuse::updated_info: {inode:?}");
        let info = self.content.get_mut(&inode);
        if let Some(info) = info {
            if info.ttl < now() {
                let key = &info.key;
                return match key {
                    NodeContentKey::Id(_) => {
                        info.ttl = end_of_time();
                        Ok(info.clone())
                    },
                    NodeContentKey::Slot(slot) => {
                        info.ttl = now() + SLOT_TTL;
                        let description = self.actor.get_slot(*slot).await?;
                        info.description = Some(description);
                        Ok(info.clone())
                    }
                }
            }
            return Ok(info.clone())
        };
        Err("No slot found".into())
    }

    async fn directory(&mut self, inode: INode) -> crate::result::Result<Arc<NodeDirectory>> {
        let content_info = self.updated_info(inode).await?;
        if content_info.kind != NodeContentKind::Directory {
            Err("inode is not a directory")?
        }
        
        if content_info.ttl > now() {
            if let Some(node_directory) = self.entries.get(&inode) {
                if let Some(description) = content_info.description {
                    if node_directory.id == description.id {
                        return Ok(node_directory.clone())
                    }
                }
            }
        }

        let description = if let Some(description) = content_info.description {
            description
        } else {
            return Err("Invalid slot".into())
        };
        let id = description.id;
        let dir_content = self.actor.get_directory(id).await?;
        let mut entries: HashMap<OsString, NodeDirectoryEntry> = HashMap::new();
        for entry in dir_content.entries {
            let (kind, info) = match entry {
                DirectoryEntry::File(info) => (NodeContentKind::File, info),
                DirectoryEntry::Directory(info) => (NodeContentKind::Directory, info)
            };
            let name: OsString = info.name.into();
            let mtime: SystemTime = info.modify_time.into();
            let (key, size) = match info.content {
                Content::Described(description) => (NodeContentKey::Id(description.id), Some(description.size)),
                Content::Node(slot) => (NodeContentKey::Slot(slot), None)
            };
            let (content, inode) = match info.content {
                Content::Described(description) => {
                    let inode = self.inode_of_id(description.id);
                    let info = NodeContentInformation {
                        inode,
                        key,
                        kind,
                        mtime,
                        description: Some(description),
                        ttl: if size.is_some() { end_of_time() } else { now() },
                        executable: info.executable,
                    };
                    (info, inode)
                },
                Content::Node(slot) => {
                    let inode = self.inode_of_slot(slot);
                    let info = NodeContentInformation {
                        inode,
                        key,
                        kind,
                        mtime,
                        description: None,
                        ttl: now(),
                        executable: info.executable,

                    };
                    (info, inode)
                }
            };
            self.content.insert(inode, content);
            entries.insert(name, NodeDirectoryEntry { inode, kind });
        }
        let directory = Arc::new(NodeDirectory { id, entries });
        self.entries.insert(inode, directory.clone());
        Ok(directory.clone())
    }

    async fn lookup_from(&mut self, parent: INode, name: &OsStr) -> crate::result::Result<Option<&NodeContentInformation>> {
        let directory = self.directory(parent).await?;
        if let Some(entry) = directory.entries.get(name) {
            if let Some(info) = self.content.get(&entry.inode) {
                Ok(Some(info))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn lookup_from_sync(&mut self, content: INode, name: &OsStr) -> crate::result::Result<Option<&NodeContentInformation>> {
        let runtime = self.actor.runtime();
        runtime.block_on(self.lookup_from(content, name))
    }

    fn info_to_file_attr(info: &NodeContentInformation, req: &fuser::Request<'_>) -> FileAttr {
        let size = info.description.and_then(|d| { Some(d.size) }).unwrap_or(0u64);
        let kind = match info.kind {
            NodeContentKind::Directory => FileType::Directory,
            NodeContentKind::File => FileType::RegularFile
        };
        let uid = req.uid();
        let gid = req.gid();
        let perm = if kind == FileType::Directory { 0o744 } else { 
            if info.executable { 0o744 } else { 0o644 }
        };
        FileAttr {
            ino: info.inode.id,
            size, 
            blocks: 1, 
            atime: now(), 
            mtime: info.mtime, 
            ctime: info.mtime, 
            crtime: info.mtime, 
            kind, 
            perm, 
            nlink: 1, 
            uid, gid, 
            rdev: 0, 
            blksize: 1, 
            flags: 0,
        }
    }

    async fn get_file_attr(&mut self, inode: INode, req: &fuser::Request<'_>) -> crate::result::Result<(FileAttr, Duration)> {
        if let Some(info) = self.content.get(&inode) {
            let attr = Self::info_to_file_attr(info, &req);
            let ttl = info.ttl.duration_since(now()).unwrap_or(ZERO);
            Ok((attr, ttl))
        } else {
            Err("Unknown inode")?
        }
    }

    fn get_file_attr_sync(&mut self, inode: INode, req: &fuser::Request<'_>) -> crate::result::Result<(FileAttr, Duration)> {
        let runtime = self.actor.runtime();
        runtime.block_on(self.get_file_attr(inode, req))
    }

    async fn read(&mut self, inode: INode, offset: i64, size: u32) -> crate::result::Result<Bytes> {
        info!("ContentFuse::read(inode: {inode:?}, offset: {offset}, size: {size})");
        let info = self.updated_info(inode).await?;
        let id = if let Some(description) = info.description {
            description.id
        } else {
            Err("No content")?;
            Id::Sha256([0; 32])
        };
        let mut skip: i128 = offset.into();
        let mut needed: i128 = size.into();
        let mut stream = self.actor.get_stream(id).await?;
        let mut result = BytesMut::new();
        while let Some(data) = stream.next().await {
            let data = data?;
            let len: i128 = data.len().try_into()?;
            let byte_offset: i128 = if skip > 0 {
                skip -= len;
                if skip > 0 { continue; }
                let byte_offset = -skip;
                skip = 0;
                let result: usize = byte_offset.try_into()?;
                result.try_into()?
            } else {
                0
            };
            
            let available = len - byte_offset;
            let byte_offset: usize = byte_offset.try_into()?;
            if available < needed {
                result.put(&data[byte_offset..]);
                needed = 0;
            } else {
                let to_send: usize = available.try_into()?;
                result.put(&data[byte_offset..byte_offset + to_send]);
                needed -= available;
            }
        }
        Ok(result.into())
    }

    fn read_sync(&mut self, inode: INode, offset: i64, size: u32) -> crate::result::Result<Bytes> {
        info!("ContentFuse::Filesystem::read_sync(inode: {inode:?}, offset: {offset}, size: {size})");
        let runtime = self.actor.runtime();
        runtime.block_on(self.read(inode, offset, size))
    }

    async fn read_directory(&mut self, inode: INode, offset: i64, chunk_size: usize) -> crate::result::Result<Vec<NodeDirectoryNamedEntry>> {
        info!("ContentFuse::Filesystem::read_directory(inode: {inode:?}, offset: {offset})");
        let directory = self.directory(inode).await?;
        let offset: usize = offset.try_into()?;
        let result: Vec<NodeDirectoryNamedEntry>  = directory.entries.iter().skip(offset).take(chunk_size)
            .map(|entry| { 
                NodeDirectoryNamedEntry {
                    inode: entry.1.inode, 
                    kind: entry.1.kind,
                    name: entry.0.clone(),
                }
            }).collect();
        Ok(result)
    }

    fn read_directory_sync(&mut self, inode: INode, offset: i64, chunk_size: usize) -> crate::result::Result<Vec<NodeDirectoryNamedEntry>>  {
        let runtime = self.actor.runtime();
        runtime.block_on(self.read_directory(inode, offset, chunk_size))
    }
}

impl Filesystem for ContentFuse { 
    fn lookup(&mut self, req: &fuser::Request<'_>, parent: u64, name: &OsStr, reply: fuser::ReplyEntry) {
        info!("ContentFuse::Filesystem::lookup({parent}, {name:?})");
        let parent_node = INode { id: parent };
        if let Ok(Some(info)) = self.lookup_from_sync(parent_node, name) {
            let attr = Self::info_to_file_attr(&info, req);
            let ttl = info.ttl.duration_since(now()).unwrap_or(ZERO);
            reply.entry(&ttl, &attr, 1);
            return;
        };
        reply.error(ENOENT);
    }

    fn getattr(&mut self, req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyAttr) {
        info!("ContentFuse::Filesystem::getattr({ino})");
        let inode = INode { id: ino };
        if let Ok((file_attr, ttl)) = self.get_file_attr_sync(inode, req) {
            reply.attr(&ttl, &file_attr)
        } else {
            reply.error(ENOENT)
        }
     }

     fn read(
             &mut self,
             _req: &fuser::Request<'_>,
             ino: u64,
             _fh: u64,
             offset: i64,
             size: u32,
             _flags: i32,
             _lock_owner: Option<u64>,
             reply: fuser::ReplyData,
         ) {
        info!("ContentFuse::Filesystem::read(ino={ino}, offset={offset}, size={size})");
        let inode = INode { id: ino };
        if let Ok(bytes) = self.read_sync(inode, offset, size) {
            reply.data(&bytes[..])
        } else {
            reply.error(ENOENT)
        }
     }

     fn readdir(
             &mut self,
             _req: &fuser::Request<'_>,
             ino: u64,
             _fh: u64,
             offset: i64,
             mut reply: fuser::ReplyDirectory,
         ) {
         let inode = INode { id: ino };
         let mut offset = offset;
         info!("ContentFuse::readdir(ino={ino}, offset={offset})");
         if offset == 0 {
            loop {
                let entries_result = self.read_directory_sync(inode, offset, DIRECTORY_CHUNK_SIZE);
                if let Ok(entries) = entries_result {
                    let len = entries.len();
                    for entry in entries {
                        info!("readdir: emit offset {offset} {entry:?}");
                        offset += 1;
                        if reply.add(entry.inode.id, offset, entry.kind.into(), entry.name) {
                            break;
                        }
                    }
                    if len < DIRECTORY_CHUNK_SIZE {
                        break
                    }
                } else {
                    log::error!("Error encountered: {:?}", entries_result);
                    break;
                }
            }
        }
        reply.ok();
     }

}
