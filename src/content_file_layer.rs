use async_recursion::async_recursion;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut, BufMut};
use log::info;
use ring::digest::{Context, SHA256};
use tokio::{sync::RwLock, io::{AsyncWrite, AsyncWriteExt}};
use std::{ffi::{OsStr, OsString}, time::SystemTime, collections::{HashMap, HashSet}, sync::Arc, alloc::System, cmp::Ordering};
use futures::StreamExt;

use crate::{result::Result, content::{Slot, Id, Description, Directory, DirectoryEntry, Content, EntryInformation}, content_actor::ContentActorHandle};

pub type Node = u64;

pub const ROOT: Node = 1;

#[async_trait]
pub trait FileContent {
    async fn lookup(&mut self, parent: Node, name: &OsStr) -> Result<Option<Node>>;
    async fn info(&mut self, node: Node) -> Result<LayerContentInformation>;

    async fn create_file(&mut self, parent: Node, name: &OsStr) -> Result<Node>;
    async fn read_file(&mut self, node: Node, offset: i64, size: i64) -> Result<Bytes>;
    async fn write_file(&mut self, node: Node, offset: i64, data: &[u8]) -> Result<i64>;
    async fn set_attributes(&mut self, node: Node, executable: bool) -> Result<()>;
    async fn remove_file(&mut self, parent: Node, name: &OsStr) -> Result<()>; 
    async fn allocate_file_space(&mut self, node: Node, offset: i64, size: i64) -> Result<()>;

    async fn create_directory(&mut self, parent: Node, name: &OsStr) -> Result<Node>;
    async fn read_directory(&mut self, node: Node, offset: i64, count: i64) -> Result<Vec<LayerDirectoryEntry>>;
    async fn remove_directory(&mut self, parent: Node, name: &OsStr) -> Result<()>;
    
    async fn mount_directory(&mut self, parent: Node, name: &OsStr, slot: Slot) -> Result<()>;
    async fn mount_file(&mut self, parent: Node, name: &OsStr, slot: Slot) -> Result<()>;
    async fn sync(&mut self, node: Node) -> Result<()>;
}

fn new(actor: ContentActorHandle, root: Slot) -> impl FileContent {
    let root_info = LayerContentInformation {
        node: ROOT,
        kind: LayerContentKind::Directory,
        content: None,
        mtime: now(),
        ttl: now(),
        executable: false,
    };
    let mut content: HashMap<Node, LayerContentInformation> = HashMap::new();
    content.insert(ROOT, root_info);
    let mut mount_points: HashMap<Node, Slot> = HashMap::new();
    mount_points.insert(ROOT, root);
    FileContentImpl {
        actor,
        content,
        entries: HashMap::new(),
        mount_points,
        node_to_parent: HashMap::new(),
        invalid_nodes: HashSet::new(),
        next_node: 2,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LayerContentKind {
    Directory,
    File,
}

#[derive(Clone, Debug)]
pub struct LayerSegment {
    pub content: LayerContent,
    pub offset: i64,
    pub size: i64,
}

#[derive(Clone, Debug)]
pub enum LayerContent {
    Segment(Box<LayerSegment>),
    Composite(Box<Vec<LayerContent>>, i64),
    Bytes(Box<Bytes>),
    Described(Box<Description>),
    Zero(i64),
    Empty
}

impl From<Description> for LayerContent {
    fn from(value: Description) -> Self {
        Self::Described(Box::new(value))
    }
}

impl From<&[u8]> for LayerContent {
    fn from(value: &[u8]) -> Self {
        let mut bytes = BytesMut::with_capacity(value.len());
        bytes.put(value);
        Self::Bytes(Box::new(bytes.freeze()))
    }
}

impl From<Bytes> for LayerContent {
    fn from(value: Bytes) -> Self {
        Self::Bytes(Box::new(value))
    }
}

impl LayerContent {
    pub fn size(&self) -> i64 {
        match self {
            Self::Segment(segment) => segment.size,
            Self::Composite(_, size) => *size,
            Self::Bytes(bytes) => bytes.len().try_into().unwrap(),
            Self::Described(description) => description.size,
            Self::Zero(size) => *size,
            Self::Empty => 0,
        }
    }

    pub fn segment(self, offset: i64, size: i64) -> LayerContent {
        if let Self::Segment(segment) = self {
            let offset = segment.offset + offset;
            assert!(offset + size < segment.size);
            Self::Segment(Box::new(LayerSegment { content: segment.content, offset, size }))
        } else {
            Self::Segment(Box::new(LayerSegment { content: self, offset, size }))
        }
    }

    pub fn composite(vector: Vec<LayerContent>, size: i64) -> LayerContent {
        Self::Composite(Box::new(vector), size)
    }
}

#[derive(Clone, Debug)]
pub struct LayerDirectoryEntry {
    pub name: String,
    pub node: Node,
    pub kind: LayerContentKind,
}

pub struct LayerDirectory {
    pub entries: HashMap<OsString, LayerDirectoryEntry>,
}

#[derive(Clone)]
pub struct LayerContentInformation {
    pub node: Node,
    pub kind: LayerContentKind,
    pub content: Option<LayerContent>, 
    pub mtime: SystemTime,
    pub ttl: SystemTime,
    pub executable: bool,
}

struct FileContentImpl {
    actor: ContentActorHandle,
    content: HashMap<Node, LayerContentInformation>,
    entries: HashMap<Node, Arc<RwLock<LayerDirectory>>>,
    mount_points: HashMap<Node, Slot>,
    node_to_parent: HashMap<Node, Node>,
    invalid_nodes: HashSet<Node>,
    next_node: u64,
}

#[async_trait]
impl FileContent for FileContentImpl {
    async fn lookup(&mut self, parent: Node, name: &OsStr) -> Result<Option<Node>> {
        info!("FileContent::lookup({parent}, {name:?})");
        let directory = self.directory(parent).await?;
        let entries = &directory.read().await.entries;
        Ok(
            if let Some(entry) = entries.get(name) {
               Some(entry.node)
            } else {
                None
            }
        )
    }

    async fn info(&mut self, node: Node) -> Result<LayerContentInformation> {
        info!("FileContent::info({node})");
        self.info_for_node(node)
    }

    async fn create_file(&mut self, parent: Node, name: &OsStr) -> Result<Node> {
        info!("FileContent::create_file({parent}, {name:?})");
        self.create_node(parent, name, LayerContentKind::File).await
    }

    async fn read_file(&mut self, node: Node, offset: i64, size: i64) -> Result<Bytes> {
        info!("FileContent::read_file({node}, {offset}, {size})");
        let content = self.content(node).await?;
        let result = self.bytes_of_content(&content, offset, size).await?;
        Ok(result)
    }

    async fn write_file(&mut self, node: Node, offset: i64, data: &[u8]) -> Result<i64> {
        info!("FileContent::write_file({node}, {offset}");
        let content = self.content(node).await?;
        let content_size = content.size();
        let data_size: i64 = data.len().try_into()?;
        let offset = if offset < 0 { content_size + offset } else { offset };
        let (new_content, new_size) = if offset == content_size {
            let mut vector = if let LayerContent::Composite(vector, _) = content {
                *vector.clone()
            } else { 
                vec![content]
            };
            vector.push(data.into());
            let new_size = offset + data_size;
            (LayerContent::composite(vector, new_size), new_size)
        } else if offset > content_size {
            let zero_size = offset - content_size;
            let new_size = offset + content_size;
            let vector = vec![content, LayerContent::Zero(zero_size), data.into()];
            let new_content = LayerContent::composite(vector, new_size);
            (new_content, new_size)
        } else if offset == 0 {
            if data_size == content_size {
                (data.into(), data_size)
            } else if data_size < content_size {
                let vector = vec![data.into(), content.segment(data_size, content_size- data_size)];
                (LayerContent::composite(vector, content_size), content_size)
            } else {
                let last_offset = offset + data_size;
                let last_size = content_size - last_offset;
                let vector = vec![data.into(), content.segment(last_offset, last_size)];
                (LayerContent::composite(vector, content_size), content_size)
            }
        } else {
            let last_offset = offset + data_size;
            let last_size = content_size - last_offset;
            let vector = vec![content.clone().segment(0, offset), data.into(), content.segment(last_offset, last_size)];
            (LayerContent::composite(vector, content_size), content_size)
        };
        let info = self.content.get_mut(&node).unwrap();
        info.content = Some(new_content);
        Ok(new_size)
    }

    async fn set_attributes(&mut self, node: Node, executable: bool) -> Result<()> {
        info!("FileContent::set_attributes({node}, {executable})");
        let info = self.content.get_mut(&node);
        if let Some(info) = info {
            if info.executable != executable {
                info.executable = executable;
                self.invalid_nodes.insert(node);
            }
        } else {
            return Err("Invalid node".into())
        }
        Ok(())
    }

    async fn remove_file(&mut self, parent: Node, name: &OsStr) -> Result<()> {
        info!("FileContent::remove_file({parent}, {name:?})");
        self.remove_node(parent, name, LayerContentKind::File).await
    }

    async fn allocate_file_space(&mut self, node: Node, offset: i64, size: i64) -> Result<()> {
        todo!("FileContent::allocate_file_space({node}, {offset}, {size})");
    }

    async fn create_directory(&mut self, parent: Node, name: &OsStr) -> Result<Node> {
        info!("FileContent::create_directory({parent}, {name:?})");        
        self.create_node(parent, name, LayerContentKind::Directory).await
    }

    async fn read_directory(&mut self, node: Node, offset: i64, count: i64) -> Result<Vec<LayerDirectoryEntry>> {
        info!("FileContent::read_directory({node}, {offset}, {count})");
        let directory = self.directory(node).await?;
        let entries = &directory.read().await.entries;
        let offset: usize = offset.try_into()?;
        let count: usize = count.try_into()?;
        let result: Vec<_> = entries
            .values()
            .skip(offset)
            .take(count)
            .map(|entry| { entry.clone() }).collect();
        Ok(result)
    }

    async fn remove_directory(&mut self, parent: Node, name: &OsStr) -> Result<()> {
        info!("FileContent::remove_directory({parent}, {name:?})");
        self.remove_node(parent, name, LayerContentKind::Directory).await
    }
    
    async fn mount_directory(&mut self, parent: Node, name: &OsStr, slot: Slot) -> Result<()> {
        info!("FileContent::mount_directory({parent}, {name:?}, {slot})");
        self.mount_node(parent, name, slot, LayerContentKind::Directory).await
    }

    async fn mount_file(&mut self, parent: Node, name: &OsStr, slot: Slot) -> Result<()> {
        info!("FileContent::mount_file({parent}, {name:?}, {slot})");
        self.mount_node(parent, name, slot, LayerContentKind::File).await
    }

    async fn sync(&mut self, node: Node) -> Result<()> {
        info!("FileContent::sync({node})");
        if let Some(invalid) = self.root_invalid(node) {
            self.sync_node(invalid).await?;
        }
        Ok(())
    }
}

trait SomeOrError<T> {
    fn some_or_error(&self, message: &str) -> Result<T>;
}

impl FileContentImpl {
    async fn content(&mut self, node: Node) -> Result<LayerContent> {
        let info = self.info_for_node(node)?;
        if let Some(content) = info.content {
            if info.ttl > now() {
                return Ok(content)
            }
        } 
        let slot = self.mount_points.get(&node).unwrap();
        let description = self.actor.get_slot(*slot).await?;
        let content: LayerContent = description.into();
        let info = self.content.get_mut(&node).unwrap();
        info.content = Some(content.clone());
        info.ttl = slot_time_to_live();
        Ok(content)
    }

    async fn directory(&mut self, parent: Node) -> Result<Arc<RwLock<LayerDirectory>>> {
        let content_info = if let Some(content_info) = self.content.get_mut(&parent) {
            content_info
        } else {
            return Err("Unknown node".into());
        };
        if content_info.kind != LayerContentKind::Directory {
            return Err("Not a directory".into());
        }

        if content_info.ttl > now() {
            if let Some(directory) = self.entries.get(&parent) {
                return Ok(directory.clone())
            }
        }

        let content = self.content(parent).await?;
        let bytes = self.bytes_of_content(&content, 0i64, i64::MAX).await?;
        let directory: Directory = rmp_serde::decode::from_slice(&bytes)?;
        let mut entries: HashMap<OsString, LayerDirectoryEntry> = HashMap::new();
        for entry in directory.entries {
            let (kind, info) = match entry {
                DirectoryEntry::File(info) => (LayerContentKind::File, info),
                DirectoryEntry::Directory(info) => (LayerContentKind::Directory, info)
            };
            let node = self.next_node;
            self.next_node += 1; 
            let layer_entry = LayerDirectoryEntry {
                name: info.name.clone(),
                node,
                kind,
            };
            entries.insert(info.name.into(), layer_entry);
            self.node_to_parent.insert(node, parent);
            let content: Option<LayerContent> = match info.content {
                Content::Described(description) => Some(description.into()),
                Content::Node(slot) => {
                    self.mount_points.insert(node, slot);
                    None
                }
            };
            let info = LayerContentInformation {
                node,
                kind,
                content,
                ttl: now(),
                mtime: info.modify_time.into(),
                executable: info.executable,
            };
            self.content.insert(node, info);
        }
        let directory = Arc::new(RwLock::new(LayerDirectory { entries }));
        self.entries.insert(parent, directory.clone());
        
        Ok(directory)
    }

    async fn create_node(&mut self, parent: Node, name: &OsStr, kind: LayerContentKind) -> Result<Node> {
        let directory = self.directory(parent).await?;
        if let Some(_) = directory.read().await.entries.get(name) {
            return Err("Name already exists".into());
        }
        let node = self.next_node;
        self.next_node += 1;
        let content = LayerContent::Empty;
        if kind == LayerContentKind::Directory {
            let new_directory = LayerDirectory { entries: HashMap::new() };
            self.entries.insert(node, Arc::new(RwLock::new(new_directory)));
        }
        let info = LayerContentInformation {
            node,
            kind,
            content: Some(content),
            mtime: now(),
            ttl: end_of_time(),
            executable: false,
        };
        let entry_name: String = name.to_string_lossy().into();
        self.content.insert(node, info);
        let entry = LayerDirectoryEntry {
            name: entry_name,
            kind,
            node,
        };
        directory.write().await.entries.insert(name.into(), entry);
        self.invalid_nodes.insert(node);
        self.invalid_nodes.insert(parent);
        Ok(node)
    }

    async fn mount_node(&mut self, parent: Node, name: &OsStr, slot: Slot, kind: LayerContentKind) -> Result<()> {
        let node = self.create_node(parent, name, kind).await?;
        let info = self.content.get_mut(&node).unwrap();
        info.content = None;
        self.mount_points.insert(node, slot);
        Ok(())
    }

    async fn remove_node(&mut self, parent: Node, name: &OsStr, kind: LayerContentKind) -> Result<()> {
        let directory = self.directory(parent).await?;
        let entries = &mut directory.write().await.entries;
        if let Some(entry) = entries.get(name) {
            if entry.kind != kind {
                match kind {
                    LayerContentKind::File => Err("Expected a file".into()),
                    LayerContentKind::Directory => Err("Expected a directory".into())
                }
            } else {
                self.mount_points.remove(&entry.node);
                self.content.remove(&entry.node);
                entries.remove(name);
                self.invalid_nodes.insert(parent);
                Ok(())
            }
        } else {
            match kind {
                LayerContentKind::File => Err("No file".into()),
                LayerContentKind::Directory => Err("No directory".into())
            }
        }
    }

    async fn bytes_of_content(&mut self, content: &LayerContent, offset: i64, size: i64) -> Result<Bytes> {
        let mut result = BytesMut::new();
        self.bytes_of_content_inner(content, offset, size, &mut result).await?;
        Ok(result.freeze())
    }

    #[async_recursion]
    async fn bytes_of_content_inner(&mut self, content: &LayerContent, offset: i64, size: i64, result: &mut BytesMut) -> Result<()> {
        match content {
            LayerContent::Bytes(bytes) => {
                let offset: usize = offset.try_into().unwrap();
                let size: usize = usize::min(size.try_into().unwrap(), bytes.len() - offset);
                result.extend_from_slice(&bytes[offset..offset + size]);
            },
            LayerContent::Composite(nested, _) => {
                let mut size = size;
                let mut offset = offset;
                for segment in nested.iter() {
                    let prior: i64 = result.len().try_into().unwrap();
                    self.bytes_of_content_inner(segment, offset, size, result).await?;
                    let written: i64 = result.len().try_into().unwrap();
                    let used_size = written - prior;

                    size -= used_size;
                    offset += used_size;
                    if size <= 0 { break }
                }
            },
            LayerContent::Segment(segment) => {
                let new_offset = offset + segment.offset;
                let new_size = size.min(segment.size);
                self.bytes_of_content_inner(&segment.content, new_offset, new_size, result).await?
            },
            LayerContent::Described(description) => {
                self.bytes_from_id(description.id, offset, size, result).await?;
            },
            LayerContent::Zero(size) => {
                let size: usize = (*size).try_into().unwrap();
                result.resize(result.len() + size, 0)
            },
            LayerContent::Empty => { }
        }
        Ok(())
    }

    async fn sync_file(&mut self, node: Node) -> Result<Description> {
        let content = self.content(node).await?;
        if self.is_invalid(node) {
            Ok(self.write_content(node, content).await?)
        } else if let LayerContent::Described(description) = content {
            Ok(*description)
        } else {
            Ok(self.write_content(node, content).await?)     
        }
    }

    fn is_invalid(&self, node: Node) -> bool {
        self.invalid_nodes.get(&node).is_some()
    }

    async fn write_content(&mut self, node: Node, content: LayerContent) -> Result<Description> {
        let content = self.content(node).await?;
        let size = content.size();
        let mut context = Context::new(&SHA256);
        self.hash_content(&content, 0, size, &mut context).await?;
        let hash = context.finish();
        let id = Id::from_sha256(hash.as_ref());
        let mut writer = self.actor.put(id).await?;
        self.write_content_inner(&content, 0, size, &mut writer).await?;
        let info = self.content.get_mut(&node).unwrap();
        let description = Description { id, size };
        info.content = Some(description.into());
        info.ttl = if let Some(slot) = self.mount_points.get(&node) {
            self.actor.update_slot(*slot, description).await?;
            slot_time_to_live()
        } else {
            end_of_time()
        };
        self.invalid_nodes.remove(&node);
        Ok(Description { id, size })
    }

    #[async_recursion]
    async fn sync_directory(&mut self, node: Node) -> Result<Description> {
        if self.is_invalid(node) {
            let layer_directory  = if let Some(entries) = self.entries.get(&node).cloned() {
                entries
            } else {
                return Err("Unknown node".into())
            };
            let layer_directory = layer_directory.read().await;
    
            // produce serialized directory
            let mut directory = Directory { entries: Vec::new() };

            for layer_entry in layer_directory.entries.values().into_iter() {
                let description = self.sync_node(node).await?;
                let content = if let Some(slot) = self.mount_points.get_mut(&node) {
                    Content::Node(*slot)
                } else {
                    Content::Described(description)
                };
                let info = self.content.get(&layer_entry.node).unwrap();
                let entry_info = EntryInformation {
                    name: layer_entry.name.clone(),
                    modify_time: info.mtime.into(),
                    content,
                    executable: info.executable,
                };
                let entry = if info.kind == LayerContentKind::Directory {
                    DirectoryEntry::Directory(entry_info)
                } else {
                    DirectoryEntry::File(entry_info)
                };
                directory.entries.push(entry);
            }
            directory.entries.sort_by(|a, b| { a.compare(b) });
            let new_content: Bytes = rmp_serde::encode::to_vec(&directory)?.into();
            let info = self.content.get_mut(&node).unwrap();
            info.content = Some(new_content.into());
        }
         
        self.sync_file(node).await
    }

    async fn sync_node(&mut self, node: Node) -> Result<Description> {
        let info = if let Some(info) = self.content.get(&node) {
            info
        } else {
            return Err("Invalid node".into())
        };
        if info.kind == LayerContentKind::Directory {
            self.sync_directory(node).await
        } else {
            self.sync_file(node).await
        }
    }

    #[async_recursion]
    async fn write_content_inner(&mut self, content: &LayerContent, offset: i64, size: i64, writer: &mut Box<dyn AsyncWrite + Unpin + Send>) -> Result<()> {
        match content {
            LayerContent::Bytes(bytes) => {
                let write_end: usize = (offset + size).try_into()?;
                let write_offset: usize = offset.try_into()?;
                let _ = writer.write(&bytes[write_offset..write_end]).await?;
            },
            LayerContent::Composite(vector, _) => {
                let mut running_offset = offset;
                let mut running_size = offset;
                for nested in vector.iter() {
                    let nested_size = nested.size();
                    let size = running_size.min(nested_size);
                    self.write_content_inner(&nested, running_offset, size, writer).await?;
                    running_offset += size;
                    running_size -= size;
                    if running_size == 0 {
                        break
                    }
                };
            },
            LayerContent::Described(description) => {
                let bytes = self.bytes_of_content(content, 0, description.size).await?;
                let write_end: usize = (offset + size).try_into()?;
                let write_offset: usize = offset.try_into()?;
                let _ = writer.write(&bytes[write_offset..write_end]).await?;
            },
            LayerContent::Empty => { },
            LayerContent::Segment(segment) => {
                let segment_offset = offset + segment.offset;
                let segment_size = size.min(segment.size);
                self.write_content_inner(&segment.content, segment_offset, segment_size, writer).await?;
            },
            LayerContent::Zero(_) => {
                for _ in 1..size {
                    writer.write_u8(0).await?;
                }
            }
        };
        Ok(())
    }

    #[async_recursion]
    async fn hash_content(&mut self, content: &LayerContent, offset: i64, size: i64, context: &mut Context) -> Result<(i64, i64)> {
        return match content {
            LayerContent::Bytes(bytes) => {
                let bytes_size: i64 = bytes.len().try_into()?;
                let end = offset + size;
                let offset: usize = offset.try_into()?;
                let end: usize = usize::max(offset, usize::min(end.try_into()?, bytes.len()));
                context.update(&bytes[offset..end]);
                let size: i64 = bytes.len().try_into()?;
                Ok((bytes_size, size))
            },
            LayerContent::Composite(vector, _) => {
                let mut result_size: i64 = 0;
                let mut running_offset: i64 = offset;
                let mut running_size: i64 = size;
                
                for item in vector.iter() {
                    let (size, actual_size) = self.hash_content(item, running_offset, running_size,  context).await?;
                    result_size += actual_size;
                    running_offset += size;
                    running_size -= size;
                    if running_size < 0 {
                        break;
                    }
                }
                Ok((result_size, running_size))
            },
            LayerContent::Described(descripton) => {
                let bytes = self.bytes_of_content(&content, offset, size).await?;
                context.update(&bytes[..]);
                Ok((bytes.len().try_into()?, descripton.size))
            },
            LayerContent::Segment(_) => {
                todo!()
            },
            LayerContent::Zero(_) => {
                let b: [u8; 1] = [0; 1];
                for _ in 1..size {
                    context.update(&b);
                }
                Ok((size, size))
            },
            LayerContent::Empty => {
                Ok((0, 0))
            }
        }
    }

    async fn bytes_from_id(&mut self, id: Id, offset: i64, size: i64, result: &mut BytesMut) -> Result<()> {
        let mut skip: usize = offset.try_into().unwrap();
        let mut needed: usize = size.try_into().unwrap();
        let mut stream = self.actor.get_stream(id).await?;
        while let Some(bytes) = stream.next().await {
            let bytes = bytes?;
            let len = bytes.len();
            if skip > len {
                skip -= bytes.len(); 
                continue 
            }
            let effective = if len > needed { needed } else { len };
            result.put(&bytes[skip..skip + effective]);
            needed -= effective;
            if effective == 0 { break };
        }
        Ok(())
    }

    fn info_for_node(&mut self, node: Node) -> Result<LayerContentInformation> {
        if let Some(info) = self.content.get(&node) {
            Ok(info.clone())
        } else {
            Err("Invalid node".into())
        }
    }

    fn root_invalid(&self, node: Node) -> Option<Node> {
        if let Some(node) = self.invalid_nodes.get(&node) {
            let mut current = *node;
            loop {
                if let Some(parent) = self.node_to_parent.get(&current) {
                    if let Some(_) = self.invalid_nodes.get(parent) {
                        current = *parent;
                    }
                    else {
                        return Some(current);
                    }
                } else {
                    return Some(current);
                }
            }
        } else {
            return None
        }
    }
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

#[cfg(test)]
mod tests {
    use std::{ffi::OsStr, sync::Arc};

    use ring::digest::{SHA256, Context};
    use tokio::{io::AsyncWriteExt, runtime::Runtime};
    use tokio_test::assert_ok;

    use crate::{memory_provider::MemoryProvider, content_actor::ContentActorHandle, content::{Directory, Id, Slot, Description}, content_provider::{ContentStore, SlotHolder}, content_file_layer::ROOT};

    use super::{FileContent, new};

    #[test]
    fn can_create_a_new_file_content() {
        let provider = MemoryProvider::new();
        let store = provider.clone();
        let slots = provider.clone();
        let actor = assert_ok!(ContentActorHandle::new(provider, store, slots));
        let slot_bytes: [u8; 32] = [0; 32];
        let slot = Slot::ed25519(&slot_bytes);
        let _ = new(actor, slot);
    }

    #[test]
    fn can_mount_empty_directory() {
        let runtime = new_runtime();
        runtime.clone().block_on(async {
            let (mut file_content, slot, _) = empty_content(runtime.clone()).await;
            assert_ok!(file_content.mount_directory(ROOT, OsStr::new("name"), slot).await);
        });
    }

    #[test]
    fn can_create_file_and_read_from_it() {
        let runtime = new_runtime();
        runtime.clone().block_on(async {
            let (mut file_content, _, _) = empty_content(runtime.clone()).await; 
            let node = assert_ok!(file_content.create_file(ROOT, OsStr::new("name")).await);
            let data = b"This is some test data";
            assert_ok!(file_content.write_file(node, 0, data).await);

            let read = assert_ok!(file_content.read_file(node, 0, 1024).await);
            assert_eq!(read.len(), data.len());
            for i in 0..read.len() {
                assert_eq!(data[i], read[i])
            }
        });
    }

    #[test]
    fn can_sync_a_file() {
        let runtime = new_runtime();
        runtime.clone().block_on(async {
            let (mut file_content, root, slots) = empty_content(runtime.clone()).await;
            let node = assert_ok!(file_content.create_file(ROOT, OsStr::new("some_file.txt")).await);
            let data = b"This is some test data";
            assert_ok!(file_content.write_file(node, 0, data).await);
            assert_ok!(file_content.sync(node).await);
            let new_root = assert_ok!(slots.current(root).await);
            let id = assert_ok!(Id::sha256("EgZq8zuXjGnratK8f1JYCy7Yp2g4X6NA2AK48WPLySaR"));
            assert_eq!(new_root.id, id);
        });
    }

    async fn store_empty_directory(store: &dyn ContentStore, slots: &dyn SlotHolder, slot: Slot) {
        let empty_directory = Directory {
            entries: Vec::new(),
        };
        let empty_directory_bytes = assert_ok!(rmp_serde::to_vec(&empty_directory));
        let id = id_of(&empty_directory_bytes.as_ref());
        let size: i64 = assert_ok!(empty_directory_bytes.len().try_into());
        let mut writer = assert_ok!(store.put(id).await); 
        assert_ok!(writer.write_all(empty_directory_bytes.as_ref()).await);
        assert_ok!(writer.shutdown().await);
        let description = Description {
            id,
            size
        };
        assert_ok!(slots.update(slot, description).await);
    }

    async fn empty_content(runtime: Arc<Runtime>) -> (impl FileContent, Slot, impl SlotHolder) {
        let memory = MemoryProvider::new();
        let provider = memory.clone();
        let store = memory.clone();
        let slots = memory.clone();
        let preload = memory.clone();
        let actor = ContentActorHandle::new_rt(provider, store, slots.clone(), runtime.clone());
        let slot_data: [u8; 32] = [0; 32];
        let slot = Slot::ed25519(&slot_data);
        store_empty_directory(&preload, &preload, slot).await;
        let file_content = new(actor, slot);
        (file_content, slot, slots)
    }

    fn new_runtime() -> Arc<Runtime> {
        Arc::new(tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
        )        
    }

    fn id_of(bytes: &[u8]) -> Id {
        let mut context = Context::new(&SHA256);
        context.update(bytes);
        let digest = context.finish();
        Id::from_sha256(digest.as_ref())
    }
}