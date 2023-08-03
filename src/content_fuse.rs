use std::{sync::Arc, ffi::OsStr, time::{SystemTime, Duration}};
use fuser::{Filesystem, FileAttr, FileType, Request, ReplyEntry, ReplyDirectory};
use libc::ENOENT;
use log::info;
use tokio::runtime::Runtime;

use crate::{content_file_layer::{FileContent, LayerContentInformation, LayerContentKind}, result::{Result, require}};

pub struct FileContentFuse<T: FileContent> {
    content: T,
    runtime: Arc<Runtime>,
}

impl<T: FileContent> Filesystem for FileContentFuse<T> {
    fn lookup(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        info!("Filesystem::lookup({parent}, {name:?})");
        let runtime = self.runtime.clone();
        let info = runtime.block_on(self.content_lookup(parent, name));
        match info {
            Ok(info) => {
                let (attr, ttl) = info_to_file_attr(&info, req);
                reply.entry(&ttl, &attr, 1);
            },
            Err(_) => {
                reply.error(ENOENT)
            } 
        }
    }

    fn getattr(&mut self, req: &Request<'_>, ino: u64, reply: fuser::ReplyAttr) {
        info!("Filesystem::getattr({ino})");
        let info = self.runtime.block_on(self.content.info(ino));
        match info {
            Ok(info) => {
                let (attr, ttl) = info_to_file_attr(&info, req);
                reply.attr(&ttl, &attr)
            },
            Err(_) => reply.error(ENOENT) 
        }
    }

    fn read(
            &mut self,
            _req: &Request<'_>,
            ino: u64,
            _fh: u64,
            offset: i64,
            size: u32,
            _flags: i32,
            _lock_owner: Option<u64>,
            reply: fuser::ReplyData,
        ) {
        let result = self.runtime.block_on(self.content.read_file(ino, offset, size.into()));
        match result {
            Ok(bytes) => reply.data(&bytes[..]),
            Err(_) => reply.error(ENOENT) 
        }
    }

    fn readdir(
            &mut self,
            _req: &Request<'_>,
            ino: u64,
            _fh: u64,
            offset: i64,
            mut reply: ReplyDirectory,
        ) {
        info!("Filesystem::readdir({ino}, {offset})");
        let result = self.runtime.block_on(self.content.read_directory(ino, offset, 65536));
        match result {
            Ok(vector) => {
                info!("Filesystem::readdir - {}", vector.len());
                let mut off = offset;
                for entry in vector {
                    let kind = if entry.kind == LayerContentKind::Directory { FileType::Directory } else { FileType::RegularFile };
                    if reply.add(entry.node, off + 1, kind, entry.name) {
                        break
                    }
                    off += 1;    
                }
                reply.ok()
            },
            Err(_) => reply.error(ENOENT)
        }
    }

    fn write(
            &mut self,
            _req: &Request<'_>,
            ino: u64,
            _fh: u64,
            offset: i64,
            data: &[u8],
            _write_flags: u32,
            _flags: i32,
            _lock_owner: Option<u64>,
            reply: fuser::ReplyWrite,
        ) {
        info!("Filesystem::write({ino}, {offset})");
        let result = self.runtime.block_on(self.content.write_file(ino, offset, data));
        match result {
            Ok(size) => {
                let size: u32 = size.try_into().unwrap();
                reply.written(size)
            },
            Err(_) => reply.error(ENOENT)
        }
    }

    fn mknod(
            &mut self,
            req: &Request<'_>,
            parent: u64,
            name: &OsStr,
            _mode: u32,
            _umask: u32,
            _rdev: u32,
            reply: ReplyEntry,
        ) {
        info!("Filesystem::mknod({parent}, {name:?})");
        let runtime = self.runtime.clone();
        let result = runtime.block_on(self.create_file(parent, name));

        match result {
            Ok(info) => {
                let (attr, ttl) = info_to_file_attr(&info, &req);
                reply.entry(&ttl, &attr, 1)
            },
            Err(_) => reply.error(ENOENT)
        }
    }

    fn mkdir(
            &mut self,
            req: &Request<'_>,
            parent: u64,
            name: &OsStr,
            _mode: u32,
            _umask: u32,
            reply: ReplyEntry,
        ) {
        info!("Filesystem::mkdir({parent}, {name:?})");
        let runtime = self.runtime.clone();
        let result = runtime.block_on(self.create_directory(parent, name));
        match result {
            Ok(info) => {
                let (attr, ttl) = info_to_file_attr(&info, &req);
                reply.entry(&ttl, &attr, 1)
            },
            Err(_) => reply.error(ENOENT)
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        info!("Filesystem::unlink({parent}, {name:?})");
        let result = self.runtime.block_on(self.content.remove_file(parent, name));
        match result {
            Ok(_) => {
                reply.ok()
            },
            Err(_) => reply.error(ENOENT)
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        info!("Filesystem::rmdir({parent}, {name:?})");
        let result = self.runtime.block_on(self.content.remove_directory(parent, name));
        match result {
            Ok(_) => {
                reply.ok()
            },
            Err(_) => reply.error(ENOENT)
        }
    }
}

impl<T: FileContent> FileContentFuse<T> {
    pub fn new(runtime: Arc<Runtime>, content: T) -> Self {
        Self { content, runtime }
    }

    async fn content_lookup(&mut self, parent: u64, name: &OsStr) -> Result<LayerContentInformation> {
        let node = require(self.content.lookup(parent, name).await?, "Unkonwn name")?;
        Ok(self.content.info(node).await?)
    }
    
    async fn create_file(&mut self, parent: u64, name: &OsStr) -> Result<LayerContentInformation> {
        let node = self.content.create_file(parent, name).await?;
        let info = self.content.info(node).await?;
        Ok(info)
    }

    async fn create_directory(&mut self, parent: u64, name: &OsStr) -> Result<LayerContentInformation> {
        let node = self.content.create_directory(parent, name).await?;
        let info = self.content.info(node).await?;
        Ok(info)
    }
}

fn info_to_file_attr(info: &LayerContentInformation, req: &Request<'_>) -> (FileAttr, Duration) {
    let uid = req.uid();
    let gid = req.gid();
    let kind = if info.kind == LayerContentKind::Directory { FileType::Directory } else { FileType::RegularFile };
    let perm = if kind == FileType::Directory { 0o744 } else { 
        if info.executable { 0o744 } else { 0o644 }
    };
    let n = now();
    let duration = if info.ttl < n { ZERO } else { info.ttl.duration_since(n).unwrap() };
    let attr = FileAttr {
        ino: info.node,
        size: info.size().try_into().unwrap_or(0),
        blocks: 1,
        atime: info.mtime,
        mtime: info.mtime,
        crtime: info.mtime,
        ctime: info.mtime,
        kind,
        perm,
        nlink: 1,
        uid,
        gid,
        rdev: 0,
        blksize: 1,
        flags: 0,
    };
    (attr, duration)    
}

fn now() -> SystemTime {
    SystemTime::now()
}

const ZERO: std::time::Duration = std::time::Duration::new(0u64, 0u32);