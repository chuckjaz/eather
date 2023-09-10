use std::{sync::Arc, ffi::OsStr, time::{SystemTime, Duration}};
use bytes::Bytes;
use fuser::{Filesystem, FileAttr, FileType, Request, ReplyEntry, ReplyDirectory};
use libc::ENOENT;
use log::debug;
use tokio::{runtime::Runtime, sync::Mutex};

use crate::{content_file_layer::{FileContent, LayerContentInformation, LayerContentKind, LayerDirectoryEntry}, result::{Result, require}};

pub struct FileContentFuse {
    content: Arc<Mutex<dyn FileContent>>,
    runtime: Arc<Runtime>,
}

impl Filesystem for FileContentFuse {
    fn lookup(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        debug!("Filesystem::lookup({parent}, {name:?})");
        let runtime = self.runtime.clone();
        let info = runtime.block_on(self.lookup(parent, name));
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
        debug!("Filesystem::getattr({ino})");
        let runtime = self.runtime.clone();
        let info = runtime.block_on(self.info(ino));
        match info {
            Ok(info) => {
                let (attr, ttl) = info_to_file_attr(&info, req);
                reply.attr(&ttl, &attr)
            },
            Err(_) => reply.error(ENOENT)
        }
    }

    fn setattr(
            &mut self,
            req: &Request<'_>,
            ino: u64,
            _mode: Option<u32>,
            _uid: Option<u32>,
            _gid: Option<u32>,
            size: Option<u64>,
            _atime: Option<fuser::TimeOrNow>,
            _mtime: Option<fuser::TimeOrNow>,
            _ctime: Option<SystemTime>,
            _fh: Option<u64>,
            _crtime: Option<SystemTime>,
            _chgtime: Option<SystemTime>,
            _bkuptime: Option<SystemTime>,
            flags: Option<u32>,
            reply: fuser::ReplyAttr,
        ) {
        debug!("Filesystem::setattr({ino}, {size:?}, {flags:?}");
        let runtime = self.runtime.clone();
        let result = runtime.block_on(self.set_attr(ino, size, flags));
        match result {
            Ok(info) => {
                let (attr, ttl) = info_to_file_attr(&info, req);
                reply.attr(&ttl, &attr);
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
        let runtime = self.runtime.clone();
        let result = runtime.block_on(self.read(ino, offset, size.into()));
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
        debug!("Filesystem::readdir({ino}, {offset})");
        let runtime = self.runtime.clone();
        let result = runtime.block_on(self.readdir(ino, offset));
        match result {
            Ok(vector) => {
                debug!("Filesystem::readdir - {}", vector.len());
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
        debug!("Filesystem::write({ino}, {offset})");
        let runtime = self.runtime.clone();
        let result = runtime.block_on(self.write(ino, offset, data));
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
        debug!("Filesystem::mknod({parent}, {name:?})");
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
        debug!("Filesystem::mkdir({parent}, {name:?})");
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
        debug!("Filesystem::unlink({parent}, {name:?})");
        let runtime = self.runtime.clone();
        let result = runtime.block_on(self.unlink(parent, name));
        match result {
            Ok(_) => reply.ok(),
            Err(_) => reply.error(ENOENT)
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        debug!("Filesystem::rmdir({parent}, {name:?})");
        let runtime = self.runtime.clone();
        let result = runtime.block_on(self.rmdir(parent, name));
        match result {
            Ok(_) =>  reply.ok(),
            Err(_) => reply.error(ENOENT)
        }
    }
}

impl FileContentFuse {
    pub fn new(runtime: Arc<Runtime>, content: Arc<Mutex<dyn FileContent>>) -> Self {
        Self { content, runtime }
    }

    async fn info(&mut self, node: u64) -> Result<LayerContentInformation> {
        let mut content = self.content.lock().await;
        content.info(node).await
    }

    async fn lookup(&mut self, parent: u64, name: &OsStr) -> Result<LayerContentInformation> {
        let mut content = self.content.lock().await;
        let node = require(content.lookup(parent, name).await?, "Unkonwn name")?;
        content.info(node).await
    }

    async fn set_attr(&mut self, node: u64, size: Option<u64>, flags: Option<u32>) -> Result<LayerContentInformation> {
        let mut content = self.content.lock().await;
        if let Some(size) = size {
            content.set_size(node, size.try_into()?).await?;
        }
        if let Some(flags) = flags {
            let executable = flags & 0o400 != 0;
            content.set_attributes(node, executable).await?;
        }
        content.info(node).await
    }

    async fn create_file(&mut self, parent: u64, name: &OsStr) -> Result<LayerContentInformation> {
        let mut content = self.content.lock().await;
        let node = content.create_file(parent, name).await?;
        let info = content.info(node).await?;
        Ok(info)
    }

    async fn create_directory(&mut self, parent: u64, name: &OsStr) -> Result<LayerContentInformation> {
        let mut content = self.content.lock().await;
        let node = content.create_directory(parent, name).await?;
        let info = content.info(node).await?;
        Ok(info)
    }

    async fn read(&mut self, node: u64, offset: i64, size: u32) -> Result<Bytes> {
        let mut content = self.content.lock().await;
        content.read_file(node, offset, size.into()).await
    }

    async fn readdir(&mut self, node: u64, offset: i64) -> Result<Vec<LayerDirectoryEntry>> {
        let mut content = self.content.lock().await;
        content.read_directory(node, offset, 65535).await
    }

    async fn write(&mut self, node: u64, offset: i64, data: &[u8]) -> Result<i64> {
        let mut content = self.content.lock().await;
        content.write_file(node, offset, data).await
    }

    async fn unlink(&mut self, parent: u64, name: &OsStr) -> Result<()> {
        let mut content = self.content.lock().await;
        content.remove_file(parent, name).await
    }

    async fn rmdir(&mut self, parent: u64, name: &OsStr) -> Result<()> {
        let mut content = self.content.lock().await;
        content.remove_directory(parent, name).await
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