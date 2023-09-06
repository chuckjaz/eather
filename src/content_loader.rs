use std::{path::Path, fs::{self, File, DirEntry}, io::{BufReader, Read}, os::unix::prelude::PermissionsExt};
use async_recursion::async_recursion;
use bytes::{Bytes, Buf};
use ed25519_dalek::Keypair;
use rand::rngs::OsRng;
use ring::digest::{Context, SHA256, Digest};
use log::info;
use tokio::io::AsyncWriteExt;

use crate::{result::Result, content::{DirectoryEntry, Directory, EntryInformation, Content, Description, Id, Slot, SlotOwner, SlotEntry}, content_provider::{ContentStore, SlotHolder, SlotOwnerStore}};

async fn directory_of(path: &Path, store: &(impl ContentStore + Sync + Send)) -> Result<Content> {
    let mut entries: Vec<DirectoryEntry> = Vec::new();
    for dir_entry in fs::read_dir(path)? {
        let dir_entry = dir_entry?;
        entries.push(directory_entry_of_entry(&dir_entry, store).await?);
    }
    let directory = Directory { entries };
    let directory_vec = rmp_serde::to_vec(&directory)?;
    let bytes_instance = Bytes::copy_from_slice(&directory_vec);
    let bytes = bytes_instance.chunk();
    let sha = sha256_of_buffer(bytes);
    let id = Id::from_sha256(sha.as_ref());
    let content = Content::Described(
        Description { id, size: bytes.len() as i64 }
    );
    let mut writer = store.put(id).await?;
    writer.write(bytes).await?;
    writer.shutdown().await?;
    Ok(content)
}

#[async_recursion]
async fn directory_entry_of_entry(entry: &DirEntry, store: &(impl ContentStore + Sync + Send)) -> Result<DirectoryEntry> {
    info!("Loading entry {entry:?}");
    let name = entry.file_name().to_string_lossy().to_string();
    let modify_time = entry.metadata()?.modified()?;
    if entry.file_type()?.is_dir() {
        let p = entry.path();
        let mut entries: Vec<DirectoryEntry> = Vec::new();
        for dir_entry in fs::read_dir(p)? {
            let dir_entry = dir_entry?;
            entries.push(directory_entry_of_entry(&dir_entry, store).await?);
        }
        let directory = Directory { entries };
        let directory_vec = rmp_serde::to_vec(&directory)?;
        let bytes_instance = Bytes::copy_from_slice(&directory_vec);
        let bytes = bytes_instance.chunk();
        let sha = sha256_of_buffer(bytes);
        let id = Id::from_sha256(sha.as_ref());
        let content = Content::Described(
            Description { id, size: bytes.len() as i64 }
        );
        let mut writer = store.put(id).await?;
        writer.write(bytes).await?;
        writer.shutdown().await?;
        Ok(
            DirectoryEntry::Directory(
                EntryInformation {
                    name,
                    modify_time: modify_time.into(),
                    content,
                    executable: false,
                }
            )
        )
    } else {
        let file_path_buf = entry.path();
        let file_path = &file_path_buf.as_path();
        println!("file: {file_path:?}");
        let (id, size) = sha256_of_file(file_path)?;
        let content = Content::Described(Description { id, size });
        let mut writer = store.put(id).await?;
        let mut file = File::open(file_path)?;
        let executable = file.metadata()?.permissions().mode() & 0o111 != 0;
        let mut buffer: [u8; 4096] = [0; 4096];
        let mut size = file.read(&mut buffer[..])?;
        while size > 0 {
            writer.write(&buffer[..size]).await?;
            size = file.read(&mut buffer[..])?;
        }
        writer.shutdown().await?;
        println!(" id: {id}");
        let info = EntryInformation {
            name,
            modify_time: modify_time.into(),
            content,
            executable,
        };
        Ok(DirectoryEntry::File(info))
    }
}

fn sha256_of_buffer(buf: &[u8]) -> Digest {
    let mut context = Context::new(&SHA256);
    context.update(buf);
    context.finish()
}

fn sha256_of_file(path: &Path) -> Result<(Id, i64)> {
    let input = File::open(path)?;
    let size = input.metadata()?.len().try_into().unwrap();
    let reader = BufReader::new(input);
    let digest = sha256_digest(reader)?;
    let id = Id::from_sha256(digest.as_ref());
    Ok((id, size))
}

fn sha256_digest<R: Read>(mut reader: R) -> Result<Digest> {
    let mut context = Context::new(&SHA256);
    let mut buffer = [0; 1024];

    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        context.update(&buffer[..count]);
    }

    Ok(context.finish())
}

async fn slot_of_path(
    path: &Path,
    store: &(impl ContentStore + Sync + Send),
    slots: &(impl SlotHolder + Sync + Send),
    owners: &(impl SlotOwnerStore + Sync + Send)
) -> Result<Slot>{
    // Store the directory
    let directory_content = directory_of(&path, store).await?;
    let description = if let Content::Described(description) = directory_content {
        description
    } else {
        panic!("Expected a description")
    };

    // Generate the slot
    let (slot, owner) = new_slot_pair();

    // Sign the entry
    let entry = SlotEntry::signed(description, None, slot, owner)?;

    // Create the slot in the holder
    slots.create(slot, entry).await?;

    // Add the owner to the slot owner store
    owners.save_slot_owner(slot, owner).await?;

    Ok(slot)
}

pub(crate) fn new_slot_pair() -> (Slot, SlotOwner) {
    let mut csprng = OsRng{};
    let pair = Keypair::generate(&mut csprng);
    let public = pair.public.as_bytes();
    let secret = pair.secret.as_bytes();
    let slot = Slot::ed25519(public);
    let owner = SlotOwner::ed25519(secret);
    (slot, owner)
}

pub fn load_directory(
    store: &(impl ContentStore + Sync + Send),
    slots: &(impl SlotHolder + Sync + Send),
    owners: &(impl SlotOwnerStore + Sync + Send),
    path: &Path
) -> Result<Slot> {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(slot_of_path(path, store, slots, owners))
}
