use crate::{content_provider::ContentProvider, content::{Id, Directory, DirectoryEntry, Content}, result::Result};
use bytes::{BytesMut, BufMut};
use futures::StreamExt;

pub fn dump_directory(
    provider: &(impl ContentProvider + Sync + Send),
    id: Id,
) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(dump_dir(provider, id))
}

async fn dump_dir(
    provider: &(impl ContentProvider + Sync + Send),
    id: Id,
) -> Result<()> {
    let mut result = BytesMut::new();
    let mut stream = provider.get(id).await?;
    while let Some(bytes) = stream.next().await {
        let bytes = bytes?;
        result.put(&bytes[..]);
    }
    let directory: Directory = rmp_serde::from_slice(&result)?;

    for entry in &directory.entries {
        let (info, directory) = match entry {
            DirectoryEntry::Directory(info) => (info, true),
            DirectoryEntry::File(info) => (info, false)
        };

        print!("{}{} {:30}", if directory { "D" } else { "F" }, if info.executable { "E" } else { " " }, info.name);
        match info.content {
            Content::Described(content) => {
                print!("{:>10}     {}", content.size, content.id);
            },
            Content::Node(slot) => {
                print!("      slot     {}", slot)
            }
        }
        println!();
    }
//    println!("{directory:?}");
    Ok(())
}

