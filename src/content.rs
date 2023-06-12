use std::fmt::{self, Display};

use crate::result::Result;
use base_x::{decode, encode};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
pub type Date = DateTime<Utc>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum Id {
    Sha256([u8; 32]),
    Ed25519PublicKey([u8; 32]),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Description {
    pub id: Id,
    pub size: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Content {
    Described(Description),
    Node(Id),
}

impl Content {
    pub fn id(&self) -> Id {
        match self {
            Content::Described(description) => description.id,
            Content::Node(id) => *id,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub enum FileType {
    File,
    Directory,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EntryInformation {
    pub name: String,
    pub modify_time: Date,
    pub content: Content,
    pub executable: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum DirectoryEntry {
    File(EntryInformation),
    Directory(EntryInformation),
}

impl DirectoryEntry {
    pub fn info(&self) -> EntryInformation {
        match self {
            DirectoryEntry::File(info) => info.clone(),
            DirectoryEntry::Directory(info) => info.clone(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Directory {
    pub entries: Vec<DirectoryEntry>,
}

impl Id {
    pub fn from_sha256(bytes: &[u8]) -> Id {
        assert!(bytes.len() == 32);
        let mut b: [u8; 32] = [0; 32];
        for (dst, src) in b.iter_mut().zip(bytes) {
            *dst = *src;
        }
        Id::Sha256(b)
    }

    pub fn from_string(str: &str) -> Result<Id> {
        let bytes = decode_base58(str)?;
        Ok(Self::from_sha256(&bytes[..]))
    }

    pub fn id_string(&self) -> String {
        encode_base58(match self {
            Id::Sha256(bytes) => bytes,
            Id::Ed25519PublicKey(bytes) => bytes,
        })
    }
}

impl Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sha256(bytes) => f.write_fmt(format_args!("Sha256({})", encode_base58(bytes))),
            Self::Ed25519PublicKey(bytes) => {
                f.write_fmt(format_args!("Ed25519({})", encode_base58(bytes)))
            }
        }
    }
}

impl Display for Content {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Node(id) => f.write_fmt(format_args!("N:{}", id)),
            Self::Described(Description { id, size }) => {
                f.write_fmt(format_args!("D:{id}({size})"))
            }
        }
    }
}

const BASE58: &str = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

fn encode_base58(bytes: &[u8]) -> String {
    encode(BASE58, bytes)
}

fn decode_base58(input: &str) -> Result<Vec<u8>> {
    Ok(decode(BASE58, input)?)
}
