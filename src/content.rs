use std::{fmt::{self, Display}, cmp::Ordering};

use crate::result::Result;
use base_x::{decode, encode};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
pub type Date = DateTime<Utc>;

#[derive(Clone, Copy, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub enum Slot {
    Ed25519PublicKey([u8; 32]),
}

impl std::fmt::Debug for Slot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ed25519PublicKey(key) =>  f.write_fmt(format_args!("PublicKey({})", encode_base58(key)))?
        };
        Ok(())
    }
}

impl Slot {
    pub fn ed25519(bytes: &[u8; 32]) -> Slot {
        Slot::Ed25519PublicKey(*bytes)
    }
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub enum SlotOwner {
    Ed25519PrivateKey([u8; 32]),
}

impl std::fmt::Debug for SlotOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ed25519PrivateKey(key) =>  f.write_fmt(format_args!("PrivateKey({})", encode_base58(key)))?
        };
        Ok(())
    }
}

impl SlotOwner {
    pub fn ed25519(bytes: &[u8; 32]) -> SlotOwner {
        SlotOwner::Ed25519PrivateKey(*bytes)
    }
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub enum Id {
    Sha256([u8; 32]),
}

impl Id {
    #[cfg(test)]
    pub fn sha256(value: &str) -> Result<Self> {
        let val = decode_base58(value)?;
        let mut bytes: [u8; 32] = [0; 32];
        for i in 0..32 {
            bytes[i] = val[i];
        }
        Ok(Self::Sha256(bytes))
    }
}

impl std::fmt::Debug for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sha256(key) =>  f.write_fmt(format_args!("Sha({})", encode_base58(key)))?
        };
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct Description {
    pub id: Id,
    pub size: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Content {
    Described(Description),
    Node(Slot),
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
    pub fn info(&self) -> &EntryInformation {
        match self {
            DirectoryEntry::File(info) => info,
            DirectoryEntry::Directory(info) => info,
        }
    }

    pub fn compare(&self, other: &DirectoryEntry) -> Ordering {
        let a_info = self.info();
        let b_info = other.info();
        if a_info.name < b_info.name { Ordering::Less }
        else if a_info.name == b_info.name { Ordering::Equal }
        else { Ordering::Greater }
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
        })
    }
}

impl Slot {
    pub fn slot_string(&self) -> String {
        encode_base58(match self {
            Slot::Ed25519PublicKey(bytes) => bytes
        })
    }

    pub fn from_string(str: &str) -> Result<Self> {
        let bytes = decode_base58(str)?;
        Ok(Self::from_slice(&bytes[..]))
    }

    pub fn from_slice(bytes: &[u8]) -> Self {
        assert!(bytes.len() == 32);
        let mut b: [u8; 32] = [0; 32];
        for (dst, src) in b.iter_mut().zip(bytes) {
            *dst = *src;
        }
        Self::Ed25519PublicKey(b)
    }
}

impl Display for Slot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ed25519PublicKey(bytes) => {
                f.write_fmt(format_args!("Ed25519({})", encode_base58(bytes)))
            }
        }
    }
}

impl Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sha256(bytes) => f.write_fmt(format_args!("Sha256({})", encode_base58(bytes))),
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

pub fn encode_base58(bytes: &[u8]) -> String {
    encode(BASE58, bytes)
}

fn decode_base58(input: &str) -> Result<Vec<u8>> {
    Ok(decode(BASE58, input)?)
}
