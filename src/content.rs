use std::{fmt::{self, Display}, cmp::Ordering};

use crate::result::Result;
use base_x::{decode, encode};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Digest, Keypair, PublicKey, SecretKey};
use serde::{Deserialize, Serialize};
use sha2::Sha512;
pub type Date = DateTime<Utc>;

#[derive(Clone, Copy, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub enum Slot {
    Ed25519PublicKey([u8; 32]),
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub enum SlotOwner {
    Ed25519PrivateKey([u8; 32]),
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub enum Signature {
    Ed25519([u8; 32], [u8; 32]),
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub enum Id {
    Sha256([u8; 32]),
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
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

#[derive(Debug, Deserialize, Serialize)]
pub struct Directory {
    pub entries: Vec<DirectoryEntry>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SlotEntry {
    pub description: Description,
    pub previous: Option<Description>,
    pub signature: Option<Signature>,
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

impl std::fmt::Debug for SlotOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ed25519PrivateKey(key) =>  f.write_fmt(format_args!("PrivateKey({})", encode_base58(key)))?
        };
        Ok(())
    }
}

impl SlotOwner {
    pub fn ed25519(bytes: &[u8; 32]) -> Self {
        SlotOwner::Ed25519PrivateKey(*bytes)
    }

    #[allow(dead_code)]
    pub fn owner_string(&self) -> String {
        encode_base58(match self {
            SlotOwner::Ed25519PrivateKey(bytes) => bytes
        })
    }
}

impl Signature {
    pub fn ed25519(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 64 {
            Err("Invalid size for signature".into())
        } else {
            let mut first: [u8; 32] =  [0u8; 32];
            first.copy_from_slice(&bytes[0..32]);
            let mut second: [u8; 32] = [0u8; 32];
            second.copy_from_slice(&bytes[32..]);
            Ok(Signature::Ed25519(first, second))
        }
    }

    #[allow(dead_code)]
    pub fn to_ed25519_signature(&self) -> Result<ed25519_dalek::Signature> {
        match self {
            Self::Ed25519(first, second) => {
                let mut bytes: [u8; 64] = [0u8; 64];
                bytes[0..32].copy_from_slice(first);
                bytes[32..].copy_from_slice(second);
                Ok(ed25519_dalek::Signature::from_bytes(&bytes)?)
            },
        }
    }
}

impl std::fmt::Debug for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ed25519(first, second) => f.write_fmt(format_args!("Ed25519({}, {})", encode_base58(first), encode_base58(second)))
        }
    }
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

impl Id {
    pub fn from_sha256(bytes: &[u8]) -> Id {
        assert!(bytes.len() == 32);
        let mut b: [u8; 32] = [0; 32];
        for (dst, src) in b.iter_mut().zip(bytes) {
            *dst = *src;
        }
        Id::Sha256(b)
    }

    #[allow(dead_code)]
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

impl SlotEntry {
    #[allow(dead_code)]
    pub fn new(description: Description, previous: Option<Description>, signature: Option<Signature>) -> Self {
        Self { description, previous, signature }
    }

    pub fn signed(description: Description, previous: Option<Description>, slot: Slot, owner: SlotOwner) -> Result<Self> {
        let Slot::Ed25519PublicKey(public) = slot;
        let SlotOwner::Ed25519PrivateKey(private) = owner;
        let key_pair = Keypair {
            public: PublicKey::from_bytes(&public)?,
            secret: SecretKey::from_bytes(&private)?,
        };
        let digest = Self::digest(&description, &previous)?;
        let signature_bytes = key_pair.sign_prehashed(digest, None).unwrap().to_bytes();
        let signature = Signature::ed25519(&signature_bytes)?;
        return Ok(Self { description, previous, signature: Some(signature) })
    }

    #[allow(dead_code)]
    pub fn validate(&self, slot: Slot) -> Result<()> {
        let Slot::Ed25519PublicKey(public) = slot;
        if let Some(signature) = self.signature {
            let validator = PublicKey::from_bytes(&public)?;
            let digest = Self::digest(&self.description, &self.previous)?;
            let ed25519_signature = signature.to_ed25519_signature()?;
            validator.verify_prehashed(digest, None, &ed25519_signature)?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn is_valid(&self, slot: Slot) -> bool {
        match self.validate(slot) {
            Ok(_) => true,
            _ => false
        }
    }

    fn digest(description: &Description, previous: &Option<Description>) -> Result<Sha512> {
        let mut digest = Sha512::new();
        let description_bytes = rmp_serde::encode::to_vec(description)?;
        let previous_bytes = rmp_serde::encode::to_vec(previous)?;
        digest.update(description_bytes.as_slice());
        digest.update(previous_bytes.as_slice());
        Ok(digest)
    }
}

const BASE58: &str = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

pub fn encode_base58(bytes: &[u8]) -> String {
    encode(BASE58, bytes)
}

fn decode_base58(input: &str) -> Result<Vec<u8>> {
    Ok(decode(BASE58, input)?)
}
