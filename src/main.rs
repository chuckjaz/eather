use std::ffi::OsString;
use std::path::Path;

use content_fuse::ContentFuse;
use ed25519_dalek::Keypair;
use object_store::local::LocalFileSystem;
use rand::rngs::OsRng;
use crate::content::{Slot, encode_base58};
use crate::content_loader::load_directory;
use crate::result::Result;
use env_logger::Env;
use clap::{Parser, Subcommand};
use home::home_dir;

mod content;
mod content_actor;
mod content_fuse;
mod content_loader;
mod content_provider;
mod object_store_provider;
mod result;

#[derive(Debug, Parser)]
#[command(name = "Eather")]
#[command(author = "Chuck Jazdzewski (chuckjaz@gmail.com)")]
#[command(version = "0.1.0")]
#[command(about = "Distributed file system")]
pub struct Eather {
    /// Location of the configuration 
    #[arg(long)]
    config: Option<OsString>,

    #[clap(subcommand)]
    command: EatherCommand
}

#[derive(Debug, Subcommand)]
enum EatherCommand {
    #[command(about = "Map a distributed directory to a physcial directory")]
    Fuse {
        /// The slot ID (base58) of the directory to map
        #[arg()]
        slot: String,

        /// The location to map the directory
        path: OsString,
    },
    #[command(about = "Load the content of the directory into the cache")]
    Load {
        /// The path to load
        path: OsString, 
    },
    #[command(about = "Generate a slot public/private key pair")]
    Slot {

    }
}

fn start_fuse(slot: String, config: OsString, path: OsString) -> Result<()> {
    let slot = Slot::from_string(&slot)?;
    let cache_path = Path::new(&config).join("cache");
    let slot_path = Path::new(&config).join("slots");
    let file_system = LocalFileSystem::new_with_prefix(cache_path.clone())?;
    let provider = object_store_provider::ObjectStoreProvider::new(file_system);
    let file_system = LocalFileSystem::new_with_prefix(cache_path)?;
    let store = object_store_provider::ObjectStoreProvider::new(file_system);
    let file_system = LocalFileSystem::new_with_prefix(slot_path)?;
    let slots = object_store_provider::ObjectStoreProvider::new(file_system);
    let content_fuse = ContentFuse::new(provider, store, slots, slot)?;
    log::info!("Mount {slot:?} to {path:?}");
    fuser::mount2(content_fuse, path, &[])?;
    Ok(())    
}


fn default_config() -> Option<OsString> {
    if let Some(mut dir) = home_dir() {
        dir.push(".eather");
        let path: OsString= dir.as_os_str().into();
        Some(path)
    } else {
        None
    }
}

fn main() -> Result<()> {
    let env = Env::new().filter("EATHER_LOG");
    env_logger::try_init_from_env(env)?;

    let config = Eather::parse();
    println!("config: {config:?}");    
    let config_dir = if let Some(config_dir) = config.config.or_else(|| { default_config() }) {
        config_dir
    } else {
        return Err("Unable to find configuration".into())
    };
    match config.command {
        EatherCommand::Fuse { slot, path } => {
            start_fuse(slot, config_dir, path)?;
            Ok(())
        },
        EatherCommand::Load { path } => {
            let (slot, owner) = load_directory(config_dir, path)?;
            println!("public: {slot:?}, private: {owner:?}");
            Ok(())
        },
        EatherCommand::Slot {  } => {
            let mut csprng = OsRng{};
            let pair = Keypair::generate(&mut csprng);
            let public = pair.public.as_bytes();
            let secret = pair.secret.as_bytes();
            let public_str = encode_base58(public);
            let secret_str: String = encode_base58(secret);
            println!("public: {public_str}, secret: {secret_str}");
            Ok(())
        }
    }
}
