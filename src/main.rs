use std::ffi::OsString;
use std::fs::{File, create_dir, create_dir_all};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use content::Id;
use content_dump::dump_directory;
use content_file_layer::{Notifier, Node};
use content_provider::{ContentStore, SlotHolder, SlotOwnerStore, ContentProvider};
use ed25519_dalek::Keypair;
use object_store::local::LocalFileSystem;
use rand::rngs::OsRng;
use serde::{Serialize, Deserialize};
use tokio::sync::Mutex;
use crate::content::{Slot, encode_base58};
use crate::content_actor::ContentActorHandle;
use crate::content_file_layer::FileContent;
use crate::content_fuse::FileContentFuse;
use crate::content_loader::load_directory;
use crate::content_tasks::ContentTaskHandle;
use crate::result::Result;
use env_logger::Env;
use clap::{Parser, Subcommand};
use home::home_dir;

mod content;
mod content_actor;
mod content_dump;
mod content_file_layer;
mod content_fuse;
mod content_loader;
mod content_provider;
mod content_tasks;
mod memory_provider;
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
    #[command(about = "Initializer and configure eather")]
    Init {
        /// Root slot location
        #[clap(long)]
        default_slot: Option<String>,

        /// The blocks provider, defaults to `local:`
        #[clap(long)]
        provider: Option<OsString>,

        /// The location to store new blocks, defaults to `local:`
        #[clap(long)]
        store: Option<OsString>,

        /// Slot holder configuration, defaults to `local:`
        #[clap(long)]
        slots: Option<OsString>,

        /// Slot owners configuration, defaults to `local:`
        #[clap(long)]
        owners: Option<OsString>,
    },

    #[command(about = "Map a distributed directory to a physical directory")]
    Fuse {
        /// The slot ID (base58) of the directory to map
        #[clap(long)]
        slot: Option<String>,

        /// The location to map the directory
        path: OsString,
    },

    #[command(about = "Load the content of the directory into the cache")]
    Load {
        /// The path to load
        path: OsString,
    },

    #[command(about = "Generate a slot public/private key pair")]
    Slot { },

    #[command(about = "Dump the content of an Id")]
    Dump {
        id: String,

        #[clap(long)]
        directory: bool,
    }
}

#[derive(Deserialize, Serialize, Clone)]
enum ServiceConfiguration {
    LocalFileSystem { path: Option<OsString> }
}

impl ServiceConfiguration {
    fn parse(config: OsString) -> Result<ServiceConfiguration> {
        if let Some(result) = parse_local(config) {
            return Ok(result)
        }
        return Err("Unrecognized configuration".into())
    }

    fn parse_if(config: Option<OsString>) -> Result<ServiceConfiguration> {
        match config {
            Some(config) => Ok(Self::parse(config)?),
            _ => Ok(ServiceConfiguration::LocalFileSystem { path: None })
        }
    }
}

fn parse_local(config: OsString) -> Option<ServiceConfiguration> {
    let str = config.to_string_lossy();
    if str.starts_with("local:") {
        let (_, location) = str.split_at(6);
        let config =  ServiceConfiguration::LocalFileSystem {
            path: if location.len() > 0 { Some(OsString::from(location)) } else { None }
        };
        Some(config)
    } else {
        None
    }
}

#[derive(Deserialize, Serialize)]
struct Configuration {
    default_slot: Option<Slot>,
    provider: ServiceConfiguration,
    store: ServiceConfiguration,
    slots: ServiceConfiguration,
    owners: ServiceConfiguration,
}

fn local_provider(config_path: &Path) -> Result<impl ContentProvider + Send + Send> {
    let cache_path = config_path.join("cache");
    let file_system = LocalFileSystem::new_with_prefix(cache_path)?;
    Ok(object_store_provider::ObjectStoreProvider::new(file_system))
}

fn local_store(config_path: &Path) -> Result<impl ContentStore + Send + Sync> {
    let cache_path = config_path.join("cache");
    let file_system = LocalFileSystem::new_with_prefix(cache_path)?;
    Ok(object_store_provider::ObjectStoreProvider::new(file_system))
}

fn local_slots(config_path: &Path) -> Result<impl SlotHolder + Send + Sync> {
    let slots_path = config_path.join("slots");
    let file_system = LocalFileSystem::new_with_prefix(slots_path)?;
    Ok(object_store_provider::ObjectStoreProvider::new(file_system))
}

fn local_owners(config_path: &Path) -> Result<impl SlotOwnerStore + Send + Sync> {
    let owners_path = config_path.join("owners");
    let file_system = LocalFileSystem::new_with_prefix(owners_path)?;
    Ok(object_store_provider::ObjectStoreProvider::new(file_system))
}

fn path_of(config_path: &Path, path: &Option<OsString>) -> Result<PathBuf> {
    if let Some(path) = path {
        return Ok(Path::new(path).to_path_buf());
    }
    Ok(config_path.to_owned())
}

fn provider_from_configuration(
    config_path: &Path,
    cache_config: &ServiceConfiguration
) -> Result<impl ContentProvider + Send + Sync> {
    match cache_config {
        ServiceConfiguration::LocalFileSystem { path } => {
            let path = path_of(config_path, path)?;
            local_provider(&path)
        }
    }
}

fn store_from_configuration(
    config_path: &Path,
    cache_config: &ServiceConfiguration
) -> Result<impl ContentStore + Send + Sync> {
    match cache_config {
        ServiceConfiguration::LocalFileSystem { path } => {
            let path = path_of(config_path, path)?;
            local_store(&path)
        }
    }
}

fn slots_from_configuration(
    config_path: &Path,
    cache_config: &ServiceConfiguration
) -> Result<impl SlotHolder + Send + Sync> {
    match cache_config {
        ServiceConfiguration::LocalFileSystem { path } => {
            let path = path_of(config_path, path)?;
            local_slots(&path)
        }
    }
}

fn owners_from_configuration(
    config_path: &Path,
    cache_config: &ServiceConfiguration
) -> Result<impl SlotOwnerStore + Send + Sync> {
    match cache_config {
        ServiceConfiguration::LocalFileSystem { path } => {
            let path = path_of(config_path, path)?;
            local_owners(&path)
        }
    }
}

// Commands

// Initialize
fn initialize(
    config_path: &Path,
    default_slot: Option<String>,
    provider: Option<OsString>,
    store: Option<OsString>,
    slots: Option<OsString>,
    owners: Option<OsString>,
) -> Result<()> {
    let default_slot = default_slot.map(|slot| Slot::from_string(&slot)).transpose()?;
    let provider = ServiceConfiguration::parse_if(provider)?;
    let store = ServiceConfiguration::parse_if(store)?;
    let slots = ServiceConfiguration::parse_if(slots)?;
    let owners = ServiceConfiguration::parse_if(owners)?;
    let configuration = Configuration {
        default_slot,
        provider: provider.clone(),
        store: store.clone(),
        slots: slots.clone(),
        owners: owners.clone(),
    };

    let bytes = rmp_serde::to_vec(&configuration)?;

    create_dir_all(config_path)?;
    let mut file = File::create(config_path.join("configuration.rmp"))?;
    file.write_all(bytes.as_slice())?;
    ensure_diretory_for(config_path, provider, "cache")?;
    ensure_diretory_for(config_path, store, "cache")?;
    ensure_diretory_for(config_path, slots, "slots")?;
    ensure_diretory_for(config_path, owners, "owners")?;
    Ok(())
}

fn ensure_diretory_for(config_path: &Path, service_configuration: ServiceConfiguration, name: &str) -> Result<()> {
    match service_configuration {
        ServiceConfiguration::LocalFileSystem { path } => {
            if path.is_none() {
                let path = config_path.join(name);
                if !path.is_dir() {
                    create_dir(config_path.join(name))?;
                }
            }
        }
    }
    Ok(())
}

fn load_configuration(config_path: &Path) -> Result<Configuration> {
    let file = File::open(config_path.join("configuration.rmp"))?;
    Ok(rmp_serde::from_read(file)?)
}

// Fuse
fn start_fuse(config_path: &Path, configuration: &Configuration, slot: Slot, path: &Path) -> Result<()> {
    let provider = provider_from_configuration(config_path, &configuration.provider)?;
    let store = store_from_configuration(config_path, &configuration.store)?;
    let slots = slots_from_configuration(config_path, &configuration.owners)?;
    let owners = owners_from_configuration(config_path, &configuration.owners)?;

    let runtime = Arc::new(tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
    );
    let actor = ContentActorHandle::new(provider, store, slots, runtime.clone());
    let content_layer = Arc::new(Mutex::new(crate::content_file_layer::new(
        actor.clone(),
        slot,
        Box::new(owners),
    )));
    let tasks = ContentTaskHandle::new(runtime.clone(), content_layer.clone());
    let invalidate_handler = Box::new(TaskNotifier::new(tasks));
    runtime.block_on(watch_sync(content_layer.clone(), invalidate_handler));
    let content_fuse = FileContentFuse::new(runtime, content_layer);
    log::info!("Mount {slot:?} to {path:?}");
    fuser::mount2(content_fuse, path, &[])?;
    Ok(())
}

// Load
fn load(config_path: &Path, configuration: &Configuration, path: &Path) -> Result<Slot> {
    let store = store_from_configuration(config_path, &configuration.store)?;
    let slots = slots_from_configuration(config_path, &configuration.owners)?;
    let owners = owners_from_configuration(config_path, &configuration.owners)?;
    load_directory(&store, &slots, &owners, path)
}

// Dump directory
fn dump_directory_command(config_path: &Path, configuration: &Configuration, id: &str) -> Result<()> {
    let provider = provider_from_configuration(config_path, &configuration.provider)?;
    let id = Id::from_string(id)?;
    dump_directory(&provider, id)
}

struct TaskNotifier {
    task_handler: ContentTaskHandle,
}

#[async_trait]
impl Notifier for TaskNotifier {
    async fn notify_invalid(&mut self, node: Node) -> Result<()> {
        self.task_handler.sync(node).await
    }
}

impl TaskNotifier {
    fn new(task_handler: ContentTaskHandle) -> Self {
        Self { task_handler }
    }
}

async fn watch_sync(content_layer: Arc<Mutex<dyn FileContent>>, notifier: Box<TaskNotifier>) -> () {
    content_layer.lock().await.watch(notifier);
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
    let config_dir = if let Some(config_dir) = config.config.or_else(|| { default_config() }) {
        config_dir
    } else {
        return Err("Unable to find configuration".into())
    };
    let config_path = Path::new(&config_dir);
    match config.command {
        EatherCommand::Init { default_slot, provider, store, slots, owners } => {
            initialize(config_path, default_slot, provider, store, slots, owners)?;
            println!("Eather configured correctly");
            Ok(())
        },
        EatherCommand::Fuse { slot, path } => {
            let configuration = load_configuration(config_path)?;
            let slot = slot.map(|slot| Slot::from_string(&slot)).transpose()?.or(configuration.default_slot);
            let slot = if let Some(slot) = slot { slot } else {
                return Err("A slot must be provided, or a default configured, to start a fuse".into());
            };
            let path = Path::new(&path);
            start_fuse(config_path, &configuration, slot, path)?;
            Ok(())
        },
        EatherCommand::Load { path } => {
            let configuration = load_configuration(config_path)?;
            let path = Path::new(&path);
            let slot = load(config_path, &configuration, &path)?;
            println!("Slot: {slot:?}");
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
        },
        EatherCommand::Dump { id, directory } => {
            if directory {
                let configuration = load_configuration(config_path)?;
                dump_directory_command(config_path, &configuration, &id)?
            } else {
                println!("Only directory are supported for now")
            }
            Ok(())
        }
    }
}
