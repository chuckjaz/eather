use crate::content::Id;
use crate::result::Result;

mod content;
mod content_actor;
mod content_fuse;
mod content_provider;
mod object_store_provider;
mod result;

fn main() -> Result<()> {
    let arg: Vec<_> = std::env::args().collect();
    if arg.len() == 2 {
        let param = &arg[1];
        let id = Id::from_string(param)?;
        println!("Id: {}", id);
    } else {
        println!("Expected a root slot ID")
    }
    Ok(())
}
