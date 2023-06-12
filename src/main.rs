use crate::content::Id;
use crate::result::Result;

mod content;
mod content_provider;
mod result;

fn main() -> Result<()> {
    let arg: Vec<_> = std::env::args().collect();
    let param = &arg[1];
    let id = Id::from_string(param)?;
    println!("Id: {}", id);
    Ok(())
}
