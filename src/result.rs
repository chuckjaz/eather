use std::error::Error;

pub type Result<T> = std::result::Result<T, Box<dyn Error + Sync + Send>>;

pub fn require<T>(option: Option<T>, msg: &str) -> Result<T> {
    match option {
        Some(value) => Ok(value),
        None => Err(msg.into())
    }
}