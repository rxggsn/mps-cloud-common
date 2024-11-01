pub mod scheduler;

pub trait Task: Ord + Eq + Send + Sync {}