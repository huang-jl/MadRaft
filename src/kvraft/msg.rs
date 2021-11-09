use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Op {
    Get { key: String },
    Put { key: String, value: String },
    Append { key: String, value: String },
}


#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Error {
    #[error("not leader, hint: {hint}")]
    NotLeader { hint: usize },
    #[error("timeout")]
    Timeout,
    #[error("failed to reach consensus")]
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClerkReq<T> {
    pub req: T,
    pub client: String, //client unique identifier
    pub rid: u64, //request id
}