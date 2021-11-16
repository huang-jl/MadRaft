use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{kvraft::server::RecentInfo, shard_ctrler::msg::ConfigId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Op {
    Get {
        key: String,
    },
    Put {
        key: String,
        value: String,
    },
    Append {
        key: String,
        value: String,
    },
    Take {
        sid: usize,
    },
    Receive {
        sid: usize,
        kv: HashMap<String, String>,
        client: HashMap<String, RecentInfo<Reply>>,
    },
    UpdateConfigId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Reply {
    Get {
        value: Option<String>,
    },
    Ok,
    WrongGroup,
    Shard {
        sid: usize,
        kv: HashMap<String, String>,
        client: HashMap<String, RecentInfo<Reply>>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardKvOp {
    pub op: Op,
    pub config_id: ConfigId,
}

impl ShardKvOp {
    pub fn new(op: Op, config_id: ConfigId) -> Self {
        Self { op, config_id }
    }
}
