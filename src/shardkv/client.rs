use madsim::{net, time};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use crate::kvraft::msg::{ClerkReq, Error};
use crate::shard_ctrler::{client::Clerk as CtrlerClerk, msg::Config as CtrlerConfig};
use crate::shardkv::msg::{Op, Reply};

use super::key2shard;
use super::msg::ShardKvOp;

pub struct Clerk {
    ctrl_ck: CtrlerClerk,
    cid: String,
    rid: AtomicU64,
    // TODO client cannot send concurrent request, maybe do not need mutex
    config: Mutex<CtrlerConfig>,
}

impl Clerk {
    pub fn new(servers: Vec<SocketAddr>) -> Clerk {
        Clerk {
            ctrl_ck: CtrlerClerk::new(servers),
            cid: format!("{}", rand::random::<u64>()),
            rid: AtomicU64::new(0),
            config: Mutex::new(Default::default()),
        }
    }

    pub async fn get(&self, key: String) -> String {
        self.check_for_valid_shard_service().await;
        loop {
            match self.send_with_key(&key, Op::Get { key: key.clone() }).await {
                Reply::Get { value } => return value.unwrap_or("".to_string()),
                Reply::WrongGroup => self.update_config().await,
                x => panic!("[KvClient] Op::Get {} replied with {:?}", key, x),
            }
        }
    }

    pub async fn put(&self, key: String, value: String) {
        self.check_for_valid_shard_service().await;
        loop {
            match self
                .send_with_key(
                    &key,
                    Op::Put {
                        key: key.clone(),
                        value: value.clone(),
                    },
                )
                .await
            {
                Reply::Ok => return,
                x => panic!("[KvClient] Op::Get {} replied with {:?}", key, x),
            }
        }
    }

    pub async fn append(&self, key: String, value: String) {
        self.check_for_valid_shard_service().await;
        loop {
            match self
                .send_with_key(
                    &key,
                    Op::Append {
                        key: key.clone(),
                        value: value.clone(),
                    },
                )
                .await
            {
                Reply::Ok => return,
                x => panic!("[KvClient] Op::Get {} replied with {:?}", key, x),
            }
        }
    }

    /// Since there is no group when startup,
    /// client request has to wait until there is valid config.
    async fn check_for_valid_shard_service(&self) {
        let group_num = self.config.lock().unwrap().groups.len();
        if group_num == 0 {
            self.update_config().await;
        }
    }

    async fn update_config(&self) {
        loop {
            let config = self.ctrl_ck.query().await;
            if config.groups.len() > 0 {
                *self.config.lock().unwrap() = config;
                break;
            }
            time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn get_group_of_sid(&self, sid: usize) -> Vec<SocketAddr> {
        let config = self.config.lock().unwrap();
        let gid = config.shards[sid];
        config.groups.get(&gid).unwrap().clone()
    }

    async fn send_with_key(&self, key: &str, op: Op) -> Reply {
        let sid = key2shard(key);
        self.rid.fetch_add(1, Ordering::SeqCst);
        let net = net::NetLocalHandle::current();
        let mut args = ClerkReq {
            client: self.cid.clone(),
            rid: self.rid.load(Ordering::SeqCst),
            req: ShardKvOp {
                op,
                config_id: 0,
            },
        };
        loop {
            let (mut old_s, mut s, mut timeout_times, mut cycle) = (0, 0, 0, 0);
            let groups = self.get_group_of_sid(sid);
            loop {
                old_s = s;
                args.rid = self.rid.load(Ordering::SeqCst);
                args.req.config_id = self.config.lock().unwrap().num;
                info!(
                    "[ShardKvClerk] {} call {:?}, with args = {:?}",
                    self.cid.clone(),
                    groups[s],
                    args
                );
                let ret = net
                    .call_timeout::<ClerkReq<ShardKvOp>, Result<Reply, Error>>(
                        groups[s],
                        args.clone(),
                        Duration::from_millis(500),
                    )
                    .await;
                info!(
                    "[ShardKvClerk] {} call {} args = {:?} get ret = {:?}",
                    self.cid.clone(),
                    groups[s],
                    args,
                    ret
                );
                if matches!(ret, Ok(Err(Error::Timeout)) | Err(_)) {
                    timeout_times += 1;
                } else {
                    timeout_times = 0;
                }
                if let Ok(ret) = ret {
                    match ret {
                        Ok(reply) => match reply {
                            Reply::WrongGroup => {
                                // self.rid.fetch_add(1, Ordering::SeqCst);
                                break;
                            }
                            x => return x,
                        },
                        Err(err) => match err {
                            Error::NotLeader { hint } => s = hint,
                            _ => s = (s + 1) % groups.len(),
                        },
                    }
                } else {
                    s = (s + 1) % groups.len();
                }
                if s == old_s {
                    cycle += 1;
                } else {
                    cycle = 0;
                }
                if cycle > 2 {
                    s = (s + 1) % groups.len();
                }
                if timeout_times >= groups.len() {
                    break;
                }
            }
            self.update_config().await;
        }
    }
}
