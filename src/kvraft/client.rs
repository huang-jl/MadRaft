use super::msg::*;
use madsim::{net, time::*};
use std::{
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
};

pub struct Clerk {
    core: ClerkCore<Op, String>,
}

impl Clerk {
    pub fn new(servers: Vec<SocketAddr>) -> Clerk {
        Clerk {
            core: ClerkCore::new(servers),
        }
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    pub async fn get(&self, key: String) -> String {
        self.core.call(Op::Get { key }).await
    }

    pub async fn put(&self, key: String, value: String) {
        self.core.call(Op::Put { key, value }).await;
    }

    pub async fn append(&self, key: String, value: String) {
        self.core.call(Op::Append { key, value }).await;
    }
}

pub struct ClerkCore<Req, Rsp> {
    servers: Vec<SocketAddr>,
    rid: AtomicU64, // request id, monotonically increasing
    pub cid: String,
    _mark: std::marker::PhantomData<(Req, Rsp)>,
}

const LOOP_LIMIT: i32 = 2;

impl<Req, Rsp> ClerkCore<Req, Rsp>
where
    Req: net::Message + Clone,
    Rsp: net::Message,
{
    pub fn new(servers: Vec<SocketAddr>) -> Self {
        ClerkCore {
            rid: AtomicU64::new(0),
            servers,
            cid: format!("{}", rand::random::<u64>()),
            _mark: std::marker::PhantomData,
        }
    }

    /// Question: What is the diff between kvraft::Error::Timeout and call_timeout's io::Error
    pub async fn call(&self, args: Req) -> Rsp {
        self.increase_rid();
        self.send_to_group(args, &self.servers).await
    }

    pub async fn send_to_group(&self, args: Req, group: &[SocketAddr]) -> Rsp {
        let net = net::NetLocalHandle::current();
        let args = ClerkReq {
            req: args,
            client: self.cid.clone(),
            rid: self.rid.load(Ordering::SeqCst),
        };
        let mut old_s;
        let mut s = 0;
        let mut iter_times = 0;
        loop {
            info!(
                "[Clerk] {} call {:?}, with args = {:?}",
                self.cid.clone(),
                group[s],
                args
            );
            old_s = s;
            let ret = net
                .call_timeout::<ClerkReq<Req>, Result<Rsp, Error>>(
                    group[s],
                    args.clone(),
                    Duration::from_millis(500),
                )
                .await;
            info!(
                "[Clerk] {} call {:?} get response = {:?}",
                self.cid.clone(),
                group[s],
                ret
            );
            if let Ok(ret) = ret {
                match ret {
                    Ok(reply) => return reply,
                    Err(err) => match err {
                        Error::NotLeader { hint } => s = hint,
                        Error::Failed => {}
                        Error::Timeout => s = (s + 1) % group.len(),
                    },
                }
            } else {
                s = (s + 1) % group.len();
            }
            // optimiazation for partition
            if s == old_s {
                iter_times += 1;
            } else {
                iter_times = 0;
            }
            if iter_times > LOOP_LIMIT {
                s = (s + 1) % group.len();
                iter_times = 0;
            }
        }
    }

    pub fn increase_rid(&self) {
        self.rid.fetch_add(1, Ordering::SeqCst);
    }
}
