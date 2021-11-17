use super::{key2shard, msg::*};
use crate::kvraft::client::ClerkCore;
use crate::kvraft::msg::ClerkReq;
use crate::kvraft::server::{RecentInfo, Server, State};
use crate::shard_ctrler::client::Clerk as CtrlerClerk;
use crate::shard_ctrler::msg::ConfigId;
use crate::shard_ctrler::N_SHARDS;
use madsim::{task, time};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Duration;
use std::{net::SocketAddr, sync::Arc};

const CHECK_CONFIG_PERIOD: Duration = Duration::from_millis(100);

pub struct ShardKvServer {
    inner: Arc<Server<ShardKv>>,
    gid: u64,
    // kv_ck is used to send rpc when config get changed to transfer shards among group
    kv_ck: ClerkCore<ShardKvOp, Reply>,
    my_group: Vec<SocketAddr>,
}

impl ShardKvServer {
    pub async fn new(
        ctrl_ck: CtrlerClerk,
        servers: Vec<SocketAddr>,
        gid: u64,
        me: usize,
        max_raft_state: Option<usize>,
    ) -> Arc<Self> {
        let kv_ck: ClerkCore<ShardKvOp, Reply> = ClerkCore::new(vec![]);
        let inner = Server::new(servers.clone(), me, max_raft_state).await;
        let this = Arc::new(ShardKvServer {
            inner,
            gid,
            kv_ck,
            my_group: servers,
        });
        this.prepare_deamon(ctrl_ck);
        this
    }

    fn prepare_deamon(self: &Arc<Self>, ctrl_ck: CtrlerClerk) {
        let this = Arc::clone(&self);
        task::spawn(async move {
            this.check_config_deamon(ctrl_ck).await;
        })
        .detach();
    }

    // TODO When restoring, we cannot identify which shard is belong to current server.
    // This deamon will call `start_receive` after restoring, but the server maybe does not
    // need it.
    //
    // Solution: KvState should remember the config id all the time. When it sees a coming
    // Take or Receive request, it will check the config id to determine whether this request is duplicated.
    async fn check_config_deamon(self: Arc<Self>, ctrl_ck: CtrlerClerk) {
        // Server must check config one by one.
        // Ensure that every tranfer is correct and will not miss any data.
        'outer: loop {
            time::sleep(CHECK_CONFIG_PERIOD).await;
            if !self.inner.is_leader() {
                continue;
            }
            let prev_config_id = self.inner.get_config_id();
            let prev_config = ctrl_ck.query_at(prev_config_id).await;
            let config = ctrl_ck.query_at(prev_config_id + 1).await;
            if prev_config_id == config.num {
                continue;
            }
            let my_sid = config
                .shards
                .iter()
                .enumerate()
                .filter(|(_, gid)| **gid == self.gid)
                .map(|x| x.0)
                .collect::<Vec<_>>();
            // Shard transfer happened between group leaders
            let missing_shards_info = my_sid
                .iter()
                .filter(|sid| prev_config.shards[**sid] != self.gid)
                .map(|x| *x)
                .collect::<Vec<_>>();
            info!(
                "[ShardKv] Gid = {} my_sid = {:?} Missing sid = {:?}, prev_config = {:?}, config = {:?}",
                self.gid, my_sid, missing_shards_info, prev_config, config
            );
            // If `me` become follower after sending Op::Take,
            // the next-term leader cannot take again?
            // The solution is let each server keep their kv storage
            for (sid, gid) in missing_shards_info
                .into_iter()
                .map(|sid| (sid, prev_config.shards[sid]))
            {
                if gid == 0 {
                    match self
                        .send_to_my_group(ShardKvOp::new(
                            Op::Receive {
                                sid,
                                kv: HashMap::new(),
                                client: HashMap::new(),
                            },
                            config.num,
                        ))
                        .await
                    {
                        Ok(_) => continue,
                        Err(_) => continue 'outer,
                    }
                }
                info!(
                    "[ShardKv] Gid {} start send Take({}) to gid {}",
                    self.gid, sid, gid
                );
                self.kv_ck.increase_rid();
                loop {
                    match self
                        .kv_ck
                        .send_to_group(
                            ShardKvOp::new(Op::Take { sid }, config.num),
                            &prev_config.groups[&gid],
                        )
                        .await
                    {
                        Reply::Shard { sid, kv, client } => {
                            match self
                                .send_to_my_group(ShardKvOp::new(
                                    Op::Receive { sid, kv, client },
                                    config.num,
                                ))
                                .await
                            {
                                Ok(_) => break,
                                Err(_) => continue 'outer,
                            }
                        }
                        Reply::WrongGroup => {
                            // The target server is not ready
                            time::sleep(Duration::from_millis(50)).await;
                        }
                        x => panic!(
                            "[ShardKv] G{} send Op::Take({}) get reply = {:?}",
                            self.gid, sid, x
                        ),
                    }
                }
            }
            self.send_to_my_group(ShardKvOp::new(Op::UpdateConfigId, config.num))
                .await
                .expect("UpdateConfigId get");
        }
    }

    async fn send_to_my_group(self: &Arc<Self>, op: ShardKvOp) -> Result<(), ()> {
        if !matches!(op.op, Op::Receive { .. } | Op::UpdateConfigId) {
            panic!("KvShard can only send Op::Receive or Op::UpdateConfigId to their own groups");
        }
        self.kv_ck.increase_rid();
        match self.kv_ck.send_to_group(op.clone(), &self.my_group).await {
            Reply::Ok => Ok(()),
            Reply::WrongGroup if !self.inner.is_leader() => Err(()),
            x => panic!(
                "[ShardKv] G{} send {:?} to Self get = {:?}, self.config_id = {}",
                self.gid,
                op,
                x,
                self.inner.get_config_id(),
            ),
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ShardKv {
    // Your data here.
    storage: [HashMap<String, String>; N_SHARDS],
    /// `own` is used *only* for deciding whether a request with `config_id` = k
    ///  should be handle when `Op::Take` happened in the `config_id` = k + 1
    own: [bool; N_SHARDS],
    client: [HashMap<String, RecentInfo<Reply>>; N_SHARDS],

    // shard_config_ids: [ConfigId; N_SHARDS],
    config_id: ConfigId,
    config_client: HashMap<String, RecentInfo<Reply>>,
}

impl ShardKv {
    fn get_sid_from_cmd(cmd: &ClerkReq<ShardKvOp>) -> Option<usize> {
        match &cmd.req.op {
            Op::Put { key, .. } | Op::Append { key, .. } | Op::Get { key, .. } => {
                Some(key2shard(key))
            }
            Op::Take { sid, .. } | Op::Receive { sid, .. } => Some(*sid),
            _ => None,
        }
    }
}

impl State for ShardKv {
    type Command = ShardKvOp;
    type Output = Reply;

    fn apply(&mut self, cmd: ClerkReq<Self::Command>) -> Self::Output {
        let sid = Self::get_sid_from_cmd(&cmd);
        let config_id = cmd.req.config_id;
        let valid = self.config_id == config_id;
        let res = match cmd.req.op {
            Op::Put { key, value } => {
                let sid = sid.unwrap();
                info!(
                    "[ShardKvState] Put {{ k: {}, v: {} }}, config_id = {}, self.config_id= {}, sid = {}, valid = {}",
                    key, value, config_id, self.config_id, sid, valid
                );
                if valid && self.own[sid] {
                    self.storage[sid].insert(key, value);
                    Reply::Ok
                } else {
                    warn!(
                        "Wrong group reply happended in {:?}",
                        Op::Put { key, value }
                    );
                    Reply::WrongGroup
                }
            }
            Op::Append { key, value } => {
                let sid = sid.unwrap();
                info!(
                    "[ShardKvState] Append {{ k: {}, v: {} }}, config_id = {}, self.config_id= {}, sid = {}, valid = {}",
                    key, value, config_id, self.config_id, sid, valid
                );
                if valid && self.own[sid] {
                    let kv = &mut self.storage[sid];
                    if let Some(s) = kv.get_mut(&key) {
                        s.push_str(&value);
                    } else {
                        kv.insert(key, value);
                    }
                    Reply::Ok
                } else {
                    warn!(
                        "Wrong group reply happended in {:?}",
                        Op::Append { key, value }
                    );
                    Reply::WrongGroup
                }
            }
            Op::Get { key } => {
                let sid = key2shard(&key);
                info!(
                    "[ShardKvState] Get {{ k: {} }}, config_id = {}, self.config_id= {}, sid = {}, valid = {}",
                    key, config_id, self.config_id, sid, valid
                );
                if valid && self.own[sid] {
                    Reply::Get {
                        value: self.storage[sid].get(&key).cloned(),
                    }
                } else {
                    warn!("Wrong group reply happended in {:?}", Op::Get { key });
                    Reply::WrongGroup
                }
            }
            Op::Take { sid } => {
                info!(
                    "[ShardKvState] Take {{ sid: {} }} , config_id = {}, self.config_id= {}, valid = {}",
                    sid, config_id, self.config_id,
                    config_id <= self.config_id + 1
                );
                if config_id <= self.config_id + 1 {
                    if config_id == self.config_id + 1 {
                        self.own[sid] = false;
                    }
                    Reply::Shard {
                        sid,
                        kv: self.storage[sid].clone(),
                        client: self.client[sid].clone(),
                    }
                } else {
                    warn!("Wrong group reply happended in {:?}", cmd.req);
                    Reply::WrongGroup
                }
            }
            Op::Receive {
                sid,
                kv,
                client: shard_client,
            } => {
                info!(
                    "[ShardKvState] Receive {{ sid: {}, kv: {:?} }} , config_id = {}, self.config_id= {}, valid = {}",
                    sid,
                    kv, config_id, self.config_id,
                    config_id == self.config_id + 1
                );
                if config_id == self.config_id + 1 {
                    self.storage[sid] = kv;
                    self.own[sid] = true;
                    let client = &mut self.client[sid];
                    for (cid, recent_info) in shard_client.into_iter() {
                        match client.get_mut(&cid) {
                            Some(info) if info.rid >= recent_info.rid => {}
                            _ => {
                                client.insert(cid, recent_info);
                            }
                        }
                    }
                    Reply::Ok
                } else if config_id <= self.config_id {
                    // duplicated
                    Reply::Ok
                } else {
                    warn!(
                        "Wrong group reply happended in {:?}",
                        Op::Receive {
                            sid,
                            kv,
                            client: shard_client
                        }
                    );
                    Reply::WrongGroup
                }
            }
            Op::UpdateConfigId => {
                self.update_config_id(config_id);
                Reply::Ok
            }
        };
        let c = if matches!(sid, Some(..)) {
            &mut self.client[sid.unwrap()]
        } else {
            &mut self.config_client
        };
        if !matches!(res, Reply::WrongGroup) {
            c.insert(
                cmd.client,
                RecentInfo {
                    response: res.clone(),
                    rid: cmd.rid,
                },
            );
        }
        res
    }

    fn name() -> &'static str {
        "[ShardKv]"
    }

    fn duplicate(&self, req: &ClerkReq<Self::Command>) -> Option<Self::Output> {
        let c = if let Some(sid) = Self::get_sid_from_cmd(req) {
            &self.client[sid]
        } else {
            &self.config_client
        };
        match c.get(&req.client) {
            Some(RecentInfo { response, rid }) if *rid == req.rid => Some(response.clone()),
            _ => {
                if self.is_wrong_group(req) {
                    Some(Reply::WrongGroup)
                } else {
                    None
                }
            }
        }
    }
}

impl ShardKv {
    // pub fn get_invalid_sid(&self) -> Vec<usize> {
    //     info!("[ShardKv] valid = {:?}", self.valid);
    //     self.valid
    //         .iter()
    //         .enumerate()
    //         .filter(|(_, valid)| !**valid)
    //         .map(|x| x.0)
    //         .collect()
    // }

    fn is_wrong_group(&self, cmd: &ClerkReq<<ShardKv as State>::Command>) -> bool {
        let config_id = cmd.req.config_id;
        let sid = Self::get_sid_from_cmd(cmd);
        info!(
            "[ShardKv] Check is wrong group: cmd = {:?}, self.config_id = {}",
            cmd, self.config_id
        );
        match &cmd.req.op {
            Op::Append { .. } | Op::Get { .. } | Op::Put { .. } => {
                self.config_id != config_id || !self.own[sid.unwrap()]
            }
            Op::Take { .. } | Op::Receive { .. } => config_id > self.config_id + 1,
            _ => false,
        }
    }

    pub fn get_config_id(&self) -> ConfigId {
        self.config_id
    }

    pub fn update_config_id(&mut self, config_id: ConfigId) {
        self.config_id = config_id.max(self.config_id);
    }
}
