use std::collections::HashMap;

use super::{msg::*, N_SHARDS};
use crate::kvraft::server::{Server, State};
use serde::{Deserialize, Serialize};

pub type ShardCtrler = Server<ShardInfo>;

#[derive(Debug, Serialize, Deserialize)]
pub struct ShardInfo {
    // Your data here.
    configs: Vec<Config>,
}

impl State for ShardInfo {
    type Command = Op;
    type Output = Option<Config>;

    fn apply(&mut self, cmd: Self::Command) -> Self::Output {
        match cmd {
            Op::Join { groups } => {
                let mut config = self.configs.last().unwrap().clone();
                for (gid, servers) in groups.into_iter() {
                    assert_eq!(
                        config.groups.get(&gid),
                        None,
                        "[Shard] Joined gid = {} already in ShardInfo",
                        gid
                    );
                    config.groups.insert(gid, servers);
                }
                config.num = self.configs.len() as u64;
                config.rebalance();
                self.configs.push(config);
                None
            }
            Op::Leave { gids } => {
                let mut config = self.configs.last().unwrap().clone();
                for gid in gids.into_iter() {
                    assert_ne!(
                        config.groups.get(&gid),
                        None,
                        "[Shard] Leaved gid = {} not in ShardInfo",
                        gid
                    );
                    config.groups.remove(&gid);
                }
                config.num = self.configs.len() as u64;
                config.rebalance();
                self.configs.push(config);
                None
            }
            Op::Move { gid, shard } => {
                let mut config = self.configs.last().unwrap().clone();
                assert_ne!(
                    config.groups.get(&gid),
                    None,
                    "[Shard] Move shard {} to non-exist group {}",
                    shard,
                    gid
                );
                config.shards[shard] = gid;
                config.num = self.configs.len() as u64;
                self.configs.push(config);
                None
            }
            Op::Query { num } => {
                let num = num as usize;
                if num >= self.configs.len() {
                    self.configs.last().cloned()
                } else {
                    self.configs.get(num).cloned()
                }
            }
        }
    }
}

impl Default for ShardInfo {
    fn default() -> Self {
        ShardInfo {
            configs: vec![Config {
                num: 0,
                shards: [0; N_SHARDS],
                groups: HashMap::new(),
            }],
        }
    }
}

impl Config {
    /// Rebalance shard of groups.
    ///
    /// Attention: Because the *iteration order is different* among replicated servers,
    /// we have to sort them to vector to guarantee the rebalance is same among replicated servers
    fn rebalance(&mut self) {
        let group_num = self.groups.len();
        if group_num == 0 {
            return;
        }
        let avg_num = N_SHARDS / group_num;
        let mut group_shard: HashMap<u64, Vec<usize>> = self
            .groups
            .keys()
            .map(|&gid| {
                (
                    gid,
                    self.shards
                        .iter()
                        .enumerate()
                        .filter(|(_, &shard_gid)| shard_gid == gid)
                        .map(|x| x.0)
                        .collect(),
                )
            })
            .collect();
        let mut above_avg_gids: Vec<Gid> = group_shard
            .iter()
            .filter(|(_, num)| num.len() > avg_num)
            .map(|(&gid, _)| gid)
            .collect();
        // from larger to smaller
        above_avg_gids.sort_unstable_by(|a, b| {
            let a_shard_num = group_shard.get(a).unwrap().len();
            let b_shard_num = group_shard.get(b).unwrap().len();
            if a_shard_num == b_shard_num {
                b.cmp(&a)
            } else {
                b_shard_num.cmp(&a_shard_num)
            }
        });
        let mut below_avg_gids: Vec<Gid> = group_shard
            .iter()
            .filter(|(_, num)| num.len() < avg_num)
            .map(|(&gid, _)| gid)
            .collect();
        below_avg_gids.sort_unstable();
        let mut orphan_shards: Vec<usize> = self
            .shards
            .iter()
            .enumerate()
            .filter(|(_, gid)| !self.groups.contains_key(*gid))
            .map(|x| x.0)
            .collect();

        // start rebalance
        // 1. allocate orphan_shards to below_avg_gids
        while !orphan_shards.is_empty() && !below_avg_gids.is_empty() {
            let shard_id = orphan_shards.pop().unwrap();
            let gid = *below_avg_gids.last().unwrap();
            let s = group_shard.get_mut(&gid).unwrap();
            s.push(shard_id);
            if s.len() >= avg_num {
                below_avg_gids.pop();
            }
        }
        if orphan_shards.is_empty() {
            // 2. allocate from above_avg_gids to below_avg_gids
            while !below_avg_gids.is_empty() {
                let above_gid = *above_avg_gids.last().unwrap();
                let above_s = group_shard.get_mut(&above_gid).unwrap();
                let shard_id = above_s.pop().unwrap();
                if above_s.len() <= avg_num {
                    above_avg_gids.pop();
                }
                let below_gid = *below_avg_gids.last().unwrap();
                let below_s = group_shard.get_mut(&below_gid).unwrap();
                below_s.push(shard_id);
                if below_s.len() >= avg_num {
                    below_avg_gids.pop();
                }
            }
            // 3.now every group has exact avg_num of shard except those in above_avg_gids
            let mut avg_gids: Vec<u64> = group_shard
                .iter()
                .filter(|(_, v)| v.len() == avg_num)
                .map(|x| *x.0)
                .collect();
            avg_gids.sort_unstable();
            // assert!(avg_gids.len() + above_avg_gids.len() == group_num);
            while !above_avg_gids.is_empty() {
                let above_gid = *above_avg_gids.last().unwrap();
                let above_s = group_shard.get_mut(&above_gid).unwrap();
                if above_s.len() <= avg_num + 1 {
                    above_avg_gids.pop();
                    continue;
                }
                let shard_id = above_s.pop().unwrap();
                group_shard
                    .get_mut(&avg_gids.pop().unwrap())
                    .unwrap()
                    .push(shard_id);
            }
        } else {
            // we have unused orphan shard
            for value in group_shard.values() {
                assert!(value.len() >= avg_num);
            }
            let mut gids: Vec<u64> = group_shard
                .iter()
                .filter(|x| x.1.len() == avg_num)
                .map(|x| *x.0)
                .collect();
            gids.sort_unstable();
            while !orphan_shards.is_empty() {
                let gid = gids.pop().unwrap();
                group_shard
                    .get_mut(&gid)
                    .unwrap()
                    .push(orphan_shards.pop().unwrap());
            }
        }
        for s in group_shard.values() {
            assert!(s.len() >= avg_num && s.len() <= avg_num + 1);
        }
        // finish rebalance
        for (gid, s) in group_shard {
            for shard_id in s {
                self.shards[shard_id] = gid;
            }
        }
    }
}
