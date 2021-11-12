use super::msg::*;
use crate::raft::{self, ApplyMsg, MsgRecver};
use futures::StreamExt;
use madsim::{
    net, task,
    time::{self, Instant},
    Handle,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::Debug,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

const APPLY_CHECK_PERIOD: Duration = Duration::from_millis(25);
const APPLY_CHECK_TIMEOUT: Duration = Duration::from_millis(500);
const SNAPSHOT_CHECK_PERIOD: Duration = Duration::from_millis(250);

pub trait State: net::Message + Default {
    type Command: net::Message + Clone;
    type Output: net::Message + Clone;
    /// apply normal operation (non-register) to state machine
    fn apply(&mut self, cmd: Self::Command) -> Self::Output;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RecentInfo<T> {
    rid: u64, // client's recent request id
    // response: Option<T>, // client's response corresponding to rid
    response: T, // client's response corresponding to rid
}

pub struct Server<S: State> {
    rf: raft::RaftHandle,
    me: usize,
    // _marker: PhantomData<S>,
    state: Mutex<S>,
    cstate: Mutex<HashMap<String, RecentInfo<S::Output>>>,
    recent_apply_index: AtomicU64, //used for snapshpt
}

impl<S: State> Server<S> {
    pub async fn new(
        servers: Vec<SocketAddr>,
        me: usize,
        max_raft_state: Option<usize>,
    ) -> Arc<Self> {
        // You may need initialization code here.
        let my_addr = servers[me];
        let (rf, mut apply_ch) = raft::RaftHandle::new(servers, me).await;
        let this = Arc::new(Server {
            rf,
            me,
            // _marker: PhantomData,
            state: Mutex::new(S::default()),
            cstate: Mutex::new(HashMap::new()),
            recent_apply_index: AtomicU64::new(0),
        });
        this.restore(&mut apply_ch).await;
        this.start_rpc_server();
        this.prepare_deamon(apply_ch, my_addr, max_raft_state);
        this
    }

    fn prepare_deamon(
        self: &Arc<Self>,
        apply_ch: MsgRecver,
        my_addr: SocketAddr,
        max_raft_state: Option<usize>,
    ) {
        let this = self.clone();
        task::spawn(async move {
            this.start_recv_from_raft_deamon(apply_ch).await;
        })
        .detach();

        let this = self.clone();
        if max_raft_state.is_some() {
            task::spawn(async move {
                this.snapshot_check_deamon(my_addr, max_raft_state.unwrap())
                    .await
            })
            .detach();
        }
    }

    async fn restore(self: &Arc<Self>, recver: &mut MsgRecver) {
        match recver.next().await.unwrap() {
            ApplyMsg::Command { .. } => {
                panic!("[AppServer] S{} get ApplyMsg::Command when restore", self.me)
            }
            ApplyMsg::Snapshot { data, index, term } => {
                self.recent_apply_index.store(index, Ordering::Release);
                if index > 0 && term > 0 {
                    let (state, cstate) = bincode::deserialize(&data).unwrap();
                    *self.state.lock().unwrap() = state;
                    *self.cstate.lock().unwrap() = cstate;
                }
            }
        }
    }

    fn start_rpc_server(self: &Arc<Self>) {
        let net = net::NetLocalHandle::current();

        let this = self.clone();
        net.add_rpc_handler(move |req: ClerkReq<S::Command>| {
            let this = this.clone();
            async move { this.apply(req).await }
        });
    }

    async fn start_recv_from_raft_deamon(self: Arc<Self>, mut recver: MsgRecver) {
        while let Some(msg) = recver.next().await {
            match msg {
                ApplyMsg::Command { data, index } => {
                    // update state
                    let command: ClerkReq<S::Command> = bincode::deserialize(&data).unwrap();
                    let mut cstate = self.cstate.lock().unwrap();
                    match cstate.get_mut(&command.client) {
                        Some(res) if res.rid == command.rid => {}
                        _ => {
                            info!("[AppServer] S{} apply log[{}] {:?}", self.me, index, command);
                            cstate.insert(
                                command.client,
                                RecentInfo {
                                    rid: command.rid,
                                    response: self.state.lock().unwrap().apply(command.req),
                                },
                            );
                        }
                    }
                    // update recent_apply_index for snapshot
                    self.recent_apply_index.store(index, Ordering::Release);
                }
                ApplyMsg::Snapshot { data, index, term } => {
                    if self.rf.cond_install_snapshot(term, index, &data).await {
                        match bincode::deserialize(&data) {
                            Ok((state, cstate)) => {
                                *self.state.lock().unwrap() = state;
                                *self.cstate.lock().unwrap() = cstate;
                                self.recent_apply_index.store(index, Ordering::Release);
                            }
                            Err(err) => {
                                warn!(
                                    "[AppServer] S{} Deserialize Snapshot get err = {:?}",
                                    self.me, err
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    async fn snapshot_check_deamon(self: Arc<Self>, my_addr: SocketAddr, max_raft_state: usize) {
        let fs = Handle::current().fs;
        loop {
            time::sleep(SNAPSHOT_CHECK_PERIOD).await;
            if matches!(fs.get_file_size(my_addr, "state"), Ok(size) if size as usize > max_raft_state)
            {
                let snapshot = {
                    let state = (&*self.state.lock().unwrap(), &*self.cstate.lock().unwrap());
                    bincode::serialize(&state).unwrap()
                };
                self.rf
                    .snapshot(self.recent_apply_index.load(Ordering::Acquire), &snapshot)
                    .await
                    .unwrap();
            }
        }
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.rf.term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.rf.is_leader()
    }

    async fn apply(&self, cmd: ClerkReq<S::Command>) -> Result<S::Output, Error> {
        let t = Instant::now();
        info!("[AppServer] S{} get new cmd = {:?}", self.me, cmd);
        match self.cstate.lock().unwrap().get(&cmd.client) {
            Some(cres) => {
                if cres.rid == cmd.rid {
                    return Ok(cres.response.clone());
                }
            }
            _ => {}
        }
        let cmd_data = bincode::serialize(&cmd).unwrap();
        match self.rf.start(&cmd_data).await {
            Ok(_) => {}
            Err(err) => match err {
                raft::Error::NotLeader(hint) => return Err(Error::NotLeader { hint }),
                _ => panic!("Non-recoverable Error occur: {}", err),
            },
        }
        while t.elapsed() < APPLY_CHECK_TIMEOUT {
            time::sleep(APPLY_CHECK_PERIOD).await;
            match self.cstate.lock().unwrap().get(&cmd.client) {
                Some(RecentInfo { rid, response, .. }) if *rid == cmd.rid => {
                    return Ok(response.clone());
                }
                _ => {}
            }
        }
        Err(Error::Timeout)
    }
}

pub type KvServer = Server<Kv>;

#[derive(Debug, Serialize, Deserialize)]
pub struct Kv {
    // Your data here.
    kv: HashMap<String, String>,
}

impl State for Kv {
    type Command = Op;
    type Output = String;
    fn apply(&mut self, cmd: Self::Command) -> Self::Output {
        match cmd {
            Op::Get { key } => self
                .kv
                .get(&key)
                .map_or_else(|| String::from(""), |v| v.to_owned()),
            Op::Put { key, value } => {
                self.kv.insert(key, value);
                String::from("")
            }
            Op::Append { key, value } => {
                if let Some(v) = self.kv.get_mut(&key) {
                    v.push_str(&value);
                } else {
                    self.kv.insert(key, value);
                }
                String::from("")
            }
        }
    }
}

impl Default for Kv {
    fn default() -> Self {
        Kv { kv: HashMap::new() }
    }
}
