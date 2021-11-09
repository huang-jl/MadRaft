use super::msg::*;
use crate::raft::{self, ApplyMsg, MsgRecver, Start};
use futures::StreamExt;
use madsim::{
    net, task,
    time::{self, Instant},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::{self, Debug},
    marker::PhantomData,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

const APPLY_CHECK_PERIOD: Duration = Duration::from_millis(25);
const APPLY_CHECK_TIMEOUT: Duration = Duration::from_millis(500);

pub trait State: net::Message + Default {
    type Command: net::Message + Clone;
    type Output: net::Message + Clone;
    /// apply normal operation (non-register) to state machine
    fn apply(&mut self, cmd: Self::Command) -> Self::Output;
}

pub struct Server<S: State> {
    rf: raft::RaftHandle,
    me: usize,
    // _marker: PhantomData<S>,
    state: Mutex<S>,
    cstate: Mutex<HashMap<String, ClientRecentResponse<S::Output>>>,
}

impl<S: State> fmt::Debug for Server<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Server({})", self.me)
    }
}

impl<S: State> Server<S> {
    pub async fn new(
        servers: Vec<SocketAddr>,
        me: usize,
        max_raft_state: Option<usize>,
    ) -> Arc<Self> {
        // You may need initialization code here.
        let (rf, apply_ch) = raft::RaftHandle::new(servers, me).await;
        let this = Arc::new(Server {
            rf,
            me,
            // _marker: PhantomData,
            state: Mutex::new(S::default()),
            cstate: Mutex::new(HashMap::new()),
        });
        this.start_rpc_server();
        this.start_recv_from_raft_deamon(apply_ch);
        this
    }

    fn start_rpc_server(self: &Arc<Self>) {
        let net = net::NetLocalHandle::current();

        let this = self.clone();
        net.add_rpc_handler(move |req: ClerkReq<S::Command>| {
            let this = this.clone();
            async move { this.apply(req).await }
        });
    }

    fn start_recv_from_raft_deamon(self: &Arc<Self>, mut recver: MsgRecver) {
        let this = self.clone();
        task::spawn(async move {
            while let Some(msg) = recver.next().await {
                match msg {
                    ApplyMsg::Command { data, .. } => {
                        let command: ClerkReq<S::Command> = bincode::deserialize(&data).unwrap();
                        let mut cstate = this.cstate.lock().unwrap();
                        match cstate.get_mut(&command.client) {
                            Some(res) if res.rid == command.rid => {}
                            _ => {
                                info!("[KVServer] S{} apply {:?}", this.me, command);
                                cstate.insert(
                                    command.client,
                                    ClientRecentResponse {
                                        rid: command.rid,
                                        response: this.state.lock().unwrap().apply(command.req),
                                    },
                                );
                            }
                        }
                    }
                    ApplyMsg::Snapshot { data, index, term } => {}
                }
            }
        })
        .detach();
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
        info!("[KVServer] S{} get new cmd = {:?}", self.me, cmd);
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
                Some(ClientRecentResponse { rid, response, .. }) if *rid == cmd.rid => {
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
struct ClientRecentResponse<T> {
    rid: u64, // client's recent request id
    // response: Option<T>, // client's response corresponding to rid
    response: T, // client's response corresponding to rid
}

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
