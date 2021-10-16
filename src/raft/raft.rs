use futures::{channel::mpsc, stream::FuturesUnordered, Future, StreamExt};
use madsim::{
    fs, net,
    rand::{self, Rng},
    task,
    time::*,
};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{self, Display},
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
pub struct RaftHandle {
    inner: Arc<Mutex<Raft>>,
}

type MsgSender = mpsc::UnboundedSender<ApplyMsg>;
pub type MsgRecver = mpsc::UnboundedReceiver<ApplyMsg>;

const HEARTBEAT_TIME: Duration = Duration::from_millis(200);
const RPC_TIMEOUT: Duration = Duration::from_millis(100);

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
pub enum ApplyMsg {
    Command {
        data: Vec<u8>,
        index: u64,
    },
    // For 2D:
    Snapshot {
        data: Vec<u8>,
        term: u64,
        index: u64,
    },
}

#[derive(Debug)]
pub struct Start {
    /// The index that the command will appear at if it's ever committed.
    pub index: u64,
    /// The current term.
    pub term: u64,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("this node is not a leader, next leader: {0}")]
    NotLeader(usize),
    #[error("IO error")]
    IO(#[from] io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LogEntry {
    entry: Vec<u8>,
    term: u64,
}

struct Raft {
    peers: Vec<SocketAddr>,
    me: usize,
    apply_ch: MsgSender,

    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    state: State,
    logs: Vec<LogEntry>,
    recv_hb_from_last_check: bool,
}

/// State of a raft peer.
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
struct State {
    term: u64,
    role: Role,
    // other state
    vote_for: Option<usize>,
    commit_index: u64,
    last_applied: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

impl Default for Role {
    fn default() -> Self {
        Role::Follower
    }
}

impl Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "<Role: {}>",
            match self {
                Role::Follower => "Follower",
                Role::Candidate => "Candidate",
                Role::Leader => "Leader",
            }
        )
    }
}

impl State {
    fn is_leader(&self) -> bool {
        matches!(self.role, Role::Leader)
    }
}

/// Data needs to be persisted.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Persist {
    // Your data here.
    term: u64,
    vote_for: Option<usize>,
    logs: Vec<LogEntry>,
}

impl fmt::Debug for Raft {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Raft({})", self.me)
    }
}

// HINT: put async functions here
impl RaftHandle {
    pub async fn new(peers: Vec<SocketAddr>, me: usize) -> (Self, MsgRecver) {
        let (apply_ch, recver) = mpsc::unbounded();
        let inner = Arc::new(Mutex::new(Raft {
            peers,
            me,
            apply_ch,
            state: State::default(),
            logs: Vec::new(),
            recv_hb_from_last_check: false,
        }));
        let handle = RaftHandle { inner };
        // initialize from state persisted before a crash
        handle.restore().await.expect("failed to restore");
        handle.start_rpc_server();

        handle.prepare_deamon();

        (handle, recver)
    }

    /// Start agreement on the next command to be appended to Raft's log.
    ///
    /// If this server isn't the leader, returns [`Error::NotLeader`].
    /// Otherwise start the agreement and return immediately.
    ///
    /// There is no guarantee that this command will ever be committed to the
    /// Raft log, since the leader may fail or lose an election.
    pub async fn start(&self, cmd: &[u8]) -> Result<Start> {
        let mut raft = self.inner.lock().unwrap();
        info!("{:?} start", *raft);
        raft.start(cmd)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        let raft = self.inner.lock().unwrap();
        raft.state.term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        let raft = self.inner.lock().unwrap();
        raft.state.is_leader()
    }

    /// A service wants to switch to snapshot.  
    ///
    /// Only do so if Raft hasn't have more recent info since it communicate
    /// the snapshot on `apply_ch`.
    pub async fn cond_install_snapshot(
        &self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        todo!()
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub async fn snapshot(&self, index: u64, snapshot: &[u8]) -> Result<()> {
        todo!()
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    async fn persist(&self) -> io::Result<()> {
        let (persist, snapshot): (Persist, Vec<u8>) = {
            let inner = self.inner.lock().unwrap();
            (
                Persist {
                    term: inner.state.term,
                    logs: inner.logs.clone(),
                    vote_for: inner.state.vote_for,
                },
                Vec::new(),
            )
        };
        // TODO implement snapshot
        let state = bincode::serialize(&persist).unwrap();

        // you need to store persistent state in file "state"
        // and store snapshot in file "snapshot".
        // DO NOT change the file names.
        let file = fs::File::create("state").await?;
        file.write_all_at(&state, 0).await?;
        // make sure data is flushed to the disk,
        // otherwise data will be lost on power fail.
        file.sync_all().await?;

        let file = fs::File::create("snapshot").await?;
        file.write_all_at(&snapshot, 0).await?;
        file.sync_all().await?;
        Ok(())
    }

    /// Restore previously persisted state.
    async fn restore(&self) -> io::Result<()> {
        match fs::read("snapshot").await {
            Ok(snapshot) => {
                todo!("restore snapshot");
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        match fs::read("state").await {
            Ok(state) => {
                let persist: Persist = bincode::deserialize(&state).unwrap();
                todo!("restore state");
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        Ok(())
    }

    fn start_rpc_server(&self) {
        let net = net::NetLocalHandle::current();

        let this = self.clone();
        net.add_rpc_handler(move |args: RequestVoteArgs| {
            let this = this.clone();
            async move { this.request_vote(args).await.unwrap() }
        });
        // add more RPC handlers here
        let this = self.clone();
        net.add_rpc_handler(move |args: AppendEntriesArgs| {
            let this = this.clone();
            async move { this.request_append(args).await.unwrap() }
        });
    }

    fn prepare_deamon(&self) {
        // start leader selection deamon
        let this = self.clone();
        task::spawn(async move { this.leader_selection_deamon().await }).detach();
        // start heartbeat deamon
        let this = self.clone();
        task::spawn(async move { this.heartbeat_deamon().await }).detach();
    }

    /// rpc handler when getting vote request from candidates
    async fn request_vote(&self, args: RequestVoteArgs) -> Result<RequestVoteReply> {
        let reply = {
            let mut inner = self.inner.lock().unwrap();
            inner.request_vote(args)
        };
        // if you need to persist or call async functions here,
        // make sure the lock is scoped and dropped.
        self.persist().await.expect("failed to persist");
        Ok(reply)
    }

    /// rpc handler when getting append request from leader
    async fn request_append(&self, args: AppendEntriesArgs) -> Result<AppendEntriesReply> {
        let reply = {
            let mut inner = self.inner.lock().unwrap();
            inner.request_append(args)
        };
        self.persist().await.expect("failed to persist");
        Ok(reply)
    }

    /// used for checking leader selection timeout periodically
    async fn leader_selection_deamon(&self) {
        loop {
            sleep(Raft::generate_election_timeout()).await;
            {
                let mut inner = self.inner.lock().unwrap();
                // if self is leader, do not issue leader selection
                // if self is not leader and already recv heartbeat, do not issue leader selection
                if inner.state.role == Role::Leader || inner.recv_hb_from_last_check {
                    inner.recv_hb_from_last_check = false;
                    continue;
                } else {
                    inner.state.role = Role::Candidate;
                }
            }
            let mut rpcs;
            {
                let mut inner = self.inner.lock().unwrap();
                rpcs = inner.send_vote_request();
            }
            let inner = Arc::clone(&self.inner);
            task::spawn(async move {
                let mut counter: usize = 1;
                while let Some(res) = rpcs.next().await {
                    if let Ok(res) = res {
                        let mut inner = inner.lock().unwrap();
                        if !inner.check_incoming_term(res.term) {
                            break;
                        }
                        if res.vote_granted {
                            counter += 1;
                        }
                        if counter > inner.peers.len()/2 {
                            info!("Server {} become leader", inner.me);
                            inner.state.role = Role::Leader;
                        }
                    }
                }
            })
            .detach();
        }
    }

    /// used for send heartbeat peridodically
    async fn heartbeat_deamon(&self) {
        loop {
            sleep(HEARTBEAT_TIME).await;
            {
                let inner = self.inner.lock().unwrap();
                if inner.state.role != Role::Leader {
                    continue;
                }
            }
            let mut rpcs = self.inner.lock().unwrap().send_heartbeat();
            let inner = Arc::clone(&self.inner);
            task::spawn(async move {
                while let Some(res) = rpcs.next().await {
                    if let Ok(res) = res {
                        let mut inner = inner.lock().unwrap();
                        if !inner.check_incoming_term(res.term) {
                            break;
                        }
                    }
                    // TODO "Handle append successfully and what to do if failed"
                }
            })
            .detach();
        }
    }
}

// HINT: put mutable non-async functions here
impl Raft {
    fn start(&mut self, data: &[u8]) -> Result<Start> {
        if !self.state.is_leader() {
            let leader = (self.me + 1) % self.peers.len();
            return Err(Error::NotLeader(leader));
        }
        todo!("start agreement");
    }

    // Here is an example to apply committed message.
    fn apply(&self) {
        let msg = ApplyMsg::Command {
            data: todo!("apply msg"),
            index: todo!("apply msg"),
        };
        self.apply_ch.unbounded_send(msg).unwrap();
    }

    /// Return true means pass the check: do not back to follower.
    /// Return false means fail the check: back to follower
    fn check_incoming_term(&mut self, incoming_term: u64) -> bool {
        if incoming_term > self.state.term {
            info!("Server {} with term = {} get larger term, back to follower", self.me, self.state.term);
            self.state.term = incoming_term;
            self.state.role = Role::Follower;
            false
        } else {
            true
        }
    }

    fn request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        let mut reply = RequestVoteReply {
            term: self.state.term,
            vote_granted: true,
        };
        match self.state.term.cmp(&args.term) {
            std::cmp::Ordering::Greater => reply.vote_granted = false,
            std::cmp::Ordering::Equal => match self.state.vote_for {
                None => self.state.vote_for = Some(args.candidate_id),
                Some(vote_for) => {
                    if vote_for != args.candidate_id {
                        reply.vote_granted = false;
                    }
                }
            },
            std::cmp::Ordering::Less => {
                self.state.vote_for = Some(args.candidate_id);
            }
        }
        self.check_incoming_term(args.term);
        info!(
            "Server {}({}) get vote request from Server {} with term = {}, granted = {}",
            self.me, self.state.role, args.candidate_id, args.term, reply.vote_granted,
        );
        reply
    }

    fn request_append(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        // first clear timer
        let mut reply = AppendEntriesReply {
            term: self.state.term,
            success: true,
        };
        match self.state.term.cmp(&args.term) {
            std::cmp::Ordering::Greater => reply.success = false,
            _ => {
                self.recv_hb_from_last_check = true;
                self.state.role = Role::Follower;
            },
        }
        self.check_incoming_term(args.term);
        // TODO: finish 2-5 on paper
        info!(
            "Server {}({}) get append request from Server {}, append_success = {}",
            self.me, self.state.role, args.leader_id, reply.success
        );
        reply
    }

    // Here is an example to generate random number.
    fn generate_election_timeout() -> Duration {
        // see rand crate for more details
        Duration::from_millis(rand::rng().gen_range(1000..2000))
    }

    // Here is an example to send RPC and manage concurrent tasks.
    fn send_vote_request(
        &mut self,
    ) -> FuturesUnordered<impl Future<Output = std::result::Result<RequestVoteReply, std::io::Error>>>
    {
        let args: RequestVoteArgs = RequestVoteArgs {
            term: {
                self.state.term += 1;
                self.state.term
            },
            candidate_id: self.me,
            last_log_index: self.logs.len() as u64,
            last_log_term: self.logs.last().map_or_else(|| 0, |log| log.term),
        };
        let net = net::NetLocalHandle::current();

        let rpcs = FuturesUnordered::new();
        for (i, &peer) in self.peers.iter().enumerate() {
            if i == self.me {
                continue;
            }
            // NOTE: `call` function takes ownerships
            let net = net.clone();
            let args = args.clone();
            rpcs.push(async move {
                net.call_timeout::<RequestVoteArgs, RequestVoteReply>(peer, args, RPC_TIMEOUT)
                    .await
            });
        }
        rpcs

        // let current_term = self.state.term;
        // let bound = self.peers.len() / 2;
        // // spawn a concurrent task
        // task::spawn(async move {
        //     // handle RPC tasks in completion order
        //     let mut counter: usize = 0;
        //     while let Some(res) = rpcs.next().await {
        //         if let Ok(res) = res {
        //             if res.vote_granted {
        //                 counter += 1;
        //             } else if res.term > current_term {
        //                 break;
        //             }
        //             if counter > bound {
        //                 break;
        //             }
        //         }
        //     }
        // })
        // .detach(); // NOTE: you need to detach a task explicitly, or it will be cancelled on drop
    }

    fn send_heartbeat(
        &self,
    ) -> FuturesUnordered<
        impl Future<Output = std::result::Result<AppendEntriesReply, std::io::Error>>,
    > {
        assert_eq!(
            self.state.role,
            Role::Leader,
            "Non-leader is sending AppendEntries RPC"
        );
        let args = AppendEntriesArgs {
            term: self.state.term,
            leader_id: self.me,
            prev_log_index: self.logs.len() as u64,
            prev_log_term: self.logs.last().map_or_else(|| 0, |log| log.term),
            entries: Vec::new(),
            leader_commit: self.state.commit_index,
        };
        let net = net::NetLocalHandle::current();
        let rpcs = FuturesUnordered::new();
        for (i, &peer) in self.peers.iter().enumerate() {
            if i == self.me {
                continue;
            }
            let net = net.clone();
            let args = args.clone();
            rpcs.push(async move {
                net.call_timeout::<_, AppendEntriesReply>(peer, args, RPC_TIMEOUT)
                    .await
            });
        }
        rpcs
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RequestVoteArgs {
    // Your data here.
    term: u64, // term for the candidate requesting vote
    candidate_id: usize,
    last_log_index: u64,
    last_log_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RequestVoteReply {
    // Your data here.
    term: u64, //term of the voter
    vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AppendEntriesArgs {
    term: u64,
    leader_id: usize,
    prev_log_index: u64,
    prev_log_term: u64,
    entries: Vec<LogEntry>,
    leader_commit: u64, //leader's commit_index
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AppendEntriesReply {
    term: u64,
    success: bool,
}
