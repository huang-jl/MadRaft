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

fn generate_election_timeout() -> Duration {
    Duration::from_millis(rand::rng().gen_range(1000..2000))
}
const APPEND_PERIOD: Duration = Duration::from_millis(100);
const APPLY_PERIOD: Duration = Duration::from_millis(50);
const COMMIT_CHECK_PERIOD: Duration = Duration::from_millis(50);
const RPC_TIMEOUT: Duration = Duration::from_millis(50);

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
impl LogEntry {
    fn new(data: &[u8], term: u64) -> Self {
        LogEntry {
            entry: data.to_vec(),
            term,
        }
    }
    fn empty() -> Self {
        LogEntry {
            entry: vec![],
            term: 0,
        }
    }
}

struct Raft {
    peers: Vec<SocketAddr>,
    me: usize,
    apply_ch: MsgSender,

    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    state: State,
    logs: Vec<LogEntry>,           //persistent
    recv_hb_from_last_check: bool, //volatile
    next_index: Vec<u64>,          // index of next log send to each server
    match_index: Vec<u64>,         // index of highest entry known to be replicated on each server
}

/// State of a raft peer.
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
struct State {
    //persistent:
    term: u64,
    vote_for: Option<usize>,
    //volatile:
    commit_index: u64,
    last_applied: u64,
    role: Role,
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
        let server_num = peers.len();
        let inner = Arc::new(Mutex::new(Raft {
            peers,
            me,
            apply_ch,
            state: State::default(),
            logs: vec![LogEntry::empty()],
            recv_hb_from_last_check: false,
            next_index: vec![0; server_num],
            match_index: vec![0; server_num],
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
        let res = self.inner.lock().unwrap().start(cmd);
        self.persist().await.unwrap();
        res
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
            let raft = self.inner.lock().unwrap();
            (
                Persist {
                    term: raft.state.term,
                    logs: raft.logs.clone(),
                    vote_for: raft.state.vote_for,
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

        // let file = fs::File::create("snapshot").await?;
        // file.write_all_at(&snapshot, 0).await?;
        // file.sync_all().await?;
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
                self.inner.lock().unwrap().restore(persist);
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
        // start commit check deamon
        let this = self.clone();
        task::spawn(async move { this.commit_check_deamon().await }).detach();
        // start log apply deamon
        let this = self.clone();
        task::spawn(async move { this.apply_deamon().await }).detach();
    }

    /// rpc handler when getting vote request from candidates
    async fn request_vote(&self, args: RequestVoteArgs) -> Result<RequestVoteReply> {
        let reply = {
            let mut raft = self.inner.lock().unwrap();
            raft.request_vote(args)
        };
        // if you need to persist or call async functions here,
        // make sure the lock is scoped and dropped.
        self.persist().await.expect("failed to persist");
        Ok(reply)
    }

    /// rpc handler when getting append request from leader
    async fn request_append(&self, args: AppendEntriesArgs) -> Result<AppendEntriesReply> {
        let reply = {
            let mut raft = self.inner.lock().unwrap();
            raft.request_append(args)
        };
        self.persist().await.expect("failed to persist");
        Ok(reply)
    }

    /// used for checking leader selection timeout periodically
    /// Note: this deamon will never terminate after this server is initialized
    async fn leader_selection_deamon(&self) {
        loop {
            sleep(generate_election_timeout()).await;
            {
                let mut raft = self.inner.lock().unwrap();
                // if self is leader, do not issue leader selection
                // if self is not leader and already recv heartbeat, do not issue leader selection
                if raft.state.is_leader() || raft.recv_hb_from_last_check {
                    raft.recv_hb_from_last_check = false;
                    continue;
                } else {
                    raft.state.role = Role::Candidate;
                }
            }
            let mut rpcs = self.inner.lock().unwrap().send_vote_request();
            self.persist().await.unwrap();
            let inner = Arc::clone(&self.inner);
            let this = self.clone();
            task::spawn(async move {
                let mut counter: usize = 1;
                while let Some(res) = rpcs.next().await {
                    if let Ok(res) = res {
                        let mut raft = inner.lock().unwrap();
                        if !raft.check_incoming_term(res.term) {
                            break;
                        }
                        if res.vote_granted {
                            counter += 1;
                        }
                        if counter > raft.peers.len() / 2 {
                            //TODO send heartbeat immediately after becoming leader
                            info!("[Vote] Server {} become leader", raft.me);
                            raft.state.role = Role::Leader;
                            // initialize nextIndex and matchIndex after becoming leaders
                            let last_log_index = raft.last_log_index();
                            for item in raft.next_index.iter_mut() {
                                *item = last_log_index + 1;
                            }
                            for item in raft.match_index.iter_mut() {
                                *item = 0;
                            }
                            drop(raft);
                            task::spawn(async move { this.leader_append_deamon().await }).detach();
                            break;
                        }
                    }
                }
            })
            .detach();
        }
    }

    /// used for send heartbeat peridodically
    /// Note: supposed to call this every time becoming leader
    /// and this coroutine will terminate automatically after downgrading from leader
    async fn leader_append_deamon(&self) {
        let temp_term;
        {
            let raft = self.inner.lock().unwrap();
            assert!(
                raft.state.is_leader(),
                "Non-leader Server {} create a leader append deamon",
                raft.me
            );
            temp_term = raft.state.term;
        }
        loop {
            // log_id means the highest index of log entries inside the rpc which is sent to the follower
            let (mut rpcs, last_send_entry_id) = {
                let raft = self.inner.lock().unwrap();
                (raft.send_append_rpc(), raft.last_log_index())
            };
            let inner = Arc::clone(&self.inner);
            task::spawn(async move {
                while let Some(res) = rpcs.next().await {
                    if let Ok((peer_id, next_id, res)) = res {
                        let mut raft = inner.lock().unwrap();
                        if !raft.check_incoming_term(res.term) {
                            break;
                        }
                        if res.success {
                            raft.update_next_index(peer_id, last_send_entry_id + 1);
                            raft.update_match_index(peer_id, last_send_entry_id);
                        } else {
                            let next_index = {
                                match res.conflict.term {
                                    Some(_) => raft.get_next_index(next_id - 1, res.conflict),
                                    None => res.conflict.first_index,
                                }
                            };
                            raft.next_index[peer_id] = next_index.min(raft.next_index[peer_id]);
                            info!("[Append] Leader {} append to {} failed due to log inconsistency, next_index = {}",
                                raft.me, peer_id, raft.next_index[peer_id]);
                        }
                    }
                    // TODO "Handle append successfully and what to do if failed"
                }
            })
            .detach();
            sleep(APPEND_PERIOD).await;
            {
                let raft = self.inner.lock().unwrap();
                if raft.state.term != temp_term {
                    break;
                }
            }
        }
    }

    async fn commit_check_deamon(&self) {
        loop {
            sleep(COMMIT_CHECK_PERIOD).await;
            let mut raft = self.inner.lock().unwrap();
            if !raft.state.is_leader() {
                continue;
            }
            let max_match_index = *raft.match_index.iter().max().unwrap();
            let bound = raft.peers.len() / 2;
            // update leader's commit index
            for n in (raft.state.commit_index + 1..=max_match_index).rev() {
                if raft.logs[n as usize].term != raft.state.term {
                    continue;
                }
                // we calculate the # of peers which match_id > n
                if raft
                    .match_index
                    .iter()
                    .enumerate()
                    .filter(|&(peer_id, _)| peer_id != raft.me)
                    .fold(
                        0,
                        |prev, (_, &match_id)| if match_id >= n { prev + 1 } else { prev },
                    )
                    + 1
                    > bound
                {
                    raft.state.commit_index = n;
                    break;
                }
            }
        }
    }

    async fn apply_deamon(&self) {
        loop {
            sleep(APPLY_PERIOD).await;
            // Apply one log at a time
            // TODO: apply as much as possible?
            self.inner.lock().unwrap().apply_one();
        }
    }
}

// HINT: put mutable non-async functions here
impl Raft {
    fn start(&mut self, data: &[u8]) -> Result<Start> {
        if !self.state.is_leader() {
            return Err(Error::NotLeader(self.me));
        }
        self.logs.push(LogEntry::new(data, self.state.term));
        info!(
            "[Start] Server {} push a new log[{}] locally",
            self.me,
            self.last_log_index()
        );
        Ok(Start {
            index: self.last_log_index(),
            term: self.last_log_term(),
        })
    }

    /// Check role as leader before call this method
    ///
    /// The results of RPC is (peer_id, next_index of this peer id, reply)
    fn send_append_rpc(
        &self,
    ) -> FuturesUnordered<
        impl Future<Output = std::result::Result<(usize, u64, AppendEntriesReply), std::io::Error>>,
    > {
        let rpcs = FuturesUnordered::new();
        for peer_index in 0..self.peers.len() {
            if peer_index == self.me {
                continue;
            }
            rpcs.push(self.send_single_append_rpc(peer_index));
        }
        rpcs
    }

    /// Check role as leader before call this method
    fn send_single_append_rpc(
        &self,
        peer_index: usize,
    ) -> impl Future<Output = std::result::Result<(usize, u64, AppendEntriesReply), std::io::Error>>
    {
        assert!(
            self.state.is_leader(),
            "Non-leader Server {} send append request",
            self.me
        );
        assert_ne!(
            peer_index, self.me,
            "Leader Server {} send append request to it self",
            self.me
        );
        let prev_log_index = self.next_index[peer_index] - 1;
        let prev_log_term = self.logs[prev_log_index as usize].term;
        let request = AppendEntriesArgs {
            term: self.state.term,
            leader_id: self.me,
            prev_log_index,
            prev_log_term,
            entries: self
                .logs
                .get(self.next_index[peer_index] as usize..)
                .map_or(vec![], |e| e.iter().cloned().collect()),
            leader_commit: self.state.commit_index,
        };
        let net = net::NetLocalHandle::current();
        let peer = self.peers[peer_index];
        let next_index = self.next_index[peer_index];
        async move {
            net.call_timeout::<AppendEntriesArgs, AppendEntriesReply>(peer, request, RPC_TIMEOUT)
                .await
                .map(|res| (peer_index, next_index, res))
        }
    }

    // apply a single log to state machine if availiable
    fn apply_one(&mut self) {
        if self.state.commit_index > self.state.last_applied {
            self.state.last_applied += 1;
            info!(
                "[Apply] Server {} apply log {}",
                self.me, self.state.last_applied
            );
            let log = &self.logs[self.state.last_applied as usize];
            let msg = ApplyMsg::Command {
                data: log.entry.clone(),
                index: self.state.last_applied,
            };
            self.apply_ch.unbounded_send(msg).unwrap();
        }
    }

    /// Return true means pass the check: do not back to follower.
    /// Return false means fail the check: back to follower
    fn check_incoming_term(&mut self, incoming_term: u64) -> bool {
        if incoming_term > self.state.term {
            info!(
                "Server {} with term = {} get larger term, back to follower",
                self.me, self.state.term
            );
            self.state.term = incoming_term;
            self.state.role = Role::Follower;
            false
        } else {
            true
        }
    }

    fn request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        use std::cmp::Ordering::{Equal, Greater, Less};
        let mut reply = RequestVoteReply {
            term: self.state.term,
            vote_granted: true,
        };
        match self.state.term.cmp(&args.term) {
            Greater => reply.vote_granted = false,
            Equal => {
                if matches!(self.state.vote_for, Some(id) if id != args.candidate_id) {
                    reply.vote_granted = false;
                }
            }
            Less => {
                self.state.role = Role::Follower;
                self.state.term = args.term;
            }
        }
        if self.is_more_up_to_date(args.last_log_term, args.last_log_index) {
            info!(
                "[Vote] S{}(last_term = {}, last_index = {}) more update that S{}",
                self.me,
                self.last_log_term(),
                self.last_log_index(),
                args.candidate_id
            );
            reply.vote_granted = false;
        }
        if reply.vote_granted {
            self.state.vote_for = Some(args.candidate_id);
            self.state.term = self.state.term.max(args.term);
        }
        info!(
            "[Vote] S{}({}) term = {} get vote request from S{}: {:?}, granted = {}",
            self.me, self.state.role, reply.term, args.candidate_id, args, reply.vote_granted,
        );
        reply
    }

    fn request_append(&mut self, mut args: AppendEntriesArgs) -> AppendEntriesReply {
        use std::cmp::Ordering::Greater;
        let mut reply = AppendEntriesReply {
            term: self.state.term,
            success: true,
            conflict: ConflictInfo {
                term: None,
                first_index: self.last_log_index() + 1,
            },
        };
        info!(
            "[Append] S{}(term = {}) get append request from S{}: {}",
            self.me, self.state.term, args.leader_id, args
        );
        match self.state.term.cmp(&args.term) {
            Greater => reply.success = false,
            _ => {
                self.recv_hb_from_last_check = true;
                self.state.role = Role::Follower;
                self.state.term = args.term;
                // append log
                if let Some(log) = self.logs.get(args.prev_log_index as usize) {
                    if log.term != args.prev_log_term {
                        reply.success = false;
                        reply.conflict = ConflictInfo {
                            term: Some(log.term),
                            first_index: self.get_first_index_of_term(log.term).unwrap(),
                        };
                        self.logs.truncate(args.prev_log_index as usize);
                    } else {
                        // append success:
                        self.append_logs(args.entries, args.prev_log_index);
                        // update commit index
                        if args.leader_commit > self.state.commit_index {
                            self.state.commit_index = args.leader_commit.min(self.last_log_index());
                        }
                    }
                } else {
                    reply.success = false;
                }
            }
        }
        info!(
            "Reply = {:?}, last_log_index = {}, commit_index = {}",
            reply,
            self.last_log_index(),
            self.state.commit_index,
        );
        reply
    }

    // Here is an example to send RPC and manage concurrent tasks.
    fn send_vote_request(
        &mut self,
    ) -> FuturesUnordered<impl Future<Output = std::result::Result<RequestVoteReply, std::io::Error>>>
    {
        assert_eq!(
            self.state.role,
            Role::Candidate,
            "Non-candidate Server {} send vote_request",
            self.me
        );
        let args: RequestVoteArgs = RequestVoteArgs {
            term: {
                self.state.term += 1;
                self.state.term
            },
            candidate_id: self.me,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        };
        self.state.vote_for = Some(self.me); //vote for itself
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
        info!(
            "[Vote] Server {} start leader selection with term = {}",
            self.me, self.state.term
        );
        rpcs
    }
}

impl Raft {
    fn last_log_index(&self) -> u64 {
        self.logs.len() as u64 - 1
    }
    fn last_log_term(&self) -> u64 {
        self.logs.last().unwrap().term
    }
    fn update_next_index(&mut self, peer_id: usize, new_next_index: u64) {
        self.next_index[peer_id] = self.next_index[peer_id].max(new_next_index);
    }
    fn update_match_index(&mut self, peer_id: usize, new_match_index: u64) {
        self.match_index[peer_id] = self.match_index[peer_id].max(new_match_index);
    }
    fn is_more_up_to_date(&self, other_log_term: u64, other_log_index: u64) -> bool {
        use std::cmp::Ordering::{Equal, Greater, Less};
        match self.last_log_term().cmp(&other_log_term) {
            Greater => true,
            Equal => self.last_log_index() > other_log_index,
            Less => false,
        }
    }
    fn restore(&mut self, persist: Persist) {
        info!("[Restore] S{} restore", self.me);
        self.state.term = persist.term;
        self.state.vote_for = persist.vote_for;
        self.logs = persist.logs;
    }
    fn append_logs(&mut self, new_entries: Vec<LogEntry>, prev_index: u64) {
        let logs = &mut self.logs;
        let mut index = prev_index as usize + 1;
        for new_log in new_entries.into_iter() {
            match logs.get(index) {
                Some(log) => {
                    if log.term != new_log.term {
                        logs.truncate(index);
                        logs.push(new_log);
                    }
                }
                None => logs.push(new_log),
            }
            index += 1;
        }
    }
    /// Return first index of log whose term is the same as `term`
    fn get_first_index_of_term(&self, target_term: u64) -> Option<u64> {
        for (i, log) in self.logs.iter().enumerate() {
            if log.term == target_term {
                return Some(i as u64);
            }
        }
        None
    }
    /// Return new `next_index` when leader get a failed append reply
    fn get_next_index(&self, conflict_index: u64, follower_conflict_info: ConflictInfo) -> u64 {
        for i in (follower_conflict_info.first_index..conflict_index - 1).rev() {
            if self.logs[i as usize].term == follower_conflict_info.term.unwrap() {
                return i + 1;
            }
        }
        follower_conflict_info.first_index
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

impl Display for AppendEntriesArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AppendArgs[{} entries] {{ term: {}, leader_id: {}, prev_log_index: {}, prev_log_term: {}, leader_commit: {}}}",
    self.entries.len(), self.term, self.leader_id, self.prev_log_index, self.prev_log_term, self.leader_commit)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AppendEntriesReply {
    term: u64,
    success: bool,
    //.0 is term and .1 is index
    conflict: ConflictInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConflictInfo {
    term: Option<u64>, //None means there is no log entry in AppendEntriesArgs's prev_log_index
    first_index: u64,  //if term is None, first_index is last_log_index + 1
}
