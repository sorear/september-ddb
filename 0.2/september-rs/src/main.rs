#![allow(dead_code)]
#![feature(plugin)]
#![feature(custom_derive)]
#![plugin(serde_macros)]
extern crate lmdb;
extern crate uuid;
extern crate capnp;
extern crate byteorder;
extern crate hyper;
extern crate serde_json;
extern crate serde;
extern crate time;

use hyper::Server;
use hyper::server::{Response, Request};
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::collections::{HashMap, HashSet};
use std::cmp;
use std::thread;
use lmdb::{Transaction as LmdbTransaction, Cursor as LmdbCursor};

struct OneShotEvent<E> {
    mutex: Mutex<Option<E>>,
    condvar: Condvar,
}

impl<E> OneShotEvent<E> {
    fn new() -> Self {
        OneShotEvent {
            mutex: Mutex::new(None),
            condvar: Condvar::new()
        }
    }

    fn put(&self, val: E) {
        let mut mut_ref = self.mutex.lock().unwrap();
        if mut_ref.is_none() {
            *mut_ref = Some(val);
            self.condvar.notify_all();
        }
    }

    fn await(&self) -> E where E: Clone {
        let mut mut_ref = self.mutex.lock().unwrap();
        while mut_ref.is_none() {
            mut_ref = self.condvar.wait(mut_ref).unwrap();
        }
        mut_ref.as_ref().unwrap().clone()
    }
}

#[derive(Debug)]
enum Error {
    LmdbError(lmdb::Error),
    HyperError(hyper::Error),
    JsonError(serde_json::Error),
    UnexpectedReply(GResponse),
    MessageOutOfOrder,
    DatabaseAlreadyCreated,
}

impl From<lmdb::Error> for Error {
    fn from(e: lmdb::Error) -> Error {
        Error::LmdbError(e)
    }
}

impl From<hyper::Error> for Error {
    fn from(e: hyper::Error) -> Error {
        Error::HyperError(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Error {
        Error::JsonError(e)
    }
}

type Result<T> = std::result::Result<T, Error>;
struct Id(Vec<i32>);

// we'll want other types of operation in the future
struct Operation {
    transaction: Id,
    id: Id,
    dependencies: Vec<Id>,
    object: Id,
    field: Id,
    value: Vec<u8>,
}

#[derive(Serialize,Deserialize)]
enum DbKey {
    ReceivedRequest(SUuid),
    LastMessageId(String),
    LastReceivedId(String),
    QueuedMessages,
    QueuedMessage(String, u64),
    TopState(String),
}

impl DbKey {
    fn uplevel(&self) -> Option<DbKey> {
        use DbKey::*;
        match *self {
            QueuedMessage(_, _) => Some(QueuedMessages),
            _ => None,
        }
    }

    fn to_str(&self) -> String {
        let mut buf = String::new();
        if let Some(up) = self.uplevel() {
            buf = up.to_str();
            buf.push('\t');
        }
        buf + &serde_json::to_string(self).unwrap()
    }
}

struct DbState {
    flush_running: HashSet<String>,
    reply_ports: HashMap<SUuid, Arc<OneShotEvent<GResponse>>>,
}

struct DbParts {
    env: lmdb::Environment,
    db: lmdb::Database,
    st: DbState,
}

#[derive(Clone)]
struct Database {
    data: Arc<Mutex<DbParts>>,
    name: String,
}

struct Transaction<'db : 'tx,'tx> {
    top: &'db Database,
    env: &'db lmdb::Environment,
    tx: &'tx mut lmdb::RwTransaction<'db>,
    db: lmdb::Database,
    st: &'db mut DbState,
}

impl Database {
    fn run(name: &str) -> Result<hyper::server::Listening> {
        let env = try!(lmdb::Environment::new().set_flags(lmdb::NO_SUB_DIR).open(&PathBuf::from(name)));
        let ldb = try!(env.open_db(None));
        let parts = DbParts {
            env: env,
            st: DbState { flush_running: HashSet::new(), reply_ports: HashMap::new() },
            db: ldb,
        };
        let db = Database { data: Arc::new(Mutex::new(parts)), name: name.to_owned() };
        let mut need_flush = HashSet::new();
        try!(db.transaction(|tx| {
            let mut ist = tx.iter_key(DbKey::QueuedMessages);
            while let Some(DbKey::QueuedMessage(peer, _)) = try!(tx.get_next_key(&mut ist)) {
                need_flush.insert(peer);
            }
            Ok(())
        }));
        for peer in need_flush {
            db.flush_later(peer);
        }
        let server = try!(Server::http(&*name));
        return Ok(try!(server.handle(db)));
    }

    fn transaction<F,R>(&self, cb: F) -> Result<R> where F: FnOnce(&mut Transaction) -> Result<R> {
        let mut guard = self.data.lock().unwrap();
        let mut state = &mut *guard;
        let mut trx = try!(state.env.begin_rw_txn());
        // TODO rollback changes to state.st if the callback fails
        let res = try!(cb(&mut Transaction {
            top: self,
            db: state.db,
            st: &mut state.st,
            env: &state.env,
            tx: &mut trx,
        }));
        try!(trx.commit());
        Ok(res)
    }

    fn now(&self) -> TAI64N {
        let ts = time::get_time();
        // TODO: ΔT, monotonicity
        TAI64N { sec: ts.sec, nsec: ts.nsec }
    }

    fn handle_request(&self, req: &GRequest) -> Result<GResponse> {
        match *req {
            GRequest::Command { ref body, ref id } => {
                let port = try!(self.transaction(|tx| {
                    // do we already have a waitqueue for this id?
                    if let Some(port_ref) = tx.st.reply_ports.get(id) {
                        return Ok(port_ref.clone());
                    }
                    let port = Arc::new(OneShotEvent::new());
                    // do we already have a reply on file for this id?
                    if let Some(rpy_ref) = try!(tx.get_val(DbKey::ReceivedRequest(id.clone()))) {
                        match rpy_ref {
                            Some(reply) => {
                                port.put(reply);
                            }
                            None => {
                                // we seem to have rebooted, things are in flight for this req
                                tx.st.reply_ports.insert(*id, port.clone());
                            }
                        }
                    } else {
                        try!(tx.set_val(DbKey::ReceivedRequest(*id), &None::<GResponse>));
                        tx.st.reply_ports.insert(*id, port.clone());
                        // need to srart the command
                        try!(tx.start_command(*id, body));
                    }
                    return Ok(port);
                }));

                return Ok(port.await());
            },
            GRequest::Message { ref from, ref body, index, .. } => {
                self.transaction(|tx| {
                    let last_msg_no = try!(tx.get_val(DbKey::LastReceivedId(from.clone()))).unwrap_or(0u64);
                    if index <= last_msg_no {
                        // replay
                        return Ok(GResponse::Ok);
                    }
                    if index != last_msg_no + 1 {
                        return Err(Error::MessageOutOfOrder);
                    }
                    try!(tx.set_val(DbKey::LastReceivedId(from.clone()), &index));
                    try!(tx.handle_message(from, body));
                    Ok(GResponse::Ok)
                })
            },
            GRequest::AckReply { ref id } => {
                self.transaction(|tx| {
                    if !tx.st.reply_ports.contains_key(id) {
                        try!(tx.del_val(DbKey::ReceivedRequest(*id)));
                    }
                    return Ok(GResponse::Ok);
                })
            },
            GRequest::GetTime => {
                Ok(GResponse::Time(self.now()))
            },
        }
    }

    fn flush_later(&self, peer: String) {
        let sref = self.clone();
        thread::spawn(move || {
            let added = sref.transaction(|tx| {
                Ok::<bool,Error>(tx.st.flush_running.insert(peer.clone()))
            }).unwrap();

            if !added {
                // there was already a flusher running.
                // it cannot exit until it takes the lock AND sees an empty queue, so our message
                // will go out.
                return;
            }

            let mut just_sent = None::<u64>;
            let client = hyper::client::Client::new();
            loop {
                let opt_next = sref.transaction(|tx| {
                    if let Some(msg_no) = just_sent.take() {
                        try!(tx.del_val(DbKey::QueuedMessage(peer.clone(), msg_no)));
                    }
                    // find the next message ID.  If we don't find it, deregister ourself.
                    let mut ist = tx.iter_key(DbKey::QueuedMessages);
                    let mut found = 0;
                    while let Some(DbKey::QueuedMessage(peer2, ix2)) = try!(tx.get_next_key(&mut ist)) {
                        if peer2 == peer {
                            found = cmp::max(ix2, found);
                        }
                    }
                    if found == 0 {
                        tx.st.flush_running.remove(&peer);
                        Ok(None)
                    } else {
                        let body = try!(tx.get_val::<GRequest>(DbKey::QueuedMessage(peer.clone(), found))).unwrap();
                        Ok(Some((found, body)))
                    }
                }).unwrap();
                if let Some ((next_id, next_body)) = opt_next {
                    // dispatch it using hyper
                    match dispatch(&client, &peer, next_body) {
                        Ok(_) => {
                            just_sent = Some(next_id);
                        }
                        Err(e) => {
                            println!("dispatch {}: {:?}", peer, e);
                            just_sent = None;
                        }
                    }
                } else {
                    break;
                }
            }
        });
    }
}

fn dispatch(client: &hyper::client::Client, peer: &str, message: GRequest) -> Result<()> {
    let bytes = try!(serde_json::to_vec(&message));
    let resp = try!(client.post(&format!("http://{}", peer)).header(hyper::header::ContentType::json()).body(&*bytes).send());
    let rpy = try!(serde_json::from_reader(resp));
    match rpy {
        GResponse::Ok => Ok(()),
        unex => Err(Error::UnexpectedReply(unex)),
    }
}

#[derive(Clone,Serialize,Deserialize)]
enum CMessage {
    ExampleMessage,
}

struct IterState {
    prefix: Vec<u8>,
    after: Option<Vec<u8>>,
}

impl<'db,'tx> Transaction<'db,'tx> {
    fn get_opt_raw<K: AsRef<[u8]>>(&self, key: &K) -> lmdb::Result<Option<&[u8]>> {
        match self.tx.get(self.db, key) {
            Ok(val) => Ok(Some(val)),
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(err) => Err(err),
        }
    }

    fn get_val<T>(&self, key: DbKey) -> Result<Option<T>> where T: serde::Deserialize {
        let raw = try!(self.get_opt_raw(&key.to_str()));
        match raw {
            None => Ok(None),
            Some(raw2) => Ok(Some(try!(serde_json::from_slice(&raw2)))),
        }
    }

    fn set_val<T>(&mut self, key: DbKey, val: &T) -> Result<()> where T: serde::Serialize {
        let raw = try!(serde_json::to_vec(&val));
        try!(self.tx.put(self.db, &key.to_str(), &raw, lmdb::WriteFlags::empty()));
        Ok(())
    }

    fn del_raw<K: ?Sized + AsRef<[u8]>>(&mut self, key: &K) -> lmdb::Result<bool> {
        match self.tx.del(self.db, &key, None) {
            Ok(_) => Ok(true),
            Err(lmdb::Error::NotFound) => Ok(false),
            Err(err) => Err(err),
        }
    }

    fn del_val(&mut self, key: DbKey) -> Result<bool> {
        return Ok(try!(self.del_raw(&key.to_str())));
    }

    fn get_next_raw(&self, state: &mut IterState) -> lmdb::Result<Option<(&[u8], &[u8])>> {
        let mut csr = try!(self.tx.open_ro_cursor(self.db));
        let val = match state.after {
            None => {
                csr.iter_from(&state.prefix).nth(0)
            }
            Some(ref after_val) => {
                csr.iter_from(&after_val).nth(1)
            }
        };
        if let Some((k, v)) = val {
            if !k.starts_with(&state.prefix) { return Ok(None); }
            state.after = Some(k.to_owned());
            return Ok(Some((k, v)));
        }
        Ok(None)
    }

    fn iter_key(&self, prefix: DbKey) -> IterState {
        let mut pfx = prefix.to_str().into_bytes();
        pfx.push(b'\t');
        IterState {
            prefix: pfx,
            after: None
        }
    }

    fn get_next_key(&self, state: &mut IterState) -> Result<Option<DbKey>> {
        if let Some((mut rkey, _)) = try!(self.get_next_raw(state)) {
            if let Some(ix) = rkey.iter().rposition(|b| *b == b'\t') {
                rkey = &rkey[ix + 1 ..];
            }
            if let Ok(key) = serde_json::from_slice(rkey) {
                return Ok(Some(key));
            }
        }
        return Ok(None);
    }

    fn now(&self) -> TAI64N {
        let ts = time::get_time();
        // TODO: ΔT, monotonicity
        TAI64N { sec: ts.sec, nsec: ts.nsec }
    }

    fn send_to(&mut self, peer: &str, message: &CMessage) -> Result<()> {
        // get the next ID for the peer
        let last_msg_no = try!(self.get_val(DbKey::LastMessageId(peer.to_owned()))).unwrap_or(0u64);
        let msg_no = last_msg_no + 1;

        // save the message and the updated next ID
        let req = GRequest::Message { from: self.top.name.clone(), body: message.clone(), stamp: self.now(), index: msg_no };
        try!(self.set_val(DbKey::LastMessageId(peer.to_owned()), &msg_no));
        try!(self.set_val(DbKey::QueuedMessage(peer.to_owned(), msg_no), &req));

        // call flush_later (will wait until commit, and noop if we abort)
        self.top.flush_later(peer.to_owned());
        Ok(())
    }

    fn command_reply(&mut self, id: SUuid, reply: &GResponse) -> Result<()> {
        // only allow reply to be set once
        match try!(self.get_val::<Option<GResponse>>(DbKey::ReceivedRequest(id))) {
            None => {}, // very stale?
            Some(Some(_)) => {}, // already set
            Some(None) => {
                try!(self.set_val(DbKey::ReceivedRequest(id), &Some(reply.clone())));

                let tref = self.top.clone();
                thread::spawn(move || {
                    tref.transaction(|tx| {
                        // we can't get here until the tx with the command reply has succeeded
                        // or failed.  check which it was.
                        match try!(tx.get_val(DbKey::ReceivedRequest(id))) {
                            Some(Some(reply)) => {
                                // oo, we have a reply
                                if let Some(port) = tx.st.reply_ports.get(&id) {
                                    port.put(reply);
                                }
                            },
                            _ => {},
                        }
                        Ok(())
                    }).unwrap();
                });
            }
        }
        return Ok(())
    }

    fn handle_message(&mut self, _peer: &String, _message: &CMessage) -> Result<()> {
        Ok(())
    }

    fn start_command(&mut self, id: SUuid, cmd: &Command) -> Result<()> {
        match *cmd {
            Command::CreateDatabase { ref name } => {
                let tstate = try!(self.get_val::<TopState>(DbKey::TopState(name.clone())));
                if tstate.is_some() {
                    return Err(Error::DatabaseAlreadyCreated);
                }

                try!(self.set_val(DbKey::TopState(name.clone()), &TopState::Root {
                    uuid: SUuid(uuid::Uuid::new_v4()),
                    reverse_siblings: vec![],
                    siblings: vec![],
                }));
                try!(self.command_reply(id, &GResponse::Ok));
            },
        }
        Ok(())
    }
}

#[derive(Clone,Copy,Debug,Serialize,Deserialize)]
struct TAI64N {
    sec: i64,
    nsec: i32,
}

#[derive(Debug,Copy,Clone,Eq,PartialEq,Hash)]
struct SUuid(pub uuid::Uuid);

impl serde::Serialize for SUuid {
    fn serialize<S: serde::Serializer>(&self, to: &mut S) -> std::result::Result<(), S::Error> {
        self.0.to_hyphenated_string().serialize(to)
    }
}

impl serde::Deserialize for SUuid {
    fn deserialize<D: serde::Deserializer>(from: &mut D) -> std::result::Result<Self, D::Error> {
        let strg = try!(String::deserialize(from));
        match uuid::Uuid::parse_str(&strg) {
            Ok(x) => Ok(SUuid(x)),
            Err(s) => Err(serde::Error::custom(format!("{}", s))),
        }
    }
}

#[derive(Clone,Debug,Serialize,Deserialize)]
enum TopState {
    Root {
        uuid: SUuid,
        siblings: Vec<String>,
        reverse_siblings: Vec<String>,
    },
    NonRoot {
        uuid: SUuid,
        parent: String,
    }
}

#[derive(Clone,Serialize,Deserialize)]
enum GRequest {
    Message {
        from: String,
        body: CMessage,
        stamp: TAI64N,
        index: u64,
    },
    Command {
        id: SUuid,
        body: Command,
    },
    AckReply {
        id: SUuid,
    },
    GetTime,
}

#[derive(Clone,Serialize,Deserialize)]
enum Command {
    CreateDatabase {
        name: String,
    },
}

#[derive(Clone,Debug,Serialize,Deserialize)]
enum GResponse {
    Ok,
    Time(TAI64N),
    Error(String),
}

impl hyper::server::Handler for Database {
    fn handle<'a, 'k>(&'a self, request: Request<'a, 'k>, mut response: Response<'a, hyper::net::Fresh>) {
        let reqj = serde_json::from_reader(request).unwrap();
        let rpyj = match self.handle_request(&reqj) {
            Ok(r) => r,
            Err(err) => GResponse::Error(format!("{:?}", err)),
        };
        response.headers_mut().set(hyper::header::ContentType::json());
        let mut response = response.start().unwrap();
        serde_json::to_writer(&mut response, &rpyj).unwrap();
        println!("CALL: {}: {} => {}", self.name, serde_json::to_string_pretty(&reqj).unwrap(), serde_json::to_string_pretty(&rpyj).unwrap());
    }
}

fn main() {
    let mut guards = Vec::new();
    for host in std::env::args().skip(1) {
        guards.push(Database::run(&*host).unwrap());
    }
}
