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

use hyper::Server;
use hyper::server::{Request, Response};
use std::path::PathBuf;
use std::sync::Arc;
use std::io::Write;

struct Transaction<'db> {
    tx: lmdb::RwTransaction<'db>,
    db: lmdb::Database,
}

#[derive(Debug)]
enum Error {
    LmdbError(lmdb::Error),
    HyperError(hyper::Error),
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

impl<'db> Transaction<'db> {

}

struct Database {
    env: Arc<lmdb::Environment>,
}

impl Database {
    fn run(name: &str) -> Result<hyper::server::Listening> {
        let env = try!(lmdb::Environment::new().set_flags(lmdb::NO_SUB_DIR).open(&PathBuf::from(name)));
        let db = Database { env: Arc::new(env) };
        let server = try!(Server::http(&*name));
        return Ok(try!(server.handle(db)));
    }

    fn handle2(&self, req: GRequest) -> GResponse {
        match req {
            GRequest::HelloRequest => GResponse::HelloReply,
        }
    }
}

#[derive(Serialize,Deserialize)]
enum GRequest {
    HelloRequest,
}

#[derive(Serialize,Deserialize)]
enum GResponse {
    HelloReply,
}

impl hyper::server::Handler for Database {
    fn handle<'a, 'k>(&'a self, request: Request<'a, 'k>, mut response: Response<'a, hyper::net::Fresh>) {
        let reqj = serde_json::from_reader(request).unwrap();
        let rpyj = self.handle2(reqj);
        response.headers_mut().set(hyper::header::ContentType::json());
        let mut response = response.start().unwrap();
        serde_json::to_writer_pretty(&mut response, &rpyj).unwrap();
        response.write(&[10u8]).unwrap();
    }
}

fn main() {
    let mut guards = Vec::new();
    for host in std::env::args().skip(1) {
        guards.push(Database::run(&*host).unwrap());
    }
}
