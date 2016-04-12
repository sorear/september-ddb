extern crate capnpc;

fn main() {
    ::capnpc::compile("src", &["src/unbase.capnp"]).unwrap();
}
