#![feature(collections)]
#![feature(core)]

extern crate rustc_serialize;
extern crate flate2;
extern crate core;

pub mod logparser;
pub mod ring;

fn main() {
    println!("Hello, world!");
    let r = ring::Ring::decode_ring_file("resources/object.ring.gz");
    println!("{:?}", r.get_part_nodes(10));
}

