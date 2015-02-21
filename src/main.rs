#![feature(old_path)]
#![feature(old_io)]
#![feature(collections)]

extern crate "rustc-serialize" as rustc_serialize;

pub mod logparser;
pub mod ring;

fn main() {
    println!("Hello, world!");
    let r = ring::Ring::decode_ring_file("resources/object.ring.gz");
    println!("{:?}", r.get_part_nodes(10));
}

