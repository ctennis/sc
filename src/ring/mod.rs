extern crate flate2;

use std::old_io::{BufferedReader, File};

//{"device": "d0", "id": 0, "ip": "10.149.30.7", "port": 6000, "region": 1, "replication_ip": "10.149.30.7", "replication_port": 6003, "weight": 3000.592982016, "zone": 3},

#[derive(Debug,RustcDecodable)]
pub struct DevData {
    device: String,
    id: u32,
    ip: String,
    port: u16,
    region: u16,
    replication_ip :String,
    replication_port: u16,
    weight: f32,
    zone: u16
}

#[derive(Debug,RustcDecodable)]
struct RingData {
    devs: Vec<Option<DevData>>,
    part_shift: u8,
    replica_count: u8,
}

#[derive(Debug)]
pub struct Ring {
    devs: Vec<DevData>,
    part_shift: u8,
    r2p2d_id: Vec<Vec<u16>>
}

impl Ring { 
    pub fn decode_ring_file(filename: &str) -> Ring 
    {
        let od = decode_ring_file(filename);
        match od {
            Some(rd) => { Ring::rd_to_ring(rd) }
            _ => { panic!("can't extract ring") }
        }
    }
    
    pub fn replica_count(self: &Ring) -> usize
    {
        self.r2p2d_id.len()
    }
    
    pub fn partition_count(self: &Ring) -> usize
    {
      self.r2p2d_id[0].len()
    }
    
    pub fn get_part_nodes(self: &Ring, part: u32) -> Vec<&DevData>
    {
        let mut part_nodes: Vec<&DevData> = Vec::new();
        for r2p2d in self.r2p2d_id.iter() {
            if (part as usize) < r2p2d.len() {
                let dev_id = r2p2d.get(part as usize);
                match dev_id {
                    Some(did) => {
                      let rd = self.dev_by_id(did.clone() as usize);
                      match rd {
                          Some(dev) => {
                              part_nodes.push(dev)
                          }
                          None => { }
                      }
                    }
                    None => { }
                }
                
            }
        }
        return part_nodes;
    }
    
    fn dev_by_id(self: &Ring, part: usize) -> Option<&DevData>
    {
        for i in self.devs.iter() {
            if i.id as usize == part {
                return Some(i);
            }
        }
        None
    }
    
    fn rd_to_ring(mut o: (RingData, Vec<Vec<u16>>)) -> Ring
    {   
        let mut dd: Vec<DevData> = Vec::new();
        for d in (o.0.devs).drain() {
           match d {
               Some(devdata) => { dd.push(devdata) }
               None => { }
           }
        }
        Ring { devs: dd, part_shift: o.0.part_shift, r2p2d_id: o.1 }
    }
}

fn decode_ring_file(filename: &str) -> Option<(RingData, Vec<Vec<u16>>)>
{
    let file = File::open(&Path::new(filename));
    let reader = BufferedReader::new(file);
    let mut d = flate2::reader::GzDecoder::new(reader);

    // First four bytes should be "RING"    
    match d.read_exact(4) {
        Ok(v) => {
            if v != [82u8, 49u8, 78u8, 71u8] {
                println!("Don't got it");
                return None;
            }
        }
        _ => {  }
    }
    
    // Next two bytes should be version 1
    match d.read_exact(2) {
        Ok(v) => {
            if v != [0u8, 1u8] {
                println!("Unknown version");
                return None;
            }
        }
        _ => { }
    }
    
    let json_size: u32 =
    match d.read_be_u32() {
        Ok(v) => {
            v
        }
        _ => { 0 }
    };
    
    let ret = 
    match d.read_exact(json_size as usize) {
        Ok(v) => {
            String::from_utf8(v)
        }
        _ => { String::from_utf8(Vec::new()) }
    };
    
    let ret2 =
    match ret {
        Ok(s) => { 
            match super::rustc_serialize::json::decode::<RingData>(&s) {
                Ok(s) => { Some(s) }
                _ => { None }
            }
        }   
        _ => { None }
    };

    let fv =
    match ret2 {
        Some(s) => { s }
        _ => { panic!("Unable to parse json"); }
    };
    
    let partition_count: u32 = 1 << (32 - fv.part_shift);

    let mut r2p2d: Vec<Vec<u16>> = Vec::new();
    
    for _ in (0u8..fv.replica_count) {
        let mut parts: Vec<u16> = Vec::new();
        for _ in (0u32..partition_count) {
            match d.read_le_u16() {
                Ok(b) => { parts.push(b); }
                _ => { }
            }
        }
        r2p2d.push(parts);
    }
    
    return Some((fv, r2p2d));

}

#[test]
fn test_replica_count()
{
 //   let r: Ring = Ring { devs: Vec::new(). part_shift: }
}

#[test]
fn test_partition_count()
{
    
}
