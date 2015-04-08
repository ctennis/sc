use std::fs::File;
use std::path::PathBuf;
use flate2::read::GzDecoder;
use std::io::Read;

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
    let path = PathBuf::from(filename);
    let file = File::open(path).unwrap();
    let mut d = GzDecoder::new(file).unwrap();

    // First four bytes should be "RING"
    let mut ring_header: [u8; 4] = [0,0,0,0];
    match d.read(&mut ring_header) {
        Ok(_) => {
            if ring_header != [82u8, 49u8, 78u8, 71u8] {
                println!("Don't got it");
                return None;
            }
        }
        _ => {  }
    }
    
    // Next two bytes should be version 1
    let mut ring_version: [u8; 2] = [0,0];
    match d.read(&mut ring_version) {
        Ok(2) => {
            if ring_version != [0u8, 1u8] {
                println!("Unknown version");
                return None;
            }
        }
        _ => { }
    }
    
    let mut json_size_bytes: [u8; 4] = [0,0,0,0];

    let json_size = match d.read(&mut json_size_bytes) {
        Ok(4) => {
            json_size_bytes[3] as u32 +
            ((json_size_bytes[2] as u32) * 256) +
            ((json_size_bytes[1] as u32) * 65536) +
            ((json_size_bytes[0] as u32) * 16777216);
        }
        _ => { 0 as u32; }
    };
    
    // TODO: compare to json_size..
    let mut data_as_string: String = String::new();
    
    let ret2 = match d.read_to_string(&mut data_as_string) {
        Ok(json_size) => {
            match super::rustc_serialize::json::decode::<RingData>(&mut data_as_string) {
                Ok(s) => { Some(s) }
                _ => { None }
            }
        }
        _ => { panic!("AHHH!") }
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
            let mut le16: [u8; 2] = [0,0];
            match d.read(&mut le16) {
                Ok(2) => { 
                    let pval: u16 = ((le16[0] as u16) * 256) + (le16[1] as u16);
                    parts.push(pval); }
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
