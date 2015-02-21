use std::old_io::{BufferedReader, File};
use std::fmt;
use std::collections::HashMap;
use std::old_path::Path;
use std::str::FromStr;

#[derive(Debug,Clone)]
enum ServerType {
    Proxy,
    Object,
    Account,
    Container,
    Unknown
}

#[derive(Debug,Clone)]
struct CommonLogEntry {
    date: String,
    time: String,
    hostname: String,
    server_type: ServerType
}

#[derive(Debug,Clone)]
struct LogEntry(CommonLogEntry, String);

#[derive(Debug,Clone)]
struct RequestEntry {
    common: CommonLogEntry,
    url: String,
    verb: String,
    status_code: u8,
}

#[derive(Debug,Clone)]
struct ErrorEntry {
    common: CommonLogEntry,
    error: String
}

#[derive(Debug,Clone)]
struct HandoffEntry {
    common: CommonLogEntry,
}

#[derive(Debug,Clone)]
pub struct Transaction {
    txid: String,
    entries: Vec<RequestEntry>,
    errors: Vec<ErrorEntry>,
    handoffs: u8
}

pub struct ProxyReq;

trait HTTPReq {
    fn status_code(&self) -> u8;
}

impl HTTPReq for ProxyReq {
    fn status_code(&self) -> u8 {
        return 0;
    }
}

impl fmt::Display for Transaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}\n", self.txid.as_slice()) 
    }
}

struct LogLine {
    line: String
}

impl LogLine {
    fn new(x: &str) -> LogLine {
        LogLine { line: x.to_string() }
    }
    
    fn to_log_entry(&self) -> LogEntry {
        let ss: Vec<String> = self.line.split(' ').map(|s| s.to_string()).collect();
        let time = ss[2].clone();
        let hn = ss[3].clone();
        let sts = ss[4].clone();
        let st: ServerType = match &sts[..] {
            "proxy-server:" => ServerType::Proxy,
            "object-server:" => ServerType::Object,
            "account-server:" => ServerType::Account,
            "container-server:" => ServerType::Container,
            _ => ServerType::Unknown
        };
        //let rest_of_log_entry = self.line.slice_from(16 + 2 + hn.len() + sts.len());
        let rest_of_log_entry = ss[5..].connect(" ");
        let cle = CommonLogEntry{date: format!("{} {}", ss[0],ss[1]), time: time, hostname: hn, server_type: st};
        LogEntry(cle, rest_of_log_entry.to_string())
    }
}

trait LogParse {
    fn parse(&LogEntry) -> Option<(String, Self)>;
}

impl LogParse for ErrorEntry {
    fn parse(e: &LogEntry) -> Option<(String, ErrorEntry)> {
        let cle = &e.0;
        let s = &e.1;

        if s.starts_with("ERROR") {
            match s.find_str("txn: tx") {
                Some(i) => {
                    let txid = s[i+5..i+5+34].to_string();
                    let ee = ErrorEntry { common: cle.clone(), error: s.clone() }; 
                    return Some((txid,ee));
                    //return None;
                }
                _ => { 
                    return None; 
                }
            }
        }
        return None;        
    }
}

//Oct 16 08:54:00 localhost proxy-server: 127.0.0.1 127.0.0.1 16/Oct/2014/07/54/00 GET /auth/v1.0 HTTP/1.0 200 - csharp-cloudfiles - - - - tx61dbd83e664f4d4e92093-00543f7998 - 0.1305 - - 1413446040.225033998 1413446040.355536938

impl LogParse for RequestEntry {
    fn parse(e: &LogEntry) -> Option<(String, RequestEntry)> {
        let cle = &e.0;
        let s = &e.1;
       
        let ss: Vec<String> = s.split(' ').map(|s| s.to_string()).collect();
        if ss.len() != 20 {
            return None;
        }
        
        let url = ss[4].to_string();
        let verb = ss[3].to_string();
        
        let scode = match FromStr::from_str(&ss[6]) {
            Ok(code) => { code },
            Err(_) => { 0 }
        };
        
        let txid = ss[13].to_string();
        let r = RequestEntry { common: cle.clone(), url: url, verb: verb, status_code: scode };
        return Some((txid, r));
    }
}

impl LogParse for HandoffEntry {
    fn parse(e: &LogEntry) -> Option<(String, HandoffEntry)> {
        let cle = &e.0;
        let s = &e.1;

        if s.starts_with("Handoff") {
            match s.find_str("txn: tx") {
                Some(i) => {
                    let txid = s[i+5..i+5+34].to_string();
                    let he = HandoffEntry { common: cle.clone() }; 
                    return Some((txid,he));
                }
                _ => { 
                    return None; 
                }
            }
        }
        return None;        
    }
}

fn parse_file(filename: &str) -> Vec<String> {
    let path = Path::new(filename);
    let open_file = File::open(&path);
    let mut xs: Vec<String> = Vec::new();
    match open_file {
        Ok(_) => {
            let mut file = BufferedReader::new(open_file);
            for line in file.lines() {
                match line {
                    Ok(x) => {
                        xs.push(x)
                    }
                    _ => { }
                }
            }
        }
        Err(_) => {
          println!("Can't open file {}", filename);
        }
    }
    xs
}


fn parse_transactions<T: LogParse>(e: &mut Vec<LogLine>) -> HashMap<String, Vec<T>>
{
    let mut lines_to_return: Vec<LogLine> = Vec::new();
    let mut tmap: HashMap<String, Vec<T>> = HashMap::new();
   
    for entry in e.drain() {
        let le = entry.to_log_entry();
        match LogParse::parse(&le) {
            Some((txid,ee)) => {
                if !tmap.contains_key(&txid) {
                    tmap.insert(txid.clone(), Vec::new());
                }
                tmap[txid].push(ee);
            }
            None => {
                lines_to_return.push(entry);
            }
        }
        
        // parse entry above
 
    
 //   println!("{}", tmap);
    }

    for entry in lines_to_return.into_iter() {
        e.push(entry)
    }
    
    return tmap;
}

pub fn parse_it(filename: &str) -> Vec<Transaction> {

    let lines = parse_file(filename);

    let mut entries: Vec<LogLine> = Vec::new();

    for line in lines.iter() {
        let ll: LogLine = LogLine::new(&line);
        entries.push(ll);
    }

    let temap: HashMap<String, Vec<ErrorEntry>> = parse_transactions(&mut entries);
    let hmap: HashMap<String, Vec<HandoffEntry>> = parse_transactions(&mut entries);
    let rmap: HashMap<String, Vec<RequestEntry>> = parse_transactions(&mut entries);

    //let empty_r_vec: Vec<RequestEntry> = Vec::new();
    //let empty_e_vec: Vec<ErrorEntry> = Vec::new();

    let mut auth_trans: Vec<Transaction> = Vec::new();
    let mut trans: Vec<Transaction> = Vec::new();

    for tx in rmap.keys() {
        let r_entries: Vec<RequestEntry> = match rmap.get(tx) {
            Some(e) => e.clone(),
            None => Vec::new()
        };
        let e_entries: Vec<ErrorEntry> = match temap.get(tx) {
            Some(e) => e.clone(),
            None => Vec::new()           
        };
        let handoffs: u8 = match hmap.get(tx) {
            Some(h) => h.len() as u8,
            None => 0
        };
    
        // Should we segregate auth entries to its own vector
        if r_entries.len() == 1 {
            match r_entries.clone().pop() {
                Some(entry) => {
                    if entry.url == "/auth/v1.0" {
                        let t = Transaction { txid: tx.clone(), errors: e_entries, entries: r_entries, handoffs: handoffs };
                        auth_trans.push(t)
                    }
                }
                _ => { }
            }
        }
        else {
            let t = Transaction { txid: tx.clone(), errors: e_entries, entries: r_entries, handoffs: handoffs };
            trans.push(t);
        }
    }
    
    return trans;
}
