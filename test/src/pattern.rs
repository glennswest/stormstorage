//! `/test pattern write|verify <dir> <seed>` — the workload the two-node
//! suite's claim pods run (#113), the test image itself, so a run pulls
//! nothing else.
//!
//! - `write`: `<dir>/pattern`, 32 MiB of bytes made from the seed, fsynced;
//!   then forever a line `tick N` appended to `<dir>/ticks` and fsynced
//!   every second, `tick N` on stdout: a claim that stops taking writes
//!   shows as ticks that stop.
//! - `verify`: the pattern read back and compared byte for byte, and the
//!   last tick; one JSON line, exit 0 when intact.

use std::io::{Read, Write};

pub const PATTERN_BYTES: usize = 32 << 20;
const CHUNK: usize = 1 << 20;

/// The `i`th chunk of the pattern for `seed` (xorshift64*, per chunk, so
/// a chunk can be checked alone).
fn chunk(seed: u64, i: u64, out: &mut [u8]) {
    let mut x = seed ^ (i.wrapping_add(1)).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    if x == 0 {
        x = 1;
    }
    for c in out.chunks_mut(8) {
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        let v = x.wrapping_mul(0x2545_F491_4F6C_DD1D).to_le_bytes();
        c.copy_from_slice(&v[..c.len()]);
    }
}

pub fn main(args: &[String]) -> i32 {
    let (Some(mode), Some(dir), Some(seed)) = (args.first(), args.get(1), args.get(2)) else {
        eprintln!("usage: test pattern write|verify <dir> <seed>");
        return 2;
    };
    let seed: u64 = seed.bytes().fold(0xcbf2_9ce4_8422_2325, |h, b| (h ^ u64::from(b)).wrapping_mul(0x100_0000_01b3));
    let dir = std::path::Path::new(dir);
    match mode.as_str() {
        "write" => match write(dir, seed) {
            Ok(()) => 0,
            Err(e) => {
                println!("{}", serde_json::json!({"pattern": "error", "error": e.to_string()}));
                1
            }
        },
        "verify" => verify(dir, seed),
        _ => 2,
    }
}

fn write(dir: &std::path::Path, seed: u64) -> std::io::Result<()> {
    let mut f = std::fs::File::create(dir.join("pattern"))?;
    let mut buf = vec![0u8; CHUNK];
    for i in 0..(PATTERN_BYTES / CHUNK) as u64 {
        chunk(seed, i, &mut buf);
        f.write_all(&buf)?;
    }
    f.sync_all()?;
    println!("{}", serde_json::json!({"pattern": "written", "bytes": PATTERN_BYTES}));
    let mut ticks = std::fs::OpenOptions::new().create(true).append(true).open(dir.join("ticks"))?;
    let mut n: u64 = 0;
    loop {
        n += 1;
        writeln!(ticks, "tick {n}")?;
        ticks.sync_data()?;
        println!("tick {n}");
        let _ = std::io::stdout().flush();
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

fn verify(dir: &std::path::Path, seed: u64) -> i32 {
    let last_tick = std::fs::read_to_string(dir.join("ticks"))
        .ok()
        .and_then(|t| t.lines().last().and_then(|l| l.strip_prefix("tick ")).and_then(|n| n.parse::<u64>().ok()));
    let mut f = match std::fs::File::open(dir.join("pattern")) {
        Ok(f) => f,
        Err(e) => {
            println!("{}", serde_json::json!({"pattern": "missing", "error": e.to_string()}));
            return 1;
        }
    };
    let (mut want, mut got) = (vec![0u8; CHUNK], vec![0u8; CHUNK]);
    for i in 0..(PATTERN_BYTES / CHUNK) as u64 {
        chunk(seed, i, &mut want);
        if let Err(e) = f.read_exact(&mut got) {
            println!("{}", serde_json::json!({"pattern": "short", "at": i as usize * CHUNK, "error": e.to_string()}));
            return 1;
        }
        if let Some(off) = want.iter().zip(&got).position(|(a, b)| a != b) {
            println!("{}", serde_json::json!({"pattern": "bad", "at": i as usize * CHUNK + off}));
            return 1;
        }
    }
    println!("{}", serde_json::json!({"pattern": "ok", "bytes": PATTERN_BYTES, "last_tick": last_tick}));
    0
}

#[cfg(test)]
mod tests {
    #[test]
    fn chunks_are_stable_and_differ() {
        let (mut a, mut b, mut c) = (vec![0u8; 64], vec![0u8; 64], vec![0u8; 64]);
        super::chunk(7, 0, &mut a);
        super::chunk(7, 0, &mut b);
        super::chunk(7, 1, &mut c);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn write_then_verify_round_trips() {
        let dir = std::env::temp_dir().join(format!("pattern-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        // The pattern alone (write loops forever on ticks).
        let mut f = std::fs::File::create(dir.join("pattern")).unwrap();
        let mut buf = vec![0u8; super::CHUNK];
        for i in 0..(super::PATTERN_BYTES / super::CHUNK) as u64 {
            super::chunk(42, i, &mut buf);
            std::io::Write::write_all(&mut f, &buf).unwrap();
        }
        drop(f);
        assert_eq!(super::verify(&dir, 42), 0);
        assert_eq!(super::verify(&dir, 43), 1, "another seed reads bad");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
