//! Engines that refuse stormstorage's token (#38).
//!
//! A 401/403 from a stormblock is a configuration error, not an outage: the
//! next poll with the same token gets the same answer, and every refused
//! call is a WARN line on that node's console. So per engine URL this backs
//! off (twice the poll interval, doubling, at most [`MAX_BACKOFF`]), reports
//! the first refusal and then at most one summary every [`REPORT_EVERY`].
//! A different token (the file was re-minted, the env changed) is worth a
//! call at once.

use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub const MAX_BACKOFF: Duration = Duration::from_secs(300);
pub const REPORT_EVERY: Duration = Duration::from_secs(300);

#[derive(Default)]
pub struct Refusals {
    engines: Mutex<BTreeMap<String, Refusal>>,
}

struct Refusal {
    token: u64,
    count: u32,
    retry_at: Instant,
    reported_at: Instant,
    /// Refusals since the last report.
    unreported: u32,
}

/// What to say about a refusal just recorded.
#[derive(Debug, PartialEq, Eq)]
pub enum Report {
    /// The first refusal of this engine (with this token).
    First,
    /// Still refused: this many calls since the last report.
    Still(u32),
    /// Reported recently; say nothing.
    Quiet,
}

fn fingerprint(token: Option<&str>) -> u64 {
    let mut h = DefaultHasher::new();
    token.unwrap_or("").hash(&mut h);
    h.finish()
}

fn key(url: &str) -> String {
    url.trim_end_matches('/').to_string()
}

impl Refusals {
    /// May stormstorage call this engine now with this token?
    pub fn may_call(&self, url: &str, token: Option<&str>, now: Instant) -> bool {
        let engines = self.engines.lock().unwrap();
        match engines.get(&key(url)) {
            None => true,
            Some(r) => r.token != fingerprint(token) || now >= r.retry_at,
        }
    }

    /// When this engine is next called, if it is backing off.
    pub fn retry_in(&self, url: &str, now: Instant) -> Option<Duration> {
        let engines = self.engines.lock().unwrap();
        engines
            .get(&key(url))
            .map(|r| r.retry_at.saturating_duration_since(now))
    }

    /// The engine refused `token`. `interval` is the poll interval.
    pub fn refused(&self, url: &str, token: Option<&str>, interval: Duration, now: Instant) -> Report {
        let fp = fingerprint(token);
        let mut engines = self.engines.lock().unwrap();
        let r = engines.entry(key(url)).or_insert(Refusal {
            token: fp,
            count: 0,
            retry_at: now,
            reported_at: now,
            unreported: 0,
        });
        let first = r.count == 0 || r.token != fp;
        if first {
            r.token = fp;
            r.count = 0;
            r.unreported = 0;
        }
        r.count += 1;
        let backoff = interval
            .max(Duration::from_secs(1))
            .saturating_mul(1u32 << r.count.min(16))
            .min(MAX_BACKOFF);
        r.retry_at = now + backoff;
        if first {
            r.reported_at = now;
            return Report::First;
        }
        r.unreported += 1;
        if now.duration_since(r.reported_at) >= REPORT_EVERY {
            r.reported_at = now;
            let n = r.unreported;
            r.unreported = 0;
            Report::Still(n)
        } else {
            Report::Quiet
        }
    }

    /// The engine answered. True when it had been refusing.
    pub fn accepted(&self, url: &str) -> bool {
        self.engines.lock().unwrap().remove(&key(url)).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const URL: &str = "http://127.0.0.1:9090";
    const POLL: Duration = Duration::from_secs(15);

    #[test]
    fn backs_off_doubling_to_the_cap() {
        let r = Refusals::default();
        let t0 = Instant::now();
        assert!(r.may_call(URL, None, t0));
        assert_eq!(r.refused(URL, None, POLL, t0), Report::First);
        assert!(!r.may_call(URL, None, t0 + Duration::from_secs(29)));
        assert!(r.may_call(URL, None, t0 + Duration::from_secs(30)));
        let mut now = t0;
        let mut waits = Vec::new();
        for _ in 0..6 {
            now += r.retry_in(URL, now).unwrap();
            r.refused(URL, None, POLL, now);
            waits.push(r.retry_in(URL, now).unwrap().as_secs());
        }
        assert_eq!(waits, vec![60, 120, 240, 300, 300, 300]);
    }

    #[test]
    fn reports_first_then_at_most_every_five_minutes() {
        let r = Refusals::default();
        let t0 = Instant::now();
        assert_eq!(r.refused(URL, Some("a"), POLL, t0), Report::First);
        assert_eq!(r.refused(URL, Some("a"), POLL, t0 + Duration::from_secs(30)), Report::Quiet);
        assert_eq!(r.refused(URL, Some("a"), POLL, t0 + Duration::from_secs(90)), Report::Quiet);
        assert_eq!(r.refused(URL, Some("a"), POLL, t0 + REPORT_EVERY), Report::Still(3));
        assert_eq!(
            r.refused(URL, Some("a"), POLL, t0 + REPORT_EVERY + Duration::from_secs(1)),
            Report::Quiet
        );
    }

    #[test]
    fn a_new_token_is_tried_at_once() {
        let r = Refusals::default();
        let t0 = Instant::now();
        r.refused(URL, None, POLL, t0);
        assert!(!r.may_call(URL, None, t0 + Duration::from_secs(1)));
        assert!(r.may_call(URL, Some("minted"), t0 + Duration::from_secs(1)));
        // A refusal of the new token is a new first refusal.
        assert_eq!(
            r.refused(URL, Some("minted"), POLL, t0 + Duration::from_secs(1)),
            Report::First
        );
    }

    #[test]
    fn accepted_clears_and_says_so_once() {
        let r = Refusals::default();
        let t0 = Instant::now();
        assert!(!r.accepted(URL));
        r.refused(&format!("{URL}/"), None, POLL, t0);
        assert!(r.accepted(URL));
        assert!(!r.accepted(URL));
        assert!(r.may_call(URL, None, t0));
    }
}
