//! Event ring: what happened to which node/volume, when.

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::time::SystemTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub seq: u64,
    pub time: SystemTime,
    /// Node or volume name the event is about.
    pub subject: Option<String>,
    pub severity: Severity,
    /// node, volume, placement, register, poll.
    pub kind: String,
    pub message: String,
}

#[derive(Debug)]
pub struct EventLog {
    next_seq: u64,
    ring: VecDeque<Event>,
    cap: usize,
}

impl EventLog {
    pub fn new(cap: usize) -> Self {
        Self {
            next_seq: 1,
            ring: VecDeque::with_capacity(cap.min(1024)),
            cap,
        }
    }

    pub fn push(
        &mut self,
        subject: Option<String>,
        severity: Severity,
        kind: &str,
        message: String,
    ) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        if self.ring.len() == self.cap {
            self.ring.pop_front();
        }
        self.ring.push_back(Event {
            seq,
            time: SystemTime::now(),
            subject,
            severity,
            kind: kind.to_string(),
            message,
        });
        seq
    }

    pub fn since(&self, since: u64) -> Vec<Event> {
        self.ring.iter().filter(|e| e.seq > since).cloned().collect()
    }

    pub fn latest_seq(&self) -> u64 {
        self.next_seq - 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_caps_and_since_filters() {
        let mut log = EventLog::new(2);
        for i in 0..4 {
            log.push(None, Severity::Info, "t", format!("e{i}"));
        }
        assert_eq!(log.latest_seq(), 4);
        assert_eq!(log.since(0).len(), 2);
        assert_eq!(log.since(3).len(), 1);
    }
}
