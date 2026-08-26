//! Placement: choose nodes for a volume's legs — spread across failure
//! domains at a rung, load-balanced within each domain. Pure functions,
//! deterministic given equal inputs.

use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize)]
pub struct Candidate {
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub free_bytes: u64,
    pub total_bytes: u64,
}

impl Candidate {
    fn free_ratio(&self) -> f64 {
        if self.total_bytes == 0 {
            // Unknown capacity sorts below any node that reported real
            // numbers, but stays eligible.
            return 0.0;
        }
        self.free_bytes as f64 / self.total_bytes as f64
    }
}

/// The failure domain a node belongs to at `rung`: its label-chain prefix
/// from the top rung down to and including `rung`, joined with '/'.
/// Missing rung labels are skipped; a node with no labels at all falls
/// back to its own name (it is its own domain).
pub fn domain_at(labels: &BTreeMap<String, String>, rungs: &[String], rung: &str) -> String {
    let mut parts = Vec::new();
    for r in rungs {
        if let Some(v) = labels.get(r) {
            parts.push(v.clone());
        }
        if r == rung {
            break;
        }
    }
    if parts.is_empty() {
        labels
            .get("node")
            .cloned()
            .unwrap_or_else(|| "?".to_string())
    } else {
        parts.join("/")
    }
}

/// Pick `replicas` nodes for legs of a `size_bytes` volume: one node per
/// distinct domain at `rung`, preferring emptier nodes (load balancing),
/// ties broken by name for determinism.
pub fn plan(
    candidates: &[Candidate],
    rungs: &[String],
    rung: &str,
    replicas: u32,
    size_bytes: u64,
) -> Result<Vec<String>, String> {
    let fitting: Vec<&Candidate> = candidates
        .iter()
        .filter(|c| c.free_bytes >= size_bytes)
        .collect();
    if fitting.is_empty() {
        return Err(format!(
            "no node with {size_bytes} bytes free ({} candidates)",
            candidates.len()
        ));
    }
    // Best node per domain: emptiest wins, ties by name.
    let mut domains: BTreeMap<String, &Candidate> = BTreeMap::new();
    for c in &fitting {
        let d = domain_at(&c.labels, rungs, rung);
        let replace = match domains.get(&d) {
            None => true,
            Some(cur) => {
                c.free_ratio() > cur.free_ratio()
                    || (c.free_ratio() == cur.free_ratio() && c.name < cur.name)
            }
        };
        if replace {
            domains.insert(d, c);
        }
    }
    if (domains.len() as u32) < replicas {
        return Err(format!(
            "need {replicas} distinct domains at rung {rung:?}, found {}: [{}]",
            domains.len(),
            domains.keys().cloned().collect::<Vec<_>>().join(", ")
        ));
    }
    // Emptiest domains first, ties by name.
    let mut picks: Vec<&Candidate> = domains.into_values().collect();
    picks.sort_by(|a, b| {
        b.free_ratio()
            .partial_cmp(&a.free_ratio())
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.name.cmp(&b.name))
    });
    Ok(picks
        .into_iter()
        .take(replicas as usize)
        .map(|c| c.name.clone())
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rungs() -> Vec<String> {
        ["site", "rack", "cluster", "node"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    fn cand(name: &str, labels: &[(&str, &str)], free: u64, total: u64) -> Candidate {
        Candidate {
            name: name.into(),
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            free_bytes: free,
            total_bytes: total,
        }
    }

    #[test]
    fn domain_chain_prefixes() {
        let l: BTreeMap<String, String> = [("site", "gw"), ("rack", "r1"), ("node", "n1")]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(domain_at(&l, &rungs(), "site"), "gw");
        assert_eq!(domain_at(&l, &rungs(), "rack"), "gw/r1");
        assert_eq!(domain_at(&l, &rungs(), "node"), "gw/r1/n1");
        assert_eq!(domain_at(&BTreeMap::new(), &rungs(), "rack"), "?");
    }

    #[test]
    fn spreads_across_distinct_domains() {
        let cands = vec![
            cand("a1", &[("rack", "r1"), ("node", "a1")], 50, 100),
            cand("a2", &[("rack", "r1"), ("node", "a2")], 90, 100),
            cand("b1", &[("rack", "r2"), ("node", "b1")], 60, 100),
        ];
        let picks = plan(&cands, &rungs(), "rack", 2, 10).unwrap();
        assert_eq!(picks.len(), 2);
        // One per rack; within r1 the emptier a2 wins; r1's pick (90%) is
        // emptier than r2's (60%) so a2 leads.
        assert_eq!(picks, vec!["a2".to_string(), "b1".to_string()]);
    }

    #[test]
    fn insufficient_domains_is_a_hard_explained_error() {
        let cands = vec![
            cand("a1", &[("rack", "r1"), ("node", "a1")], 50, 100),
            cand("a2", &[("rack", "r1"), ("node", "a2")], 90, 100),
        ];
        let err = plan(&cands, &rungs(), "rack", 2, 10).unwrap_err();
        assert!(err.contains("distinct domains"), "{err}");
        // At node rung the same two ARE two domains.
        assert_eq!(plan(&cands, &rungs(), "node", 2, 10).unwrap().len(), 2);
    }

    #[test]
    fn capacity_filters_and_load_balances() {
        let cands = vec![
            cand("small", &[("node", "small")], 5, 100),
            cand("fullish", &[("node", "fullish")], 20, 100),
            cand("empty", &[("node", "empty")], 90, 100),
        ];
        let picks = plan(&cands, &rungs(), "node", 2, 10).unwrap();
        assert_eq!(picks, vec!["empty".to_string(), "fullish".to_string()],
            "small lacks capacity; emptiest first");
        assert!(plan(&cands, &rungs(), "node", 1, 1000).is_err(), "nothing fits");
    }

    #[test]
    fn deterministic_on_ties() {
        let cands = vec![
            cand("b", &[("node", "b")], 50, 100),
            cand("a", &[("node", "a")], 50, 100),
        ];
        assert_eq!(plan(&cands, &rungs(), "node", 1, 1).unwrap(), vec!["a".to_string()]);
    }
}
