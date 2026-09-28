//! What the runner hands the container (stormcentral docs/test-standard.md).

use std::path::PathBuf;
use std::time::Duration;

pub struct Env {
    /// The node's stormstorage API, `http://<STORM_NODE>:9093`.
    pub base: Option<String>,
    pub run_id: String,
    pub timeout: Duration,
    pub results: PathBuf,
    /// Bearer token for the node's stormstorage, when its `[api] api_token`
    /// is set (#6). Without it, writes answered 401 are skips, never passes.
    pub token: Option<String>,
    /// Upper bound on a long wave's volumes.
    pub wave_max: Option<usize>,
}

fn var(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.trim().is_empty())
}

/// `host`, `host:port`, `v6`, `[v6]` or `[v6]:port` → `http://…` with
/// stormstorage's port when none is given.
fn with_port(host: &str) -> String {
    let has_port = if host.starts_with('[') {
        host.contains("]:")
    } else {
        host.matches(':').count() == 1
    };
    match (has_port, host.starts_with('['), host.contains(':')) {
        (true, _, _) => format!("http://{host}"),
        (false, true, _) => format!("http://{host}:9093"),
        (false, false, true) => format!("http://[{host}]:9093"),
        (false, false, false) => format!("http://{host}:9093"),
    }
}

impl Env {
    pub fn read(suite: &str) -> Env {
        let default_timeout = match suite {
            "short" => 120,
            "medium" => 1800,
            _ => 8 * 3600,
        };
        // STORM_STORMSTORAGE_URL overrides, for a run against a dev instance.
        let base = var("STORM_STORMSTORAGE_URL").or_else(|| {
            var("STORM_NODE").map(|n| {
                let host = n.trim_start_matches("http://").trim_end_matches('/').to_string();
                with_port(&host)
            })
        });
        Env {
            base: base.map(|b| b.trim_end_matches('/').to_string()),
            run_id: var("STORM_RUN_ID").unwrap_or_else(|| format!("local-{}", std::process::id())),
            timeout: Duration::from_secs(var("STORM_TIMEOUT").and_then(|t| t.parse().ok()).unwrap_or(default_timeout)),
            results: PathBuf::from(var("STORM_RESULTS").unwrap_or_else(|| "/results".into())),
            token: var("STORM_STORMSTORAGE_TOKEN"),
            wave_max: var("STORM_WAVE_MAX").and_then(|v| v.parse().ok()),
        }
    }

    /// A volume name owned by this run: `t-<run>-<tag>`, lowercase, safe.
    pub fn vol(&self, tag: &str) -> String {
        let run: String = self
            .run_id
            .to_lowercase()
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
            .collect();
        let run = &run[run.len().saturating_sub(12)..];
        format!("t-{run}-{tag}")
    }

    pub fn prefix(&self) -> String {
        self.vol("")
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn node_addresses() {
        use super::with_port;
        assert_eq!(with_port("10.0.0.5"), "http://10.0.0.5:9093");
        assert_eq!(with_port("node1:9999"), "http://node1:9999");
        assert_eq!(with_port("fe80::1"), "http://[fe80::1]:9093");
        assert_eq!(with_port("[fe80::1]"), "http://[fe80::1]:9093");
        assert_eq!(with_port("[fe80::1]:9093"), "http://[fe80::1]:9093");
    }
}
