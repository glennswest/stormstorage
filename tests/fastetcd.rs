//! #56 against a **real fastetcd**, built by nobody here (`SC_TOOLS=fastetcd`,
//! `FASTETCD_BIN`; without it these tests say so and pass, and
//! scripts/e2e-fastetcd.sh refuses to run). What #34 proved against a mock:
//! fastetcd's own gateway, over its mutual TLS as stormcos ships it:
//!
//! * with auth off (a stormcos cluster's fastetcd today): election, import,
//!   follow, failover and the published Endpoints;
//! * with auth on: a CN user whose role covers one prefix can lead under it,
//!   and cannot read or lead outside it; a pair renewed under a running client
//!   is taken up.

mod common;

use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};
use stormstorage::api::AppState;
use stormstorage::config::{Config, NodeConfig, StoreBackend, StoreConfig};
use stormstorage::model::{FedState, Node, NodeSource, NodeStatus};
use stormstorage::store::{b64, prefix_end, Etcd, Role};

const SERVICE: &str = "stormblock-system/stormstorage";

fn openssl(dir: &Path, args: &[&str]) {
    let out = Command::new("openssl").current_dir(dir).args(args).output().expect("openssl CLI");
    assert!(out.status.success(), "openssl {args:?}: {}", String::from_utf8_lossy(&out.stderr));
}

/// A pair from `ca`, for a client (the CN is the etcd user) or the server.
fn pair(dir: &Path, name: &str, cn: &str, ext: &str) {
    openssl(dir, &[
        "req", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes",
        "-keyout", &format!("{name}.key"), "-out", &format!("{name}.csr"), "-subj", &format!("/CN={cn}"),
    ]);
    std::fs::write(dir.join(format!("{name}.ext")), ext).unwrap();
    openssl(dir, &[
        "x509", "-req", "-in", &format!("{name}.csr"), "-CA", "ca.crt", "-CAkey", "ca.key", "-CAcreateserial",
        "-out", &format!("{name}.crt"), "-days", "2", "-extfile", &format!("{name}.ext"),
    ]);
}

const CLIENT: &str = "extendedKeyUsage=clientAuth\n";

struct Fastetcd {
    child: Child,
    url: String,
    dir: PathBuf,
}

impl Drop for Fastetcd {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// fastetcd with the TLS flags stormcos gives it, and its pairs.
async fn start(test: &str) -> Option<Fastetcd> {
    let Ok(bin) = std::env::var("FASTETCD_BIN") else {
        eprintln!("FASTETCD_BIN is not set: skipped (scripts/e2e-fastetcd.sh runs this)");
        return None;
    };
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("fastetcd-{test}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    openssl(&dir, &[
        "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes",
        "-keyout", "ca.key", "-out", "ca.crt", "-subj", "/CN=node-ca", "-days", "2",
    ]);
    pair(&dir, "fastetcd", "fastetcd", "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n");
    pair(&dir, "stormstorage-etcd", "stormstorage-etcd", CLIENT);
    pair(&dir, "root", "root", CLIENT);
    pair(&dir, "intruder", "intruder", CLIENT);

    let (client, peer, metrics) = (free_port(), free_port(), free_port());
    let url = format!("https://127.0.0.1:{client}");
    let d = |f: &str| dir.join(f).display().to_string();
    let log = std::fs::File::create(dir.join("fastetcd.log")).unwrap();
    let child = Command::new(bin)
        .args(["--data-dir", &d("data"), "--name", "default"])
        .args(["--listen-client-urls", &url, "--advertise-client-urls", &url])
        .args(["--listen-peer-urls", &format!("http://127.0.0.1:{peer}")])
        .args(["--initial-advertise-peer-urls", &format!("http://127.0.0.1:{peer}")])
        .args(["--initial-cluster", &format!("default=http://127.0.0.1:{peer}")])
        .args(["--listen-metrics-urls", &format!("http://127.0.0.1:{metrics}")])
        .args(["--cert-file", &d("fastetcd.crt"), "--key-file", &d("fastetcd.key")])
        .args(["--trusted-ca-file", &d("ca.crt"), "--client-cert-auth"])
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log))
        .spawn()
        .expect("start fastetcd");
    let f = Fastetcd { child, url, dir };
    let etcd = Etcd::new(&f.store_config("stormstorage-etcd", "/probe/")).unwrap();
    let t0 = Instant::now();
    while let Err(e) = etcd.range("/probe/x").await {
        assert!(
            t0.elapsed() < Duration::from_secs(60),
            "fastetcd did not answer: {e:#}\n{}",
            std::fs::read_to_string(f.dir.join("fastetcd.log")).unwrap_or_default()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Some(f)
}

impl Fastetcd {
    fn store_config(&self, pair: &str, prefix: &str) -> StoreConfig {
        let d = |f: String| Some(self.dir.join(f).display().to_string());
        StoreConfig {
            backend: StoreBackend::Etcd,
            endpoints: vec![self.url.clone()],
            prefix: prefix.into(),
            ca_file: d("ca.crt".into()),
            cert_file: d(format!("{pair}.crt")),
            key_file: d(format!("{pair}.key")),
            lease_ttl_secs: 3,
            ..Default::default()
        }
    }

    fn instance(&self, name: &str, prefix: &str, api: &str, fed: FedState) -> Arc<AppState> {
        let mut config = Config::default();
        config.local.enabled = false;
        config.store = self.store_config("stormstorage-etcd", prefix);
        config.store.instance = Some(name.into());
        config.store.advertise_url = Some(format!("http://192.0.2.{}:9093", name.as_bytes()[0] - b'a' + 1));
        config.store.publish_service = Some(SERVICE.into());
        config.kubernetes.server = api.into();
        config.validate().unwrap();
        Arc::new(AppState::new(config, fed, None))
    }
}

fn store(s: &AppState) -> &stormstorage::store::Store {
    s.store.as_deref().unwrap()
}

fn registered(name: &str) -> Node {
    let config: NodeConfig =
        toml::from_str(&format!("name = \"{name}\"\nengine_url = \"http://{name}:9090\"")).unwrap();
    Node { config, status: NodeStatus::new(NodeSource::Registered) }
}

/// Step until `cond` holds: a real Raft answers in its own time.
async fn until(what: &str, s: &AppState, cond: impl Fn() -> bool) {
    let t0 = Instant::now();
    while !cond() {
        assert!(t0.elapsed() < Duration::from_secs(30), "{what}: {:?}", store(s).status());
        store(s).step(s).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn election_state_failover_and_publication_over_mutual_tls() {
    let Some(f) = start("ship").await else { return };
    let api: common::ApiMock = Default::default();
    let url = common::mock_apiserver(api.clone()).await;

    let mut fed_a = FedState::default();
    fed_a.nodes.insert("pre".into(), registered("pre"));
    fed_a.revision = 5;
    let a = f.instance("a", "/stormstorage/", &url, fed_a);
    let b = f.instance("b", "/stormstorage/", &url, FedState::default());

    // A leads, imports its state, and publishes.
    until("a leads", &a, || store(&a).is_leader()).await;
    assert_eq!(api.lock().unwrap().says(SERVICE), Some(("192.0.2.1".into(), 9093, "a".into())));

    // B follows and loads it.
    until("b loads", &b, || store(&b).status().loads > 0).await;
    assert_eq!(store(&b).status().role, Role::Follower);
    assert_eq!(store(&b).status().leader.unwrap().instance, "a");
    assert!(b.fed.read().await.nodes.contains_key("pre"));
    assert_eq!(b.fed.read().await.revision, 5);

    // A's write reaches B.
    a.fed.write().await.nodes.insert("n2".into(), registered("n2"));
    a.persist().await;
    assert!(store(&a).status().writes >= 2, "{:?}", store(&a).status());
    let t0 = Instant::now();
    while !b.fed.read().await.nodes.contains_key("n2") {
        assert!(t0.elapsed() < Duration::from_secs(30));
        store(&b).step(&b).await;
    }

    // Keepalives hold the lease past its TTL.
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(4) {
        store(&a).step(&a).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(store(&a).is_leader(), "keepalive: {:?}", store(&a).status());
    store(&b).step(&b).await;
    assert_eq!(store(&b).status().role, Role::Follower);

    // A stops renewing: B takes over with the state, and the Endpoints follow.
    until("b takes over", &b, || store(&b).is_leader()).await;
    assert!(b.fed.read().await.nodes.contains_key("n2"));
    assert_eq!(api.lock().unwrap().says(SERVICE), Some(("192.0.2.2".into(), 9093, "b".into())));

    // A's own next round finds out; a write it tried is refused by the guard.
    a.fed.write().await.nodes.insert("late".into(), registered("late"));
    a.persist().await;
    until("a steps down", &a, || !store(&a).is_leader()).await;
    let keys = f_keys(&f, "/stormstorage/").await;
    assert!(!keys.iter().any(|k| k.ends_with("nodes/late")), "a deposed leader wrote: {keys:?}");
}

async fn f_keys(f: &Fastetcd, prefix: &str) -> Vec<String> {
    let etcd = Etcd::new(&f.store_config("stormstorage-etcd", prefix)).unwrap();
    etcd.range_prefix(prefix).await.unwrap().into_iter().map(|kv| kv.key).collect()
}

/// A root-only call, as the certificate whose CN is `root`.
async fn admin(root: &Etcd, path: &str, body: Value) {
    root.call(path, &body).await.unwrap_or_else(|e| panic!("{path} {body}: {e:#}"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cn_user_can_be_given_the_prefix_and_nothing_else() {
    let Some(f) = start("auth").await else { return };
    let api: common::ApiMock = Default::default();
    let url = common::mock_apiserver(api.clone()).await;

    let root = Etcd::new(&f.store_config("root", "/x/")).unwrap();
    let prefix = "/stormstorage/c1/";
    admin(&root, "/v3/auth/role/add", json!({ "name": "root" })).await;
    admin(&root, "/v3/auth/user/add", json!({ "name": "root", "options": { "no_password": true } })).await;
    admin(&root, "/v3/auth/user/grant", json!({ "user": "root", "role": "root" })).await;
    admin(&root, "/v3/auth/role/add", json!({ "name": "stormstorage" })).await;
    admin(&root, "/v3/auth/role/grant", json!({
        "name": "stormstorage",
        "perm": { "permType": "READWRITE", "key": b64(prefix.as_bytes()), "range_end": b64(&prefix_end(prefix)) },
    }))
    .await;
    admin(&root, "/v3/auth/user/add", json!({ "name": "stormstorage-etcd", "options": { "no_password": true } })).await;
    admin(&root, "/v3/auth/user/grant", json!({ "user": "stormstorage-etcd", "role": "stormstorage" })).await;
    admin(&root, "/v3/auth/enable", json!({})).await;

    // Under its prefix: it leads and writes.
    let mut fed = FedState::default();
    fed.nodes.insert("pre".into(), registered("pre"));
    let a = f.instance("a", prefix, &url, fed);
    until("a leads", &a, || store(&a).is_leader()).await;
    let keys = f_keys(&f, prefix).await;
    assert!(keys.iter().any(|k| k.ends_with("nodes/pre")), "{keys:?}");
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(4) {
        store(&a).step(&a).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(store(&a).is_leader(), "the lease holds with auth on: {:?}", store(&a).status());

    // Under another: no leadership, and it says why.
    let other = f.instance("b", "/stormstorage/c2/", &url, FedState::default());
    store(&other).step(&other).await;
    let st = store(&other).status();
    assert!(!store(&other).is_leader());
    let e = st.last_error.unwrap_or_default();
    assert!(e.contains("403") || e.to_lowercase().contains("permission"), "{e}");

    // A certificate whose CN names no user reads nothing.
    let intruder = Etcd::new(&f.store_config("intruder", prefix)).unwrap();
    assert!(intruder.range_prefix(prefix).await.is_err());

    // A pair renewed under a running client is taken up.
    let mut cfg = f.store_config("stormstorage-etcd", prefix);
    let dir = f.dir.join("renewed");
    std::fs::create_dir_all(&dir).unwrap();
    for ext in ["crt", "key"] {
        std::fs::copy(f.dir.join(format!("stormstorage-etcd.{ext}")), dir.join(format!("c.{ext}"))).unwrap();
    }
    cfg.cert_file = Some(dir.join("c.crt").display().to_string());
    cfg.key_file = Some(dir.join("c.key").display().to_string());
    let running = Etcd::new(&cfg).unwrap();
    assert!(running.range_prefix(prefix).await.is_ok());
    for ext in ["crt", "key"] {
        std::fs::copy(f.dir.join(format!("intruder.{ext}")), dir.join(format!("c.{ext}"))).unwrap();
    }
    running.refresh_tls();
    assert!(running.range_prefix(prefix).await.is_err(), "still the old pair");
}
