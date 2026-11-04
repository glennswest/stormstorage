//! #96 (stormcos#374): the engine client over https. It verifies against the
//! configured CA and nothing else, sends no request to a server it cannot
//! verify, and `http://` keeps working. The engine here is a stand-in that
//! serves real TLS; certificates come from the openssl CLI, the way stormcert
//! makes them (a node CA, a serving pair with the node's SANs).

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use stormstorage::api::AppState;
use stormstorage::config::{Config, NodeConfig};
use stormstorage::engine::Engine;
use stormstorage::model::{FedState, Node, NodeSource, NodeStatus};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

const TOKEN: &str = "node-token";

fn openssl(dir: &Path, args: &[&str]) {
    let out = Command::new("openssl").current_dir(dir).args(args).output().expect("openssl CLI");
    assert!(out.status.success(), "openssl {args:?}: {}", String::from_utf8_lossy(&out.stderr));
}

/// A fresh directory for one test's certificates.
fn certs_dir(test: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("engine-tls-{test}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A CA: `<name>.crt`, `<name>.key`.
fn make_ca(dir: &Path, name: &str) -> PathBuf {
    openssl(dir, &[
        "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes",
        "-keyout", &format!("{name}.key"), "-out", &format!("{name}.crt"),
        "-subj", &format!("/CN={name}"), "-days", "2",
    ]);
    dir.join(format!("{name}.crt"))
}

/// A serving pair for `san` from CA `ca`: (`<name>.crt`, `<name>.key`).
fn make_pair(dir: &Path, ca: &str, name: &str, san: &str) -> (PathBuf, PathBuf) {
    openssl(dir, &[
        "req", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes",
        "-keyout", &format!("{name}.key"), "-out", &format!("{name}.csr"), "-subj", &format!("/CN={name}"),
    ]);
    std::fs::write(
        dir.join(format!("{name}.ext")),
        format!("subjectAltName={san}\nextendedKeyUsage=serverAuth\n"),
    )
    .unwrap();
    openssl(dir, &[
        "x509", "-req", "-in", &format!("{name}.csr"), "-CA", &format!("{ca}.crt"),
        "-CAkey", &format!("{ca}.key"), "-CAcreateserial", "-out", &format!("{name}.crt"),
        "-days", "2", "-extfile", &format!("{name}.ext"),
    ]);
    (dir.join(format!("{name}.crt")), dir.join(format!("{name}.key")))
}

/// What a stand-in was asked: the request line and bearer of each request.
type Seen = Arc<Mutex<Vec<(String, String)>>>;

/// One HTTP/1.1 exchange, answered as a stormblock would for a poll.
async fn serve<S: AsyncRead + AsyncWrite + Unpin>(mut s: S, seen: Seen) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 2048];
    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
        match s.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
    let head = String::from_utf8_lossy(&buf).to_string();
    let line = head.lines().next().unwrap_or_default().to_string();
    let bearer = head
        .lines()
        .find_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.eq_ignore_ascii_case("authorization").then(|| v.trim().to_string())
        })
        .unwrap_or_default();
    seen.lock().unwrap().push((line.clone(), bearer));
    let body = if line.contains("/v1/nodes/capacity") {
        r#"[{"node":"n","total_bytes":1073741824,"free_bytes":536870912}]"#
    } else if line.contains("/api/v1/discovery") {
        r#"{"local_node":"n","nodes":[]}"#
    } else {
        r#"{"items":[],"count":0}"#
    };
    let reply = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = s.write_all(reply.as_bytes()).await;
    let _ = s.shutdown().await;
}

/// An engine serving TLS from this pair on 127.0.0.1. Its base URL, and what
/// reached it as HTTP (a refused handshake never does).
async fn tls_engine(cert: &Path, key: &Path) -> (String, Seen) {
    let chain: Vec<CertificateDer<'static>> =
        CertificateDer::pem_file_iter(cert).unwrap().map(|c| c.unwrap()).collect();
    let key = PrivateKeyDer::from_pem_file(key).unwrap();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen: Seen = Default::default();
    let log = seen.clone();
    tokio::spawn(async move {
        while let Ok((tcp, _)) = listener.accept().await {
            let (acceptor, log) = (acceptor.clone(), log.clone());
            tokio::spawn(async move {
                if let Ok(tls) = acceptor.accept(tcp).await {
                    serve(tls, log).await;
                }
            });
        }
    });
    (format!("https://{addr}"), seen)
}

/// The same engine before its pair: plain HTTP.
async fn plain_engine() -> (String, Seen) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen: Seen = Default::default();
    let log = seen.clone();
    tokio::spawn(async move {
        while let Ok((tcp, _)) = listener.accept().await {
            tokio::spawn(serve(tcp, log.clone()));
        }
    });
    (format!("http://{addr}"), seen)
}

fn engine(url: &str, ca: Option<&Path>) -> Engine {
    Engine::new(url, Some(TOKEN.into()), ca.and_then(Path::to_str))
}

/// The error, its causes included: what an event and the log show.
fn refusal(e: anyhow::Error) -> String {
    format!("{e:#}")
}

#[tokio::test]
async fn verified_against_the_configured_ca_with_the_token_inside() {
    let dir = certs_dir("right-ca");
    let ca = make_ca(&dir, "node-ca");
    let (cert, key) = make_pair(&dir, "node-ca", "engine", "IP:127.0.0.1");
    let (url, seen) = tls_engine(&cert, &key).await;

    let cap = engine(&url, Some(&ca)).capacity().await.unwrap();
    assert_eq!(cap.total_bytes, 1 << 30);
    let seen = seen.lock().unwrap();
    assert!(seen[0].0.contains("/v1/nodes/capacity"), "{seen:?}");
    assert_eq!(seen[0].1, format!("Bearer {TOKEN}"));
}

#[tokio::test]
async fn a_certificate_from_another_ca_is_refused_before_any_request() {
    let dir = certs_dir("other-ca");
    let _node = make_ca(&dir, "node-ca");
    let other = make_ca(&dir, "other-ca");
    let (cert, key) = make_pair(&dir, "node-ca", "engine", "IP:127.0.0.1");
    let (url, seen) = tls_engine(&cert, &key).await;

    let why = refusal(engine(&url, Some(&other)).capacity().await.unwrap_err());
    assert!(why.to_lowercase().contains("certificate"), "{why}");
    assert!(seen.lock().unwrap().is_empty(), "the token never left");
}

#[tokio::test]
async fn a_certificate_for_another_name_is_refused() {
    let dir = certs_dir("other-name");
    let ca = make_ca(&dir, "node-ca");
    let (cert, key) = make_pair(&dir, "node-ca", "engine", "IP:192.0.2.1,DNS:another-node");
    let (url, seen) = tls_engine(&cert, &key).await;

    let why = refusal(engine(&url, Some(&ca)).capacity().await.unwrap_err());
    assert!(why.to_lowercase().contains("certificate"), "{why}");
    assert!(seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn no_usable_ca_means_no_request_and_no_panic() {
    let dir = certs_dir("no-ca");
    let _ca = make_ca(&dir, "node-ca");
    let (cert, key) = make_pair(&dir, "node-ca", "engine", "IP:127.0.0.1");
    let (url, seen) = tls_engine(&cert, &key).await;

    let garbage = dir.join("garbage.crt");
    std::fs::write(&garbage, "this is not a certificate\n").unwrap();
    let empty_der = dir.join("bad-der.crt");
    std::fs::write(&empty_der, "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n").unwrap();
    let missing = dir.join("not-there.crt");
    for ca in [None, Some(missing.as_path()), Some(garbage.as_path()), Some(empty_der.as_path())] {
        assert!(engine(&url, ca).capacity().await.is_err(), "{ca:?}");
    }
    assert!(seen.lock().unwrap().is_empty(), "nothing reached the engine");
}

#[tokio::test]
async fn a_bundle_of_cas_is_trusted_and_a_renewed_file_is_read() {
    let dir = certs_dir("bundle");
    let ca = make_ca(&dir, "node-ca");
    let other = make_ca(&dir, "other-ca");
    let (cert, key) = make_pair(&dir, "node-ca", "engine", "IP:127.0.0.1");
    let (url, _) = tls_engine(&cert, &key).await;

    let file = dir.join("trust.pem");
    std::fs::copy(&other, &file).unwrap();
    assert!(engine(&url, Some(&file)).capacity().await.is_err(), "only the other CA yet");
    let bundle = [std::fs::read(&other).unwrap(), std::fs::read(&ca).unwrap()].concat();
    std::fs::write(&file, bundle).unwrap();
    engine(&url, Some(&file)).capacity().await.expect("the next client reads the renewed file");
}

#[tokio::test]
async fn http_keeps_working_and_never_reads_the_ca() {
    let (url, seen) = plain_engine().await;
    let cap = engine(&url, Some(Path::new("/nonexistent/ca.crt"))).capacity().await.unwrap();
    assert_eq!(cap.free_bytes, 1 << 29);
    assert_eq!(seen.lock().unwrap()[0].1, format!("Bearer {TOKEN}"));
    engine(&url, None).capacity().await.unwrap();
}

fn registered(url: &str, ca_file: Option<&Path>) -> FedState {
    let mut fed = FedState::default();
    let config: NodeConfig =
        toml::from_str(&format!("name = \"n\"\nengine_url = \"{url}\"\napi_token = \"{TOKEN}\"")).unwrap();
    let config = NodeConfig { ca_file: ca_file.map(|p| p.display().to_string()), ..config };
    fed.nodes.insert("n".into(), Node { config, status: NodeStatus::new(NodeSource::Registered) });
    fed
}

async fn healthy_after_poll(state: &Arc<AppState>) -> bool {
    stormstorage::registry::poll_once(state).await;
    state.fed.read().await.nodes["n"].status.last_ok.is_some()
}

fn no_local() -> Config {
    let mut c = Config::default();
    c.local.enabled = false;
    c.kubernetes.enabled = false;
    c
}

#[tokio::test]
async fn the_poller_and_engine_for_use_the_configured_ca() {
    let dir = certs_dir("poll");
    let ca = make_ca(&dir, "node-ca");
    let other = make_ca(&dir, "other-ca");
    let (cert, key) = make_pair(&dir, "node-ca", "engine", "IP:127.0.0.1");
    let (url, _) = tls_engine(&cert, &key).await;

    // [engine_tls] ca_file.
    let mut config = no_local();
    config.engine_tls.ca_file = Some(ca.display().to_string());
    let state = Arc::new(AppState::new(config, registered(&url, None), None));
    assert!(healthy_after_poll(&state).await);

    // [[nodes]] ca_file is before it, in both directions.
    let mut config = no_local();
    config.engine_tls.ca_file = Some(other.display().to_string());
    let state = Arc::new(AppState::new(config, registered(&url, Some(&ca)), None));
    assert!(healthy_after_poll(&state).await, "the node's own CA wins");

    let mut config = no_local();
    config.engine_tls.ca_file = Some(ca.display().to_string());
    let state = Arc::new(AppState::new(config, registered(&url, Some(&other)), None));
    assert!(!healthy_after_poll(&state).await, "the node's own CA, not the fleet's");
    let node = state.fed.read().await.nodes["n"].clone();
    assert_eq!(node.status.consecutive_failures, 1);
    assert!(state.engine_for(&node).capacity().await.is_err(), "engine_for verifies the same way");
}

#[tokio::test]
async fn the_local_engine_is_adopted_over_https() {
    let dir = certs_dir("adopt");
    let ca = make_ca(&dir, "node-ca");
    let (cert, key) = make_pair(&dir, "node-ca", "engine", "IP:127.0.0.1");
    let (url, _) = tls_engine(&cert, &key).await;

    let mut config = Config::default();
    config.kubernetes.enabled = false;
    config.local.engine_url = url.clone();
    config.engine_tls.ca_file = Some(ca.display().to_string());
    let state = Arc::new(AppState::new(config, FedState::default(), None));
    stormstorage::registry::poll_once(&state).await;
    let fed = state.fed.read().await;
    let node = fed.nodes.get("n").expect("adopted by the name its discovery gave");
    assert_eq!(node.config.engine_url, url);
    assert!(node.status.healthy);
}

#[tokio::test]
async fn a_heartbeat_address_takes_the_configured_scheme() {
    for (scheme, want) in [("http", "http://10.0.0.7:9090"), ("https", "https://10.0.0.7:9090")] {
        let mut config = no_local();
        config.engine_tls.address_scheme = scheme.into();
        let state = Arc::new(AppState::new(config, FedState::default(), None));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = stormstorage::api::router(state.clone());
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let body = serde_json::json!({"hostname": "n", "node_addr": "10.0.0.7:9090", "volumes": []});
        let reply = reqwest::Client::new()
            .post(format!("http://{addr}/api/v1/storage/register"))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert!(reply.status().is_success());
        assert_eq!(state.fed.read().await.nodes["n"].config.engine_url, want);
    }
}
