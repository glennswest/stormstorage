//! #81: an empty or truncated state file (a power cut mid-write) never
//! stops stormstorage: it starts, the bad file is kept, and health carries
//! the `state_recovered` condition with an Error event.

use serde_json::Value;
use std::sync::Arc;
use stormstorage::api::AppState;
use stormstorage::config::Config;
use stormstorage::model::load_state;

async fn start(content: &str, tag: &str) -> (String, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!("ss-state81-{tag}-{}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("state.json");
    std::fs::write(&path, content).unwrap();

    let (fed, rec) = load_state(&path).expect("a bad state file does not fail startup");
    let mut config = Config::default();
    config.local.enabled = false;
    let mut state = AppState::new(config, fed, Some(path));
    state.note_state_recovery(rec.expect("recovery recorded"));
    let state = Arc::new(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = stormstorage::api::router(state);
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (format!("http://{addr}/api/v1"), dir)
}

#[tokio::test]
async fn empty_and_truncated_state_files_start_with_the_condition() {
    for (tag, content) in [("empty", ""), ("truncated", "{\"volumes\": {\"v\": {\"na")] {
        let (api, dir) = start(content, tag).await;
        let c = reqwest::Client::new();

        let h: Value = c.get(format!("{api}/health")).send().await.unwrap().json().await.unwrap();
        assert_eq!(h["status"], "ok", "{tag}: serving");
        let cond = &h["conditions"][0];
        assert_eq!(cond["type"], "state_recovered", "{tag}: {h}");
        assert_eq!(cond["started_from"], "empty");
        let kept = cond["kept_as"].as_str().unwrap();
        assert!(kept.contains("state.json.bad-"), "{kept}");
        assert_eq!(std::fs::read_to_string(kept).unwrap(), content, "{tag}: bad file kept");

        let st: Value = c.get(format!("{api}/store")).send().await.unwrap().json().await.unwrap();
        assert_eq!(st["conditions"][0]["type"], "state_recovered");

        let ev: Value = c.get(format!("{api}/events")).send().await.unwrap().json().await.unwrap();
        let text = ev.to_string();
        assert!(text.contains("did not parse") && text.contains("\"error\""), "{tag}: {text}");

        // The API works: a volume list on the empty state.
        let r = c.get(format!("{api}/volumes")).send().await.unwrap();
        assert!(r.status().is_success());
        std::fs::remove_dir_all(&dir).ok();
    }
}
