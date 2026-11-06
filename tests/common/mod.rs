//! A mock apiserver for #56: stormcluster's ClusterMembers and one
//! Endpoints store, with resourceVersion conflicts as the real one has.

// Each test crate uses a part of this.
#![allow(dead_code)]
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub struct Api {
    pub members: Vec<Value>,
    /// An answer in place of the members list.
    pub members_status: Option<StatusCode>,
    /// An answer in place of any Endpoints call.
    pub endpoints_status: Option<StatusCode>,
    pub endpoints: BTreeMap<String, Value>,
    pub writes: usize,
    pub rv: u64,
    pub bearers: Vec<String>,
}

pub type ApiMock = Arc<Mutex<Api>>;

impl Api {
    /// `(ip, port, leader)` the Endpoints say.
    pub fn says(&self, key: &str) -> Option<(String, u64, String)> {
        let ep = self.endpoints.get(key)?;
        Some((
            ep["subsets"][0]["addresses"][0]["ip"].as_str()?.to_string(),
            ep["subsets"][0]["ports"][0]["port"].as_u64()?,
            ep["metadata"]["annotations"]["stormstorage.storm.io/leader"].as_str()?.to_string(),
        ))
    }
}

pub fn member(name: &str, address: &str, storage: bool) -> Value {
    json!({
        "apiVersion": "cluster.storm.io/v1alpha1",
        "kind": "ClusterMember",
        "metadata": { "name": name },
        "spec": { "storage": storage },
        "status": { "address": address, "phase": "Ready" },
    })
}

fn note(api: &ApiMock, h: &HeaderMap) {
    if let Some(a) = h.get("authorization").and_then(|v| v.to_str().ok()) {
        api.lock().unwrap().bearers.push(a.to_string());
    }
}

async fn list_members(State(api): State<ApiMock>, h: HeaderMap) -> Response {
    note(&api, &h);
    let a = api.lock().unwrap();
    match a.members_status {
        Some(s) => s.into_response(),
        None => Json(json!({ "kind": "ClusterMemberList", "items": a.members })).into_response(),
    }
}

async fn read(State(api): State<ApiMock>, Path((ns, name)): Path<(String, String)>) -> Response {
    let a = api.lock().unwrap();
    if let Some(s) = a.endpoints_status {
        return s.into_response();
    }
    match a.endpoints.get(&format!("{ns}/{name}")) {
        Some(ep) => Json(ep.clone()).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn create(State(api): State<ApiMock>, Path(ns): Path<String>, Json(mut ep): Json<Value>) -> Response {
    let mut a = api.lock().unwrap();
    if let Some(s) = a.endpoints_status {
        return s.into_response();
    }
    let key = format!("{ns}/{}", ep["metadata"]["name"].as_str().unwrap_or_default());
    if a.endpoints.contains_key(&key) {
        return StatusCode::CONFLICT.into_response();
    }
    a.rv += 1;
    ep["metadata"]["resourceVersion"] = json!(a.rv.to_string());
    a.writes += 1;
    a.endpoints.insert(key, ep.clone());
    (StatusCode::CREATED, Json(ep)).into_response()
}

async fn replace(
    State(api): State<ApiMock>,
    Path((ns, name)): Path<(String, String)>,
    Json(mut ep): Json<Value>,
) -> Response {
    let mut a = api.lock().unwrap();
    if let Some(s) = a.endpoints_status {
        return s.into_response();
    }
    let key = format!("{ns}/{name}");
    let Some(old) = a.endpoints.get(&key) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if old["metadata"]["resourceVersion"] != ep["metadata"]["resourceVersion"] {
        return StatusCode::CONFLICT.into_response();
    }
    a.rv += 1;
    ep["metadata"]["resourceVersion"] = json!(a.rv.to_string());
    a.writes += 1;
    a.endpoints.insert(key, ep.clone());
    Json(ep).into_response()
}

pub async fn mock_apiserver(api: ApiMock) -> String {
    let app = Router::new()
        .route("/apis/cluster.storm.io/v1alpha1/clustermembers", get(list_members))
        .route("/api/v1/namespaces/{ns}/endpoints", post(create))
        .route("/api/v1/namespaces/{ns}/endpoints/{name}", get(read).put(replace))
        .with_state(api);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}
