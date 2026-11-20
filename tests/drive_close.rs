//! #123: closing a drive by its URI sends the whole URI as the drive id.
//! A leg's `nvme-tcp://…?nsid=…&hostnqn=…` must reach the engine intact
//! (not cut at `?` into the query), and `force` must be the only query.

use axum::extract::{Path, RawQuery};
use axum::http::StatusCode;
use axum::routing::delete;
use axum::Router;
use std::sync::{Arc, Mutex};
use stormstorage::engine::{path_segment, AttachedLeg, Engine};

type Seen = Arc<Mutex<Vec<(String, Option<String>)>>>;

async fn engine(seen: Seen) -> String {
    let app = Router::new().route(
        "/api/v1/drives/{id}",
        delete(move |Path(id): Path<String>, RawQuery(q): RawQuery| {
            let seen = seen.clone();
            async move {
                seen.lock().unwrap().push((id, q));
                StatusCode::OK
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

#[tokio::test]
async fn drive_uri_with_query_is_one_path_segment() {
    let leg = AttachedLeg {
        nqn: "nqn.2024-01.io.stormblock:vol-a".into(),
        traddr: "192.168.16.5".into(),
        trsvcid: 4420,
        nsid: 1,
        host_nqn: Some("nqn.2014-08.org.nvmexpress:uuid:h1".into()),
    };
    let uri = leg.drive_uri();
    assert!(uri.contains('?') && uri.contains('&'), "{uri}");
    let odd = "nvme-tcp://h:1/n?nsid=2#frag 50%";

    let seen: Seen = Default::default();
    let e = Engine::new(&engine(seen.clone()).await, None, None);
    e.delete_drive(&uri, true).await.unwrap();
    e.delete_drive(&uri, false).await.unwrap();
    e.delete_drive(odd, true).await.unwrap();

    let seen = seen.lock().unwrap().clone();
    assert_eq!(
        seen,
        vec![
            (uri.clone(), Some("force=true".into())),
            (uri.clone(), None),
            (odd.to_string(), Some("force=true".into())),
        ]
    );
}

#[test]
fn path_segment_escapes_delimiters() {
    assert_eq!(path_segment("nvme-tcp://x"), "nvme-tcp:%2F%2Fx");
    assert_eq!(path_segment("a?b=1&c#d e%"), "a%3Fb%3D1%26c%23d%20e%25");
    assert_eq!(path_segment("uuid-1_2.3~x@y"), "uuid-1_2.3~x@y");
}
