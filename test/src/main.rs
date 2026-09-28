//! stormstorage's test container (#8), per stormcentral
//! `docs/test-standard.md`: `/test short|medium|long`.
//!
//! The suites drive the stormstorage running on the node under test
//! (`STORM_NODE:9093`) through its REST API. Exit 0 when every test passed,
//! 1 when one failed, 2 when the run could not happen. One JSON object per
//! test on stdout, a summary last.

mod api;
mod checks;
mod env;
mod long;
mod medium;
mod report;
mod short;

use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let suite = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("STORM_SUITE").ok())
        .unwrap_or_else(|| "short".into());
    if !["short", "medium", "long"].contains(&suite.as_str()) {
        eprintln!("usage: test short|medium|long");
        std::process::exit(2);
    }
    let env = env::Env::read(&suite);
    let mut r = report::Report::new(&env.results);
    let Some(base) = env.base.clone() else {
        eprintln!("could not run: neither STORM_NODE nor STORM_STORMSTORAGE_URL is set");
        r.summary();
        std::process::exit(2);
    };
    let api = Arc::new(api::Api::new(&base, env.token.clone()));
    let limit = env.timeout + Duration::from_secs(30);
    let result = match suite.as_str() {
        "short" => tokio::time::timeout(limit, short::run(&env, &api, &mut r)).await,
        "medium" => tokio::time::timeout(limit, medium::run(&env, &api, &mut r)).await,
        _ => tokio::time::timeout(limit, long::run(&env, api.clone(), &mut r)).await,
    };
    let code = match result {
        Ok(Ok(())) => i32::from(r.fail > 0),
        Ok(Err(e)) => {
            eprintln!("could not run: {e}");
            2
        }
        Err(_) => {
            r.record(
                "suite-timeout",
                &Err(report::Why::Fail(format!("{suite} ran past {}s", env.timeout.as_secs()))),
                0,
            );
            // Whatever the timed-out suite created goes now.
            checks::cleanup(&api, &env).await;
            1
        }
    };
    r.summary();
    std::process::exit(code);
}
