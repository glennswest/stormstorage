use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use stormstorage::api::AppState;
use stormstorage::config::Config;
use stormstorage::events::EventLog;
use stormstorage::model::FedState;
use tokio::sync::RwLock;

#[derive(Parser, Debug)]
#[command(name = "stormstorage", version, about = "Storage control plane across Storm nodes and clusters")]
struct Args {
    /// Config file (missing file = defaults)
    #[arg(long, default_value = "/etc/stormstorage/stormstorage.toml")]
    config: PathBuf,
    /// Override listen address
    #[arg(long)]
    listen: Option<String>,
    /// Override data directory
    #[arg(long)]
    data_dir: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let mut config = Config::load(&args.config)?;
    if let Some(l) = args.listen {
        config.listen_addr = l;
    }
    if let Some(d) = args.data_dir {
        config.data_dir = Some(d);
    }
    config.validate()?;

    let state_path = config
        .data_dir
        .as_ref()
        .map(|d| PathBuf::from(d).join("state.json"));
    let mut fed = match &state_path {
        Some(p) => FedState::load(p)?,
        None => {
            tracing::warn!("no data_dir configured — federation state is in-memory only");
            FedState::default()
        }
    };
    fed.apply_config_nodes(&config.nodes);
    tracing::info!(
        nodes = fed.nodes.len(),
        volumes = fed.volumes.len(),
        pools = config.pools.len(),
        "federation state loaded"
    );

    let listen = config.listen_addr.clone();
    let state = Arc::new(AppState {
        config,
        fed: RwLock::new(fed),
        events: RwLock::new(EventLog::new(4096)),
        state_path,
    });

    tokio::spawn(stormstorage::registry::run(state.clone()));

    let app = stormstorage::api::router(state.clone());
    let listener = tokio::net::TcpListener::bind(&listen).await?;
    tracing::info!(%listen, version = stormstorage::VERSION, "stormstorage control plane up");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down");
        })
        .await?;
    state.persist().await;
    Ok(())
}
