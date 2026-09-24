//! StormStorage — the storage control plane across Storm nodes and
//! clusters. Registry, pools, placement, distributed volumes assembled as
//! RAID1 over NVMe-TCP, leg moves, peer replication (tiering is design).
//! Never in the data path. See README.md and docs/architecture.md.

pub mod api;
pub mod components;
pub mod config;
pub mod engine;
pub mod events;
pub mod model;
pub mod orchestrate;
pub mod placement;
pub mod registry;
pub mod replicate;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
