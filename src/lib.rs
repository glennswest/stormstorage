//! StormStorage — the storage control plane across Storm nodes and
//! clusters. Registry, pools, placement, distributed volumes assembled as
//! RAID1 over NVMe-TCP, leg moves, peer replication (tiering is design).
//! Never in the data path. See README.md and docs/architecture.md.

pub mod api;
pub mod backup;
pub mod components;
pub mod config;
pub mod engine;
pub mod events;
pub mod expand;
pub mod head;
pub mod inventory;
pub mod kube;
pub mod model;
pub mod orchestrate;
pub mod owner;
pub mod migrate;
pub mod placement;
pub mod rebalance;
pub mod refusal;
pub mod registry;
pub mod replicate;
pub mod seed;
pub mod store;
pub mod vmmigrate;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
/// The commit this binary was built from (build.rs, #120): `unknown` when
/// the tree had no `.git` and no `STORMSTORAGE_COMMIT`.
pub const COMMIT: &str = env!("STORMSTORAGE_COMMIT");
