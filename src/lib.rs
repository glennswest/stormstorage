//! StormStorage — the storage control plane across Storm nodes and
//! clusters. Registry, pools, placement, distributed volumes, tiering.
//! Never in the data path. See docs/architecture.md.

pub mod api;
pub mod components;
pub mod config;
pub mod engine;
pub mod events;
pub mod model;
pub mod placement;
pub mod registry;
pub mod replicate;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
