//! Phase 1 market-data core: Binance USD-M public streams, local L2 synchronization,
//! recorder and deterministic replay. Public data only; no trading capability.

pub mod binance;
pub mod book;
pub mod event;
pub mod live;
pub mod pipeline;
pub mod recorder;
