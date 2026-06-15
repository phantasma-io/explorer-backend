//! Read-model queries for the HTTP API.
//!
//! Each function runs one API read query and returns a typed read-record; the
//! API layer maps those records to its wire DTOs. Keeping the SQL here gives the
//! crate a single data-access boundary (the API crate no longer embeds SQL) and
//! makes the read paths testable against a database without standing up the HTTP
//! service. Read-records are distinct from the write-side `*Upsert`/`*Record`
//! types: they are shaped for what an endpoint returns, not for ingestion.
//!
//! Layout: one submodule per API resource, mirroring how the HTTP handlers are
//! organised, so each read path stays small and independently testable. Every
//! public item is re-exported flat
//! (`explorer_db::list_chains`, ...) to keep the crate's existing flat surface,
//! matching the rest of the db crate (`pub use <module>::*`).

mod sort;
pub use sort::*;

mod address_stats;
pub use address_stats::*;

mod addresses;
pub use addresses::*;

mod blocks;
pub use blocks::*;

mod chains;
pub use chains::*;

mod circulating_supply;
pub use circulating_supply::*;

mod contract_method_histories;
pub use contract_method_histories::*;

mod contracts;
pub use contracts::*;

mod event_kinds;
pub use event_kinds::*;

mod history_prices;
pub use history_prices::*;

mod nfts;
pub use nfts::*;

mod oracles;
pub use oracles::*;

mod organizations;
pub use organizations::*;

mod overview_stats;
pub use overview_stats::*;

mod platforms;
pub use platforms::*;

mod rejected_transactions;
pub use rejected_transactions::*;

mod searches;
pub use searches::*;

mod series;
pub use series::*;

mod staking_stats;
pub use staking_stats::*;

mod events;
pub use events::*;

mod tokens;
pub use tokens::*;

mod transactions;
pub use transactions::*;

mod validator_kinds;
pub use validator_kinds::*;
