//! CoinGecko price feed — a port of the C# `Price.CoinGecko` plugin.
//!
//! This is the ONLY non-RPC outbound network egress in the worker. It fetches:
//!   * live fiat prices  -> `tokens.price_*`        (via `fetch_live_prices`)
//!   * daily USD history -> `token_daily_prices`    (via `fetch_daily_close`)
//!
//! The orchestration (which days to backfill, GOATI peg, DB writes) lives in the
//! driver; this module owns the HTTP shapes and the hardcoded id map only.

use std::collections::BTreeMap;

use explorer_db::TokenPriceUpsert;

/// Public CoinGecko v3 API. Overridable via `EXPLORER_COINGECKO_BASE_URL` for tests.
pub const COINGECKO_BASE_URL: &str = "https://api.coingecko.com/api/v3";

/// Fiat currencies the explorer stores (the `tokens.price_*` columns), lower-cased
/// for the CoinGecko `vs_currencies` query. Mirrors C# `GetSupportedFiatSymbols`.
pub const FIAT_SYMBOLS: [&str; 8] = ["usd", "eur", "gbp", "jpy", "cad", "aud", "cny", "rub"];

/// Native token symbols that have a CoinGecko listing, in a stable order. GOATI is
/// intentionally absent: it has no listing and is pegged to SOUL's price (as in C#).
pub const PRICED_SYMBOLS: [&str; 9] = [
    "SOUL", "KCAL", "ETH", "USDC", "DAI", "USDT", "BNB", "NEO", "GAS",
];

/// CoinGecko id of `KCAL`. Its daily-history endpoint needs a paid plan, so the
/// daily backfill skips it (the C# plugin lists it in `inactiveCoins`). Live
/// `/simple/price` still returns it on the free tier.
pub const KCAL_COINGECKO_ID: &str = "phantasma-energy";

/// Maps a native token symbol to its CoinGecko coin id, or `None` when the token is
/// not listed and must be skipped. Hardcoded to match the C# plugin's switch exactly.
pub fn coingecko_id(symbol: &str) -> Option<&'static str> {
    match symbol.to_ascii_uppercase().as_str() {
        "SOUL" => Some("phantasma"),
        "KCAL" => Some(KCAL_COINGECKO_ID),
        "ETH" => Some("ethereum"),
        "USDC" => Some("usd-coin"),
        "DAI" => Some("dai"),
        "USDT" => Some("tether"),
        "BNB" => Some("binancecoin"),
        "NEO" => Some("neo"),
        "GAS" => Some("gas"),
        _ => None,
    }
}

/// Errors surfaced by the price feed HTTP calls.
#[derive(Debug, thiserror::Error)]
pub enum PriceFeedError {
    #[error("price feed http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("price feed client build error: {0}")]
    ClientBuild(reqwest::Error),
}

/// `/simple/price` shape: `{ "<coin id>": { "<fiat>": <value>, ... }, ... }`.
type SimplePriceResponse = BTreeMap<String, BTreeMap<String, f64>>;

/// `/coins/{id}/history` shape (only the field we need).
#[derive(Debug, serde::Deserialize)]
struct CoinHistory {
    market_data: Option<CoinHistoryMarketData>,
}

#[derive(Debug, serde::Deserialize)]
struct CoinHistoryMarketData {
    current_price: Option<BTreeMap<String, f64>>,
}

/// Builds the shared HTTP client (30s timeout, JSON). Separate so the driver builds
/// it once per run.
pub fn build_client() -> Result<reqwest::Client, PriceFeedError> {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .user_agent("phantasma-explorer-rs/price-feed")
        .build()
        .map_err(PriceFeedError::ClientBuild)
}

/// Attaches the optional CoinGecko demo API key header when configured.
fn with_api_key(
    request: reqwest::RequestBuilder,
    api_key: Option<&str>,
) -> reqwest::RequestBuilder {
    match api_key {
        Some(key) if !key.is_empty() => request.header("x-cg-demo-api-key", key),
        _ => request,
    }
}

/// Fetches live prices for every priced symbol across all fiat currencies in a single
/// `/simple/price` call and maps them back to native-symbol price rows. Symbols absent
/// from the response (or from the DB) are simply skipped.
pub async fn fetch_live_prices(
    client: &reqwest::Client,
    base_url: &str,
    api_key: Option<&str>,
) -> Result<Vec<TokenPriceUpsert>, PriceFeedError> {
    // Distinct CoinGecko ids for the priced symbols, preserving order.
    let mut ids = Vec::new();
    for symbol in PRICED_SYMBOLS {
        if let Some(id) = coingecko_id(symbol)
            && !ids.contains(&id)
        {
            ids.push(id);
        }
    }

    let url = format!("{base_url}/simple/price");
    let request = client.get(url).query(&[
        ("ids", ids.join(",")),
        ("vs_currencies", FIAT_SYMBOLS.join(",")),
    ]);
    let response: SimplePriceResponse = with_api_key(request, api_key)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let mut upserts = Vec::new();
    for symbol in PRICED_SYMBOLS {
        let Some(id) = coingecko_id(symbol) else {
            continue;
        };
        let Some(fiat) = response.get(id) else {
            continue;
        };
        upserts.push(TokenPriceUpsert {
            symbol: symbol.to_owned(),
            price_usd: fiat.get("usd").copied(),
            price_eur: fiat.get("eur").copied(),
            price_gbp: fiat.get("gbp").copied(),
            price_jpy: fiat.get("jpy").copied(),
            price_cad: fiat.get("cad").copied(),
            price_aud: fiat.get("aud").copied(),
            price_cny: fiat.get("cny").copied(),
            price_rub: fiat.get("rub").copied(),
        });
    }

    // GOATI has no CoinGecko listing; peg its live price to SOUL's across all fiat,
    // matching C# (the daily-history backfill pegs GOATI to SOUL's USD too). Without
    // this, GOATI's live price would stay stale.
    if let Some(soul) = upserts
        .iter()
        .find(|upsert| upsert.symbol == "SOUL")
        .cloned()
    {
        upserts.push(TokenPriceUpsert {
            symbol: "GOATI".to_owned(),
            ..soul
        });
    }

    Ok(upserts)
}

/// Outcome of a single `/coins/{id}/history` fetch: a USD close, an explicit "no data"
/// (market_data missing), or a soft stop when the API rate-limited (HTTP 429) — the
/// caller stops the backfill for this run and resumes next time, like the C# plugin.
/// Other non-success statuses are surfaced as errors, not a soft stop.
pub enum DailyCloseOutcome {
    Price(f64),
    Missing,
    RateLimited,
}

/// Fetches one day's USD close for a coin id. `date` must be `DD-MM-YYYY` (CoinGecko's
/// required format). HTTP 429 maps to `RateLimited` so the caller stops gracefully;
/// other non-success statuses are surfaced as errors instead of soft-stopping.
pub async fn fetch_daily_close(
    client: &reqwest::Client,
    base_url: &str,
    api_key: Option<&str>,
    coin_id: &str,
    date_ddmmyyyy: &str,
) -> Result<DailyCloseOutcome, PriceFeedError> {
    let url = format!("{base_url}/coins/{coin_id}/history");
    let request = client.get(url).query(&[("date", date_ddmmyyyy)]);
    let response = with_api_key(request, api_key).send().await?;

    // CoinGecko rate-limits the free tier with 429; treat only that as a soft stop
    // so the backfill resumes next run. Any other non-success (401 bad key, 404,
    // 5xx, ...) is a real error and is surfaced rather than silently soft-stopping
    // the daily backfill forever.
    if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return Ok(DailyCloseOutcome::RateLimited);
    }

    let history: CoinHistory = response.error_for_status()?.json().await?;
    let usd = history
        .market_data
        .and_then(|market| market.current_price)
        .and_then(|prices| prices.get("usd").copied());

    Ok(match usd {
        Some(price) => DailyCloseOutcome::Price(price),
        None => DailyCloseOutcome::Missing,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // The id map must match the C# plugin exactly: the known listings resolve and
    // everything else (incl. GOATI, which is pegged, not fetched) is skipped.
    #[test]
    fn coingecko_id_maps_known_symbols_and_skips_others() {
        assert_eq!(coingecko_id("SOUL"), Some("phantasma"));
        assert_eq!(coingecko_id("soul"), Some("phantasma"));
        assert_eq!(coingecko_id("KCAL"), Some("phantasma-energy"));
        assert_eq!(coingecko_id("NEO"), Some("neo"));
        assert_eq!(coingecko_id("GOATI"), None);
        assert_eq!(coingecko_id("UNKNOWN"), None);
    }

    // Every priced symbol must have an id (the daily backfill relies on this).
    #[test]
    fn all_priced_symbols_resolve() {
        for symbol in PRICED_SYMBOLS {
            assert!(coingecko_id(symbol).is_some(), "missing id for {symbol}");
        }
    }
}
