//! TTRS / 22series off-chain NFT metadata fetcher — a port of the C# `Nft.TTRS`
//! plugin. POSTs a batch of NFT `token_id`s to the 22series store API and maps each
//! returned item to an off-chain metadata record: the raw JSON (stored in
//! `nfts.offchain_api_response`) plus the materialized name/description/image/mint
//! display fields. External HTTP egress (the store API), like the price feed.
//!
//! The orchestration (which NFTs, batching, DB writes) lives in the driver; this module owns the
//! HTTP shape and field extraction only.

use std::collections::BTreeMap;

use explorer_db::NftOffchainUpsert;

/// Contract whose NFTs carry 22series off-chain metadata (C# `NtfHash = "TTRS"`).
pub const TTRS_CONTRACT_NAME: &str = "TTRS";
/// 22series store endpoint. Overridable via `EXPLORER_TTRS_API_URL` for tests.
pub const TTRS_API_URL: &str = "https://www.22series.com/api/store/nft";

/// Errors surfaced by the TTRS feed HTTP calls.
#[derive(Debug, thiserror::Error)]
pub enum TtrsFeedError {
    #[error("ttrs feed http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("ttrs feed client build error: {0}")]
    ClientBuild(reqwest::Error),
}

/// Response shape: `{ "<token_id>": { item fields..., "item_info": {...} }, ... }`.
/// Parsed as raw values so the full item JSON can be stored verbatim.
type StoreNftResponse = BTreeMap<String, serde_json::Value>;

/// POSTs `{"ids":[...]}` for one batch and maps each non-system item to an off-chain
/// record. Items typed "System object" (internal non-tradable NFTs the C# plugin
/// deletes) are skipped — we never delete here, we just don't write.
pub async fn fetch_offchain_batch(
    client: &reqwest::Client,
    url: &str,
    token_ids: &[String],
) -> Result<Vec<NftOffchainUpsert>, TtrsFeedError> {
    let body = serde_json::json!({ "ids": token_ids });
    let response: StoreNftResponse = client
        .post(url)
        .json(&body)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let records = response
        .into_iter()
        .filter_map(|(token_id, item)| map_store_item(token_id, &item))
        .collect();
    Ok(records)
}

/// Maps one 22series store item to an off-chain record, or `None` when the item is
/// skipped: not a JSON object, or an internal "System object" NFT (the C# plugin
/// deletes those; we leave them as-is and simply don't write). Shared by
/// `fetch_offchain_batch` and its tests so the mapping itself is covered directly.
fn map_store_item(token_id: String, item: &serde_json::Value) -> Option<NftOffchainUpsert> {
    if !item.is_object() {
        return None;
    }

    // Skip internal "System object" NFTs (C# deletes them; we leave them as-is).
    let item_type = item
        .get("type")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    if item_type.contains("System object") {
        return None;
    }

    let item_info = item.get("item_info");
    let name = item_info
        .and_then(|info| info.get("name_english"))
        .and_then(|value| value.as_str())
        .map(str::to_owned);
    let description = item_info
        .and_then(|info| info.get("description_english"))
        .and_then(|value| value.as_str())
        .map(str::to_owned);
    let image = item
        .get("img")
        .and_then(|value| value.as_str())
        .map(str::to_owned);
    let mint_number = item
        .get("mint")
        .and_then(serde_json::Value::as_i64)
        .and_then(|value| i32::try_from(value).ok());
    let mint_date_unix_seconds = item.get("timestamp").and_then(serde_json::Value::as_i64);

    Some(NftOffchainUpsert {
        token_id,
        offchain_api_response: item.to_string(),
        name,
        description,
        image,
        mint_number,
        mint_date_unix_seconds,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // The real `map_store_item` mapper must extract the item_info fields and raw JSON,
    // and skip a "System object" item entirely (no record produced).
    #[test]
    fn maps_items_and_skips_system_objects() {
        let json = r#"{
            "111": {
                "item": "1512",
                "img": "http://x/img?id=1512",
                "type": "Item",
                "timestamp": 1594730522,
                "mint": 183,
                "item_info": {
                    "name_english": "Akuna Rear Spoiler",
                    "description_english": "Make: Kaya"
                }
            },
            "222": { "type": "System object", "item_info": {} }
        }"#;
        let response: StoreNftResponse = serde_json::from_str(json).unwrap_or_default();

        // Exercise the SAME mapper fetch_offchain_batch uses, minus the HTTP call.
        let mut records: Vec<NftOffchainUpsert> = response
            .into_iter()
            .filter_map(|(token_id, item)| map_store_item(token_id, &item))
            .collect();
        records.sort_by(|left, right| left.token_id.cmp(&right.token_id));

        assert_eq!(records.len(), 1, "system object must be skipped");
        let record = &records[0];
        assert_eq!(record.token_id, "111");
        assert_eq!(record.name.as_deref(), Some("Akuna Rear Spoiler"));
        assert_eq!(record.description.as_deref(), Some("Make: Kaya"));
        assert_eq!(record.image.as_deref(), Some("http://x/img?id=1512"));
        assert_eq!(record.mint_number, Some(183));
        assert_eq!(record.mint_date_unix_seconds, Some(1594730522));
        assert!(
            record.offchain_api_response.contains("Akuna Rear Spoiler"),
            "raw item JSON must be stored verbatim"
        );
    }
}
