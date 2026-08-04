//! One-shot decoding of the boundary-era `legacy.raw.v1` event rows.
//!
//! 1,489,837 events restored from the C# era carry their original event bytes in
//! `raw_data` but no decoded payload (the C# importer left them as raw
//! envelopes). Every layout is fixed gen1/gen2 serialization, so the rows are
//! re-derivable offline from the bytes already in the row — no RPC, no resync.
//! The worker's `--decode-legacy-raw-once` walks them once: it adds the decoded
//! payload keys, fills `token_id` and re-points `contract_id` at the token
//! contract exactly where the live path does, and flips `payload_format` to
//! `legacy.decoded.v1`.
//!
//! Shape rules:
//! - Existing payload keys are PRESERVED verbatim (including C#-era quirks such
//!   as a raw numeric `event_kind`); only the in-payload `raw_data` duplicate is
//!   dropped (the column keeps the bytes) and decoded keys are added.
//! - Subobjects for kinds the live path decodes mirror `event_to_projection`'s
//!   shapes exactly (pinned by the mirror test below).
//! - Kinds only the C# backfill shaped (ChainSwap, organizations, files, sales,
//!   chain values, validator elections) mirror the C# `Block.cs` payload switch.
//! - Kinds C# never processed (ContractKill, OwnerAdded/Removed,
//!   ValidatorRemove) gain nothing: the format flip records they carry no
//!   payload decodable by the C# rules.
use super::*;

/// Everything decoding one raw event yields for the row update.
#[derive(Debug, Default)]
pub(crate) struct LegacyRawDecodeOutcome {
    /// Keys added to the stored payload object, in insertion order.
    pub payload_additions: Vec<(&'static str, Value)>,
    /// `events.token_id` value the live path would have set.
    pub token_id: Option<String>,
    /// Token/contract symbol to re-point `events.contract_id` at.
    pub contract_symbol: Option<String>,
    /// `events.target_address_id` source (C# sets it for validator elections).
    pub target_address: Option<String>,
}

/// Decode one `legacy.raw.v1` event into its row enrichment. Unknown kinds
/// refuse the run (the caller aborts): silently skipping one would leave
/// format-3 rows behind, and the tool's contract is zero remaining.
pub(crate) fn decode_legacy_raw_event(
    event_kind: &str,
    raw_data: &str,
    chain_name: &str,
    block_height: u64,
    tx_index: usize,
    event_index: usize,
) -> Result<LegacyRawDecodeOutcome, IngestionError> {
    let mut outcome = LegacyRawDecodeOutcome::default();

    if is_legacy_market_event_kind(event_kind) {
        let market_event =
            decode_legacy_market_event(block_height, tx_index, event_index, event_kind, raw_data)?;
        outcome.token_id = Some(market_event.market_id.clone());
        outcome.contract_symbol = Some(market_event.base_token.clone());
        outcome
            .payload_additions
            .push(("token_id", serde_json::json!(&market_event.market_id)));
        outcome.payload_additions.push((
            "market_event",
            serde_json::json!({
                "base_token": &market_event.base_token,
                "quote_token": &market_event.quote_token,
                "market_event_kind": &market_event.market_event_kind,
                "market_id": &market_event.market_id,
                "price": &market_event.price,
                "end_price": &market_event.end_price,
            }),
        ));
    } else if event_kind == "Infusion" {
        let infusion_event = decode_legacy_infusion_event(
            block_height,
            tx_index,
            event_index,
            event_kind,
            raw_data,
        )?;
        outcome.token_id = Some(infusion_event.token_id.clone());
        outcome.contract_symbol = Some(infusion_event.base_token.clone());
        outcome
            .payload_additions
            .push(("token_id", serde_json::json!(&infusion_event.token_id)));
        outcome.payload_additions.push((
            "infusion_event",
            serde_json::json!({
                "token_id": &infusion_event.token_id,
                "base_token": &infusion_event.base_token,
                "infused_token": &infusion_event.infused_token,
                "infused_value": &infusion_event.infused_value,
            }),
        ));
    } else if matches!(event_kind, "GasEscrow" | "GasPayment") {
        let gas_event =
            decode_legacy_gas_event(block_height, tx_index, event_index, event_kind, raw_data)?;
        let mut gas_payload = serde_json::json!({
            "price": &gas_event.price,
            "address": &gas_event.address,
        });
        if gas_event.amount != LEGACY_UNLIMITED_GAS_RAW {
            gas_payload["amount"] = serde_json::json!(&gas_event.amount);
        }
        outcome.payload_additions.push(("gas_event", gas_payload));
    } else if is_legacy_string_event_kind(event_kind) {
        let string_event =
            decode_legacy_string_event(block_height, tx_index, event_index, event_kind, raw_data)?;
        outcome.payload_additions.push((
            "string_event",
            serde_json::json!({ "string_value": string_event }),
        ));
    } else if event_kind == "ChainSwap" {
        let (hash, platform, chain) =
            decode_legacy_settle_event(block_height, tx_index, event_index, event_kind, raw_data)?;
        outcome.payload_additions.push((
            "transaction_settle_event",
            serde_json::json!({ "hash": hash, "platform": platform, "chain": chain }),
        ));
    } else if matches!(event_kind, "OrganizationAdd" | "OrganizationRemove") {
        let (organization, member_address) = decode_legacy_organization_event(
            block_height,
            tx_index,
            event_index,
            event_kind,
            raw_data,
        )?;
        outcome.payload_additions.push((
            "organization_event",
            serde_json::json!({ "organization": organization, "address": member_address }),
        ));
    } else if matches!(event_kind, "FileCreate" | "FileDelete") {
        let hash =
            decode_legacy_hash_event(block_height, tx_index, event_index, event_kind, raw_data)?;
        outcome
            .payload_additions
            .push(("hash_event", serde_json::json!({ "hash": hash })));
    } else if event_kind == "Crowdsale" {
        let (hash, sale_event_kind) =
            decode_legacy_sale_event(block_height, tx_index, event_index, event_kind, raw_data)?;
        outcome.payload_additions.push((
            "sale_event",
            serde_json::json!({ "hash": hash, "sale_event_kind": sale_event_kind }),
        ));
    } else if matches!(event_kind, "ValueCreate" | "ValueUpdate") {
        let (name, value) = decode_legacy_chain_value_event(
            block_height,
            tx_index,
            event_index,
            event_kind,
            raw_data,
        )?;
        // C# embeds the chain name inside the subobject (chainEntry.NAME).
        outcome.payload_additions.push((
            "chain_event",
            serde_json::json!({ "name": name, "value": value, "chain": chain_name }),
        ));
    } else if event_kind == "ValidatorElect" {
        let address =
            decode_legacy_address_event(block_height, tx_index, event_index, event_kind, raw_data)?;
        outcome.target_address = Some(address.clone());
        outcome
            .payload_additions
            .push(("address_event", serde_json::json!({ "address": address })));
    } else if event_kind == "TokenCreate" {
        // A raw gen1 TokenCreate carries just the symbol string; C#'s
        // non-extended arm adds no payload and only links the token contract.
        let symbol = decode_legacy_token_create_symbol(
            block_height,
            tx_index,
            event_index,
            event_kind,
            raw_data,
        )?;
        outcome.contract_symbol = Some(symbol);
    } else if matches!(
        event_kind,
        "ContractKill" | "OwnerAdded" | "OwnerRemoved" | "ValidatorRemove"
    ) {
        // C# never processed these kinds (its default switch arm); there is
        // nothing to add and the format flip is the whole conversion.
    } else {
        return Err(legacy_event_decode_error(
            block_height,
            tx_index,
            event_index,
            event_kind,
        ));
    }

    Ok(outcome)
}

/// gen1 `Hash` rendering: the bytes serialize as a var-length array and print
/// reversed (C# `Hash.ToString()` = `ReverseBytes(_data).ToHex()`).
fn legacy_read_hash(
    reader: &mut BinaryReader<'_>,
    block_height: u64,
    tx_index: usize,
    event_index: usize,
    event_kind: &str,
) -> Result<String, IngestionError> {
    let mut bytes = reader
        .read_var_bytes(MAX_ARRAY_SIZE)
        .map_err(|_| legacy_event_decode_error(block_height, tx_index, event_index, event_kind))?;
    bytes.reverse();
    Ok(encode_hex_upper(&bytes))
}

/// `TransactionSettleEventData`: Hash + platform + chain.
fn decode_legacy_settle_event(
    block_height: u64,
    tx_index: usize,
    event_index: usize,
    event_kind: &str,
    raw_data: &str,
) -> Result<(String, String, String), IngestionError> {
    let bytes =
        decode_legacy_event_bytes(block_height, tx_index, event_index, event_kind, raw_data)?;
    let mut reader = BinaryReader::new(&bytes);
    let hash = legacy_read_hash(&mut reader, block_height, tx_index, event_index, event_kind)?;
    let platform =
        legacy_read_string(&mut reader, block_height, tx_index, event_index, event_kind)?;
    let chain = legacy_read_string(&mut reader, block_height, tx_index, event_index, event_kind)?;
    legacy_assert_eof(reader, block_height, tx_index, event_index, event_kind)?;
    Ok((hash, platform, chain))
}

/// `OrganizationEventData`: organization name + member address.
fn decode_legacy_organization_event(
    block_height: u64,
    tx_index: usize,
    event_index: usize,
    event_kind: &str,
    raw_data: &str,
) -> Result<(String, String), IngestionError> {
    let bytes =
        decode_legacy_event_bytes(block_height, tx_index, event_index, event_kind, raw_data)?;
    let mut reader = BinaryReader::new(&bytes);
    let organization =
        legacy_read_string(&mut reader, block_height, tx_index, event_index, event_kind)?;
    let member_address =
        legacy_read_address(&mut reader, block_height, tx_index, event_index, event_kind)?;
    legacy_assert_eof(reader, block_height, tx_index, event_index, event_kind)?;
    Ok((organization, member_address))
}

/// Bare `Hash` payload (FileCreate/FileDelete).
fn decode_legacy_hash_event(
    block_height: u64,
    tx_index: usize,
    event_index: usize,
    event_kind: &str,
    raw_data: &str,
) -> Result<String, IngestionError> {
    let bytes =
        decode_legacy_event_bytes(block_height, tx_index, event_index, event_kind, raw_data)?;
    let mut reader = BinaryReader::new(&bytes);
    let hash = legacy_read_hash(&mut reader, block_height, tx_index, event_index, event_kind)?;
    legacy_assert_eof(reader, block_height, tx_index, event_index, event_kind)?;
    Ok(hash)
}

/// `SaleEventData`: Hash + `SaleEventKind` (enums serialize as var-ints; the
/// C# payload stores the enum NAME).
fn decode_legacy_sale_event(
    block_height: u64,
    tx_index: usize,
    event_index: usize,
    event_kind: &str,
    raw_data: &str,
) -> Result<(String, &'static str), IngestionError> {
    let bytes =
        decode_legacy_event_bytes(block_height, tx_index, event_index, event_kind, raw_data)?;
    let mut reader = BinaryReader::new(&bytes);
    let hash = legacy_read_hash(&mut reader, block_height, tx_index, event_index, event_kind)?;
    let sale_event_kind =
        match legacy_read_var_uint(&mut reader, block_height, tx_index, event_index, event_kind)? {
            0 => "Creation",
            1 => "SoftCap",
            2 => "HardCap",
            3 => "AddedToWhitelist",
            4 => "RemovedFromWhitelist",
            5 => "Distribution",
            6 => "Refund",
            7 => "PriceChange",
            8 => "Participation",
            _ => {
                return Err(legacy_event_decode_error(
                    block_height,
                    tx_index,
                    event_index,
                    event_kind,
                ));
            }
        };
    legacy_assert_eof(reader, block_height, tx_index, event_index, event_kind)?;
    Ok((hash, sale_event_kind))
}

/// `ChainValueEventData`: governance value name + big-integer value.
fn decode_legacy_chain_value_event(
    block_height: u64,
    tx_index: usize,
    event_index: usize,
    event_kind: &str,
    raw_data: &str,
) -> Result<(String, String), IngestionError> {
    let bytes =
        decode_legacy_event_bytes(block_height, tx_index, event_index, event_kind, raw_data)?;
    let mut reader = BinaryReader::new(&bytes);
    let name = legacy_read_string(&mut reader, block_height, tx_index, event_index, event_kind)?;
    let value =
        legacy_read_big_integer(&mut reader, block_height, tx_index, event_index, event_kind)?;
    legacy_assert_eof(reader, block_height, tx_index, event_index, event_kind)?;
    Ok((name, value))
}

/// Bare `Address` payload (ValidatorElect).
fn decode_legacy_address_event(
    block_height: u64,
    tx_index: usize,
    event_index: usize,
    event_kind: &str,
    raw_data: &str,
) -> Result<String, IngestionError> {
    let bytes =
        decode_legacy_event_bytes(block_height, tx_index, event_index, event_kind, raw_data)?;
    let mut reader = BinaryReader::new(&bytes);
    let address =
        legacy_read_address(&mut reader, block_height, tx_index, event_index, event_kind)?;
    legacy_assert_eof(reader, block_height, tx_index, event_index, event_kind)?;
    Ok(address)
}

/// A raw gen1 TokenCreate: a bare var-string token symbol.
fn decode_legacy_token_create_symbol(
    block_height: u64,
    tx_index: usize,
    event_index: usize,
    event_kind: &str,
    raw_data: &str,
) -> Result<String, IngestionError> {
    let bytes =
        decode_legacy_event_bytes(block_height, tx_index, event_index, event_kind, raw_data)?;
    let mut reader = BinaryReader::new(&bytes);
    let symbol = legacy_read_string(&mut reader, block_height, tx_index, event_index, event_kind)?;
    legacy_assert_eof(reader, block_height, tx_index, event_index, event_kind)?;
    Ok(symbol)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Real raw_data vectors lifted from the live DB's format-3 rows (event ids
    // in comments). Hash expectations are the byte-reversed upper hex of the
    // stored bytes (C# Hash.ToString()); the member/elected address expectation
    // is the genesis actor P2KFNXEbt…XV8 — a real DB address that fronts the
    // same era's governance rows — decoded from kind byte 0x01 (User) via the
    // SDK codec. A layout or rendering regression breaks these literals.
    #[test]
    fn decodes_the_csharp_only_raw_layouts() -> Result<(), Box<dyn std::error::Error>> {
        // ChainSwap, event 64062565.
        let settle = decode_legacy_raw_event(
            "ChainSwap",
            "203E6F0CD347FBD5D2E96C69974004A667C431F465963CB6379FB01B763DB6AD5F036E656F036E656F",
            "main",
            1,
            0,
            0,
        )?;
        assert_eq!(
            settle.payload_additions[0].1,
            serde_json::json!({
                "hash": "5FADB63D761BB09F37B63C9665F431C467A6044097696CE9D2D5FB47D30C6F3E",
                "platform": "neo",
                "chain": "neo"
            })
        );

        // OrganizationAdd, event 64062420.
        let organization = decode_legacy_raw_event(
            "OrganizationAdd",
            "0A76616C696461746F7273220100AC42F8B9E617BE1524893A76A1B0CCF937782023BA35301948A8F94CEBC67A1F",
            "main",
            1,
            0,
            0,
        )?;
        assert_eq!(
            organization.payload_additions[0].1,
            serde_json::json!({
                "organization": "validators",
                "address": "P2KFNXEbt65rQiWqogAzqkVGMqFirPmqPw8mQyxvRKsrXV8"
            })
        );

        // ValidatorElect, event 64062425 — same address bytes as above, and the
        // target address the C# path would set.
        let elect = decode_legacy_raw_event(
            "ValidatorElect",
            "220100AC42F8B9E617BE1524893A76A1B0CCF937782023BA35301948A8F94CEBC67A1F",
            "main",
            1,
            0,
            0,
        )?;
        assert_eq!(
            elect.target_address.as_deref(),
            Some("P2KFNXEbt65rQiWqogAzqkVGMqFirPmqPw8mQyxvRKsrXV8")
        );

        // Crowdsale, event 67815747: 33-byte hash blob + one var-int enum byte.
        let sale = decode_legacy_raw_event(
            "Crowdsale",
            "204DBC2216A0EA109AA3436D23A67F381AE98282C040E5F117CDFDA80108733D0400",
            "main",
            1,
            0,
            0,
        )?;
        assert_eq!(
            sale.payload_additions[0].1,
            serde_json::json!({
                "hash": "043D730801A8FDCD17F1E540C08282E91A387FA6236D43A39A10EAA01622BC4D",
                "sale_event_kind": "Creation"
            })
        );

        // FileCreate, event 67354204.
        let file = decode_legacy_raw_event(
            "FileCreate",
            "20FDEF3627CAAF3900B5F751831B608B241FAA7F4194726B28F433C91B7A2E3414",
            "main",
            1,
            0,
            0,
        )?;
        assert_eq!(
            file.payload_additions[0].1,
            serde_json::json!({
                "hash": "14342E7A1BC933F4286B7294417FAA1F248B601B8351F7B50039AFCA2736EFFD"
            })
        );

        // ValueCreate, event 64062421: nexus.protocol.version = 1, chain from
        // the row (C# stores chainEntry.NAME inside the subobject).
        let value = decode_legacy_raw_event(
            "ValueCreate",
            "166E657875732E70726F746F636F6C2E76657273696F6E020100",
            "main",
            1,
            0,
            0,
        )?;
        assert_eq!(
            value.payload_additions[0].1,
            serde_json::json!({
                "name": "nexus.protocol.version",
                "value": "1",
                "chain": "main"
            })
        );

        // TokenCreate, event 64062480: bare symbol, contract link only.
        let token_create = decode_legacy_raw_event("TokenCreate", "044D4B4E49", "main", 1, 0, 0)?;
        assert!(token_create.payload_additions.is_empty());
        assert_eq!(token_create.contract_symbol.as_deref(), Some("MKNI"));

        // ContractKill: C# never processed it — nothing decoded, nothing added.
        let kill = decode_legacy_raw_event("ContractKill", "00", "main", 1, 0, 0)?;
        assert!(kill.payload_additions.is_empty());
        assert!(kill.contract_symbol.is_none());

        Ok(())
    }

    // The tool's subobjects for kinds the LIVE path also decodes must stay
    // byte-equal to `event_to_projection`'s shapes: both sides decode the same
    // gas vector here, and the json values are compared directly. If the live
    // shape changes without the tool following (or vice versa), this fails.
    #[test]
    fn tool_shapes_mirror_the_live_builder() -> Result<(), Box<dyn std::error::Error>> {
        // GasEscrow vector from event 64062472 (bounded amount).
        let raw =
            "2202000D6E4079E36703EBD37C00722F5891D28B0E2811DC114B129215123ADCCE3605020100030F2700";
        let gas_event = decode_legacy_gas_event(1, 0, 0, "GasEscrow", raw)?;
        let mut live_shape = serde_json::json!({
            "price": &gas_event.price,
            "address": &gas_event.address,
        });
        if gas_event.amount != LEGACY_UNLIMITED_GAS_RAW {
            live_shape["amount"] = serde_json::json!(&gas_event.amount);
        }
        let tool = decode_legacy_raw_event("GasEscrow", raw, "main", 1, 0, 0)?;
        assert_eq!(tool.payload_additions[0].0, "gas_event");
        assert_eq!(tool.payload_additions[0].1, live_shape);
        Ok(())
    }
}
