//! NFTs list read model (the `/nfts` endpoint).
//!
//! A materialized `page` CTE selects/pages the visible (non-burned, non-nsfw,
//! non-blacklisted) NFT ids matching the filter, then the outer SELECT projects
//! each NFT with its contract, series, infusion, owners JSON, and infused-into
//! reference. The API maps the rows with `nft_from_row`. See the design note in
//! [`super::tokens`].
use crate::*;
use sqlx::postgres::PgRow;

/// Sortable columns for the NFTs list.
#[derive(Debug, Clone, Copy)]
pub enum NftOrderBy {
    Id,
    MintDate,
}

impl NftOrderBy {
    /// Parse the public `order_by` query param, defaulting to `id`.
    pub fn from_api_param(value: Option<&str>) -> Option<Self> {
        match value.unwrap_or("id") {
            "id" => Some(Self::Id),
            "mint_date" => Some(Self::MintDate),
            _ => None,
        }
    }

    /// The SQL column for this key (a fixed literal, never user input).
    fn column(self) -> &'static str {
        match self {
            Self::Id => "nft.id",
            Self::MintDate => "nft.mint_date_unix_seconds",
        }
    }
}

/// Filters for the NFTs list. `name`/`q` are already in `%...%` form; `status`
/// is one of `all`/`active`/`infused` (validated by the API layer).
#[derive(Debug, Clone, Copy)]
pub struct NftFilter<'a> {
    pub chain_id: i32,
    pub creator: Option<&'a str>,
    pub contract_hash: Option<&'a str>,
    pub name: Option<&'a str>,
    /// Whitespace-split `q` tokens; every token must match somewhere (C# multi-word
    /// AND so "Crown 2261" matches "Crown #2261").
    pub q_tokens: Option<&'a [String]>,
    pub symbol: Option<&'a str>,
    pub token_id: Option<&'a str>,
    pub series_id: Option<&'a str>,
    pub status: &'a str,
    pub owner: Option<&'a str>,
}

/// List visible NFTs for a chain matching the filter, ordered by the chosen
/// column then `nft.id`. The caller passes `limit + 1` to detect a next page.
pub async fn list_nfts(
    executor: impl sqlx::PgExecutor<'_>,
    filter: &NftFilter<'_>,
    order_by: NftOrderBy,
    direction: SortDirection,
    limit: i64,
    offset: i64,
) -> Result<Vec<PgRow>, DbError> {
    let dir = direction.as_sql();
    let column = order_by.column();
    let sql = format!(
        r#"
        WITH page AS MATERIALIZED (
            SELECT nft.id
            FROM nfts nft
            JOIN contracts contract ON contract.id = nft.contract_id
            LEFT JOIN addresses creator ON creator.id = nft.creator_address_id
            LEFT JOIN series series ON series.id = nft.series_id
            WHERE nft.chain_id = $1
              AND nft.nsfw = false
              AND (nft.burned IS NULL OR nft.burned = false)
              AND nft.blacklisted = false
              AND ($2::text IS NULL OR creator.address = $2)
              AND ($3::text IS NULL OR contract.hash = $3)
              AND ($4::text IS NULL OR nft.name ILIKE $4 OR nft.description ILIKE $4)
              AND ($5::text[] IS NULL OR (
                  SELECT bool_and(
                      nft.name ILIKE '%' || tok || '%'
                      OR nft.description ILIKE '%' || tok || '%'
                      OR nft.token_id = tok
                      OR nft.token_id ILIKE '%' || tok || '%'
                      OR contract.symbol ILIKE '%' || tok || '%'
                      OR series.name ILIKE '%' || tok || '%'
                      OR series.series_id ILIKE '%' || tok || '%'
                      OR (length(tok) >= 6 AND contract.hash ILIKE '%' || tok || '%')
                      OR creator.address = tok
                      OR EXISTS (
                          SELECT 1 FROM nft_ownerships o
                          JOIN addresses oa ON oa.id = o.address_id
                          WHERE o.nft_id = nft.id AND oa.address = tok AND o.amount > 0
                      )
                  )
                  FROM unnest($5::text[]) AS tok
              ))
              AND ($6::text IS NULL OR contract.symbol = $6)
              AND ($7::text IS NULL OR nft.token_id = $7)
              AND ($8::text IS NULL OR series.series_id = $8)
              AND ($9::text != 'active' OR nft.infused_into_id IS NULL)
              AND ($9::text != 'infused' OR nft.infused_into_id IS NOT NULL)
              AND (
                  $10::text IS NULL
                  OR EXISTS (
                      SELECT 1
                      FROM nft_ownerships ownership
                      JOIN addresses owner_address ON owner_address.id = ownership.address_id
                      WHERE ownership.nft_id = nft.id
                        AND owner_address.address = $10
                        AND ownership.amount > 0
                  )
              )
            ORDER BY {column} {dir}, nft.id {dir}
            LIMIT $11 OFFSET $12
        )
        SELECT
            nft.id,
            nft.token_id,
            chain.name AS chain_name,
            contract.name AS contract_name,
            contract.hash AS contract_hash,
            contract.symbol AS contract_symbol,
            creator.address AS creator_address,
            creator.address_name AS creator_onchain_name,
            nft.description,
            nft.name,
            nft.rom,
            nft.ram,
            nft.image,
            nft.video,
            nft.info_url,
            nft.mint_date_unix_seconds,
            nft.mint_number,
            nft.metadata,
            series.id AS series_db_id,
            series.series_id,
            series.current_supply AS series_current_supply,
            series.max_supply AS series_max_supply,
            series.name AS series_name,
            series.description AS series_description,
            series.image AS series_image,
            series.royalties::text AS series_royalties,
            series.type AS series_type,
            series.attr_type_1,
            series.attr_value_1,
            series.attr_type_2,
            series.attr_value_2,
            series.attr_type_3,
            series.attr_value_3,
            series.metadata AS series_metadata,
            series.series_created_unix_seconds,
            series_creator.address AS series_creator,
            series_contract.hash AS series_contract_hash,
            series_contract.symbol AS series_symbol,
            series_chain.name AS series_chain,
            series_mode.mode_name AS series_mode_name,
            infused_nft.token_id AS infused_into_token_id,
            infused_chain.name AS infused_into_chain,
            infused_contract.name AS infused_contract_name,
            infused_contract.hash AS infused_contract_hash,
            infused_contract.symbol AS infused_contract_symbol,
            (
                SELECT COALESCE(jsonb_agg(jsonb_build_object(
                    'address', owner_address.address,
                    'onchain_name', owner_address.address_name,
                    'amount', ownership.amount
                ) ORDER BY ownership.id), '[]'::jsonb)
                FROM nft_ownerships ownership
                JOIN addresses owner_address ON owner_address.id = ownership.address_id
                WHERE ownership.nft_id = nft.id
                  AND ownership.amount > 0
            ) AS owners_json,
            (
                SELECT COALESCE(jsonb_agg(jsonb_build_object(
                    'key', infusion.key,
                    'value', infusion.value
                ) ORDER BY infusion.id), '[]'::jsonb)
                FROM infusions infusion
                WHERE infusion.nft_id = nft.id
            ) AS infusion_json
        FROM page
        JOIN nfts nft ON nft.id = page.id
        JOIN chains chain ON chain.id = nft.chain_id
        JOIN contracts contract ON contract.id = nft.contract_id
        LEFT JOIN addresses creator ON creator.id = nft.creator_address_id
        LEFT JOIN series series ON series.id = nft.series_id
        LEFT JOIN addresses series_creator ON series_creator.id = series.creator_address_id
        LEFT JOIN contracts series_contract ON series_contract.id = series.contract_id
        LEFT JOIN chains series_chain ON series_chain.id = series_contract.chain_id
        LEFT JOIN series_modes series_mode ON series_mode.id = series.series_mode_id
        LEFT JOIN nfts infused_nft ON infused_nft.id = nft.infused_into_id
        LEFT JOIN chains infused_chain ON infused_chain.id = infused_nft.chain_id
        LEFT JOIN contracts infused_contract ON infused_contract.id = infused_nft.contract_id
        ORDER BY {column} {dir}, nft.id {dir}
        "#,
    );
    let rows = sqlx::query(&sql)
        .bind(filter.chain_id)
        .bind(filter.creator)
        .bind(filter.contract_hash)
        .bind(filter.name)
        .bind(filter.q_tokens)
        .bind(filter.symbol)
        .bind(filter.token_id)
        .bind(filter.series_id)
        .bind(filter.status)
        .bind(filter.owner)
        .bind(limit)
        .bind(offset)
        .fetch_all(executor)
        .await?;

    Ok(rows)
}
