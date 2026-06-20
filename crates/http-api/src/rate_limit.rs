//! Inbound API rate limiting for the public HTTP API.
//!
//! This mirrors the Phantasma node's (`phantasma-rpc`) per-key tier limiter so the
//! explorer presents the same shape of protection on its own public surface:
//! - the API key rides in the `X-Api-Key` header (same as the node and our outbound
//!   RPC client);
//! - each known key is limited to its tier's requests-per-minute (a tier with
//!   `per_minute <= 0` is unlimited, for our own keys);
//! - an unknown/absent key falls back to a per-IP limit, but only when trusted
//!   proxies are configured (otherwise the real client IP is unknowable behind a
//!   proxy, so anonymous traffic is bounded only by the global concurrency cap);
//! - "keys-only" mode (`require_api_key`) rejects an unknown/absent key with 401;
//! - exceeding a window returns 429 with `Retry-After: 60`;
//! - an always-on global in-flight cap sheds load with 429 when saturated.
//!
//! The limiter is a single small `from_fn` layer with an in-process sliding-window
//! store (the same shared-state shape as the overview cache), deliberately avoiding
//! a heavyweight external limiter crate for a single-process, in-memory need.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use axum::extract::{ConnectInfo, Request, State};
use axum::http::{HeaderMap, Method};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use explorer_config::{RateLimitConfig, RateLimitTier};

use crate::ApiError;

/// HTTP header carrying the API key (matches the node and our RPC client).
pub const API_KEY_HEADER: &str = "X-Api-Key";
/// Sliding-window length and granularity, mirroring the node: a 1-minute window
/// split into 6 segments (10s each), so the count is smooth rather than a hard
/// fixed-window edge.
const WINDOW_SECONDS: u64 = 60;
const SEGMENTS: u64 = 6;
const SEGMENT_SECONDS: u64 = WINDOW_SECONDS / SEGMENTS;
/// `Retry-After` advertised on a 429 (seconds), matching the node.
const RETRY_AFTER_SECONDS: u64 = WINDOW_SECONDS;

/// A fixed-granularity sliding window: the trailing `SEGMENTS` time buckets. The
/// count over the window is the sum of buckets still inside it. O(SEGMENTS) per
/// request, no background sweeping (stale buckets are reset lazily on access).
#[derive(Debug)]
struct SlidingWindow {
    /// `(segment_index, count)` per slot; slot = `segment_index % SEGMENTS`.
    buckets: [(u64, u32); SEGMENTS as usize],
}

impl SlidingWindow {
    fn new() -> Self {
        Self {
            buckets: [(0, 0); SEGMENTS as usize],
        }
    }

    /// Admit a request observed in `now_segment` under `limit` requests/window.
    /// Returns true and records it when within the limit, false when it would
    /// exceed the trailing-window count (then nothing is recorded).
    fn try_admit(&mut self, now_segment: u64, limit: u32) -> bool {
        // Oldest segment index still inside the window ending at `now_segment`.
        let window_start = now_segment.saturating_sub(SEGMENTS - 1);
        let mut total: u32 = 0;
        for bucket in &mut self.buckets {
            if bucket.0 < window_start {
                // Rolled out of the window: reset lazily so it contributes nothing.
                *bucket = (0, 0);
            } else {
                total = total.saturating_add(bucket.1);
            }
        }
        if total >= limit {
            return false;
        }
        let slot = (now_segment % SEGMENTS) as usize;
        if self.buckets[slot].0 == now_segment {
            self.buckets[slot].1 = self.buckets[slot].1.saturating_add(1);
        } else {
            // Reused slot from an older segment (already counted as 0 above).
            self.buckets[slot] = (now_segment, 1);
        }
        true
    }
}

/// Shared rate-limiter state. `Clone` is cheap (an `Arc`), so it can be handed to
/// the axum layer as middleware state.
#[derive(Clone)]
pub struct RateLimiter {
    inner: Arc<RateLimiterInner>,
}

struct RateLimiterInner {
    enabled: bool,
    require_api_key: bool,
    per_ip_per_minute: u32,
    /// Flattened `key -> per_minute` map (`<= 0` means unlimited). Built once.
    key_limits: HashMap<String, i64>,
    /// Parsed trusted-proxy IPs; per-IP limiting is active only when non-empty.
    trusted_proxies: Vec<IpAddr>,
    /// Per-partition sliding windows, keyed `"apikey:<k>"` or `"ip:<addr>"`.
    windows: Mutex<HashMap<String, SlidingWindow>>,
    /// Total in-flight request bound (concurrency + queue). A simple atomic gate:
    /// over capacity we shed load with 429 rather than buffer unboundedly.
    in_flight: Arc<AtomicUsize>,
    capacity: usize,
    /// Monotonic clock origin for segment indexing.
    start: Instant,
}

/// Decrements the in-flight counter when the request future completes, whatever the
/// outcome. Held across `next.run(...)` so the gate reflects true concurrency.
struct InFlightGuard(Arc<AtomicUsize>);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl RateLimiter {
    /// Build a limiter from config. Invalid trusted-proxy entries are skipped with a
    /// warning rather than failing startup.
    pub fn new(config: &RateLimitConfig) -> Self {
        let key_limits = build_key_limits(&config.key_tiers);
        let trusted_proxies = config
            .trusted_proxies
            .iter()
            .filter_map(|raw| match raw.trim().parse::<IpAddr>() {
                Ok(ip) => Some(ip),
                Err(_) => {
                    tracing::warn!(proxy = %raw, "ignoring unparseable trusted proxy IP");
                    None
                }
            })
            .collect();
        // Total in-flight capacity = concurrency permits + queue depth. At least 1
        // so a misconfigured 0 never wedges the API entirely.
        let capacity = config
            .global_concurrent_limit
            .saturating_add(config.global_queue_limit)
            .max(1);
        Self {
            inner: Arc::new(RateLimiterInner {
                enabled: config.enabled,
                require_api_key: config.require_api_key,
                per_ip_per_minute: config.per_ip_per_minute,
                key_limits,
                trusted_proxies,
                windows: Mutex::new(HashMap::new()),
                in_flight: Arc::new(AtomicUsize::new(0)),
                capacity,
                start: Instant::now(),
            }),
        }
    }

    /// Whether the limiter does anything; lets the caller skip wiring the layer.
    pub fn is_enabled(&self) -> bool {
        self.inner.enabled
    }
}

/// Flatten configured tiers into `key -> per_minute`. Empty keys are ignored; on a
/// duplicate key the last tier wins (mirrors the node's `BuildKeyLimits`).
fn build_key_limits(tiers: &[RateLimitTier]) -> HashMap<String, i64> {
    let mut limits = HashMap::new();
    for tier in tiers {
        for key in &tier.keys {
            if key.is_empty() {
                continue;
            }
            limits.insert(key.clone(), tier.per_minute);
        }
    }
    limits
}

impl RateLimiterInner {
    /// Current segment index from the monotonic clock.
    fn current_segment(&self) -> u64 {
        self.start.elapsed().as_secs() / SEGMENT_SECONDS
    }

    /// Resolve the client IP: the socket peer, unless the peer is a configured
    /// trusted proxy and forwards an `X-Forwarded-For`, in which case the first hop
    /// (the original client) is used.
    fn resolve_client_ip(&self, peer: IpAddr, headers: &HeaderMap) -> IpAddr {
        if self.trusted_proxies.contains(&peer)
            && let Some(forwarded) = headers
                .get("x-forwarded-for")
                .and_then(|value| value.to_str().ok())
            && let Some(first) = forwarded.split(',').next()
            && let Ok(ip) = first.trim().parse::<IpAddr>()
        {
            return ip;
        }
        peer
    }

    /// Apply the per-partition sliding window. Returns true if admitted.
    fn admit_partition(&self, partition: String, limit: u32, now_segment: u64) -> bool {
        let mut windows = match self.windows.lock() {
            Ok(guard) => guard,
            // A poisoned lock means a prior panic mid-update; fail open rather than
            // wedge the whole API. The global cap is the backstop.
            Err(poisoned) => poisoned.into_inner(),
        };
        windows
            .entry(partition)
            .or_insert_with(SlidingWindow::new)
            .try_admit(now_segment, limit)
    }

    /// Decide whether to admit a request given its key and peer. `None` peer means
    /// connection info was unavailable (e.g. tests) — then per-IP limiting is
    /// skipped. The global in-flight cap is enforced separately by the caller.
    fn admit(&self, key: Option<&str>, peer: Option<IpAddr>, headers: &HeaderMap) -> bool {
        let now_segment = self.current_segment();
        // 1. A known API key is limited at its tier (<= 0 means unlimited).
        if let Some(key) = key
            && let Some(&per_minute) = self.key_limits.get(key)
        {
            if per_minute <= 0 {
                return true;
            }
            return self.admit_partition(format!("apikey:{key}"), per_minute as u32, now_segment);
        }
        // 2. Unknown/absent key → per-IP, but only behind trusted proxies (else the
        //    client IP is unknowable and all anonymous traffic shares the global cap).
        if self.trusted_proxies.is_empty() {
            return true;
        }
        let Some(peer) = peer else {
            return true;
        };
        let client_ip = self.resolve_client_ip(peer, headers);
        self.admit_partition(
            format!("ip:{client_ip}"),
            self.per_ip_per_minute,
            now_segment,
        )
    }

    /// Try to claim an in-flight slot. Returns a guard that releases it on drop, or
    /// `None` when at capacity (caller then sheds the request with 429).
    fn try_enter(&self) -> Option<InFlightGuard> {
        // Optimistic CAS loop: only claim a slot while strictly under capacity.
        let mut current = self.in_flight.load(Ordering::Acquire);
        loop {
            if current >= self.capacity {
                return None;
            }
            match self.in_flight.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(InFlightGuard(Arc::clone(&self.in_flight))),
                Err(observed) => current = observed,
            }
        }
    }
}

/// Axum middleware enforcing the rate limit. Wire with `from_fn_with_state`.
pub async fn rate_limit_middleware(
    State(limiter): State<RateLimiter>,
    request: Request,
    next: Next,
) -> Response {
    let inner = &limiter.inner;
    if !inner.enabled {
        return next.run(request).await;
    }

    // CORS preflight must pass through (no key on an OPTIONS), mirroring the node.
    if request.method() == Method::OPTIONS {
        return next.run(request).await;
    }

    let presented_key = request
        .headers()
        .get(API_KEY_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);

    // Keys-only mode: an unknown/absent key is rejected before any limiting.
    if inner.require_api_key {
        let known = presented_key
            .as_deref()
            .is_some_and(|key| inner.key_limits.contains_key(key));
        if !known {
            return ApiError::Unauthorized("API key required".to_owned()).into_response();
        }
    }

    // ConnectInfo is injected by `into_make_service_with_connect_info` (set in the
    // API binary); absent in tests/direct calls → no per-IP limiting then.
    let peer_ip = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| addr.ip());
    if !inner.admit(presented_key.as_deref(), peer_ip, request.headers()) {
        return ApiError::RateLimited {
            retry_after_secs: RETRY_AFTER_SECONDS,
        }
        .into_response();
    }

    // Global in-flight cap (held for the request's duration via the guard).
    let Some(_guard) = inner.try_enter() else {
        return ApiError::RateLimited {
            retry_after_secs: RETRY_AFTER_SECONDS,
        }
        .into_response();
    };

    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn tier(name: &str, per_minute: i64, keys: &[&str]) -> RateLimitTier {
        RateLimitTier {
            name: name.to_owned(),
            per_minute,
            keys: keys.iter().map(|key| (*key).to_owned()).collect(),
        }
    }

    fn config(tiers: Vec<RateLimitTier>) -> RateLimitConfig {
        RateLimitConfig {
            enabled: true,
            require_api_key: false,
            trusted_proxies: Vec::new(),
            per_ip_per_minute: 5,
            global_concurrent_limit: 100,
            global_queue_limit: 0,
            key_tiers: tiers,
        }
    }

    // A tier's keys each map to its per-minute limit; the last tier wins on a
    // duplicate key and empty keys are dropped (mirrors the node's BuildKeyLimits).
    #[test]
    fn builds_key_limits_with_last_tier_winning() {
        let limits = build_key_limits(&[
            tier("own", 20000, &["own-1", "own-2", ""]),
            tier("partner", 10000, &["partner-1"]),
            tier("override", 1, &["own-1"]),
        ]);
        assert_eq!(limits.get("own-1"), Some(&1)); // last tier wins
        assert_eq!(limits.get("own-2"), Some(&20000));
        assert_eq!(limits.get("partner-1"), Some(&10000));
        assert_eq!(limits.len(), 3); // empty key dropped
    }

    // The sliding window admits exactly `limit` requests inside one window and
    // rejects the next; once the window rolls past, capacity returns.
    #[test]
    fn sliding_window_enforces_limit_then_recovers() {
        let mut window = SlidingWindow::new();
        assert!(window.try_admit(0, 2));
        assert!(window.try_admit(0, 2));
        assert!(!window.try_admit(0, 2)); // 3rd in the same window is rejected
        // Advance a full window (SEGMENTS segments later) — old counts roll out.
        assert!(window.try_admit(SEGMENTS, 2));
        assert!(window.try_admit(SEGMENTS, 2));
        assert!(!window.try_admit(SEGMENTS, 2));
    }

    // A known key is limited at its tier; an unlimited tier (per_minute <= 0) is
    // always admitted regardless of volume.
    #[test]
    fn admits_known_key_by_tier_and_bypasses_unlimited() {
        let limiter = RateLimiter::new(&config(vec![
            tier("capped", 2, &["k-capped"]),
            tier("unlimited", 0, &["k-unlimited"]),
        ]));
        let headers = HeaderMap::new();
        // Capped key: 2 admitted, 3rd rejected (same window during the test run).
        assert!(limiter.inner.admit(Some("k-capped"), None, &headers));
        assert!(limiter.inner.admit(Some("k-capped"), None, &headers));
        assert!(!limiter.inner.admit(Some("k-capped"), None, &headers));
        // Unlimited key: never rejected.
        for _ in 0..1000 {
            assert!(limiter.inner.admit(Some("k-unlimited"), None, &headers));
        }
    }

    // With no trusted proxies, an anonymous (no/unknown key) request is not per-IP
    // limited — only the global cap applies — so admit() always returns true.
    #[test]
    fn anonymous_without_trusted_proxies_is_not_per_ip_limited() {
        let limiter = RateLimiter::new(&config(vec![tier("own", 2, &["k"])]));
        let headers = HeaderMap::new();
        let peer = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7));
        for _ in 0..100 {
            assert!(limiter.inner.admit(None, Some(peer), &headers));
            assert!(
                limiter
                    .inner
                    .admit(Some("unknown-key"), Some(peer), &headers)
            );
        }
    }

    // With trusted proxies configured, anonymous traffic is limited per client IP.
    #[test]
    fn anonymous_with_trusted_proxies_is_per_ip_limited() {
        let mut cfg = config(vec![]);
        cfg.per_ip_per_minute = 2;
        cfg.trusted_proxies = vec!["198.51.100.9".to_owned()];
        let limiter = RateLimiter::new(&cfg);
        let headers = HeaderMap::new();
        let client = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7));
        assert!(limiter.inner.admit(None, Some(client), &headers));
        assert!(limiter.inner.admit(None, Some(client), &headers));
        assert!(!limiter.inner.admit(None, Some(client), &headers)); // 3rd over per-IP=2
        // A different client IP has its own budget.
        let other = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 8));
        assert!(limiter.inner.admit(None, Some(other), &headers));
    }

    // The in-flight gate admits up to capacity and sheds beyond it; dropping a guard
    // returns the slot.
    #[test]
    fn in_flight_gate_caps_and_releases() {
        let mut cfg = config(vec![]);
        cfg.global_concurrent_limit = 2;
        cfg.global_queue_limit = 0;
        let limiter = RateLimiter::new(&cfg);
        let g1 = limiter.inner.try_enter();
        let g2 = limiter.inner.try_enter();
        assert!(g1.is_some() && g2.is_some());
        assert!(limiter.inner.try_enter().is_none()); // at capacity (2)
        drop(g1);
        assert!(limiter.inner.try_enter().is_some()); // slot freed
    }
}
