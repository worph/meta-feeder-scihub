//! Sci-Hub bridge — DOI-only resolver via a configurable mirror list.
//!
//! Sci-Hub has no public search API; it's a DOI → PDF resolver. The plugin
//! reflects that shape:
//!
//! - `handle_query` returns a single synthetic record IFF the user's raw
//!   query carries a `doi:<value>` filter (`type:paper doi:10.1038/foo`).
//!   Free-text queries return empty — keyword search is covered by arxiv /
//!   pubmed. Without `doi:`, the plugin contributes nothing.
//! - `compute_outcomes` walks the mirror list (configured via the
//!   dashboard / `gateway-config.json` `plugins.scihub.mirrors`), scrapes
//!   the resolver page for the PDF embed/iframe URL, fetches the PDF, hashes
//!   it. First mirror that yields a `%PDF`-prefixed body wins; mirrors
//!   that 404 or return "not found" stub pages fall through to the next.
//!
//! # Opt-in only
//!
//! Sci-Hub lives in [`super::OPT_IN_UPSTREAMS`] (default-OFF). The
//! binary instantiates it on every startup like the other plugins, but
//! it stays **disabled** in [`crate::enabled::EnabledState`]
//! until the operator flips the toggle through the dashboard's
//! Tech → Gateway panel (or `PUT /api/gateway/plugins/scihub`). The
//! state is persisted to `gateway-enabled.json` so the choice survives
//! restarts.
//!
//! The plugin also soft-skips entirely (never enters the registry) when
//! no mirrors are supplied via the dashboard / `gateway-config.json`
//! `plugins.scihub.mirrors` — without a mirror list there's
//! nothing to resolve DOIs against. Operators set the mirrors once and
//! then use the UI toggle for everything else; the gateway peer
//! auto-stores every fetched PDF into its own meta-core (so the peer
//! becomes a regular meta-core host for the CID), so operators take
//! responsibility for whatever legal exposure that implies in their
//! jurisdiction.
//!
//! # DOI as a query input, not free text
//!
//! The trait's `handle_query(q: &str, ...)` receives the raw query text
//! after gateway-filter stripping (see
//! `crate::query::strip_gateway_filters_from_raw`).
//! `extract_doi_filter` scans the remaining whitespace-tokens for a
//! `doi:<value>` prefix; nothing else triggers a record. This is the
//! mirror image of the gateway-filter gate: even within the gateway tier,
//! sci-hub stays silent unless the user explicitly named a DOI.
//!
//! See `docs/gateway-feature.md` §4 for the v0 plugin role.

use std::collections::BTreeMap;
use std::path::Path;

use async_trait::async_trait;
use tracing::debug;

use meta_feeder_sdk::common;
use meta_feeder_sdk::cache::MidhashCache;
#[cfg(test)]
use meta_feeder_sdk::plugin::HashKind;
use meta_feeder_sdk::plugin::{upstream_id_field, ConfigError, FeederPlugin, GatewayQuery, HashOutcome};
use meta_feeder_sdk::types::{DiscoveryRecord, GatewayError, PluginHealth};

/// HTTP timeout per upstream call. Sci-Hub mirrors are often slow on the
/// first request (TLS handshakes, Cloudflare warmups) but the PDF stream
/// itself is fast; 60 s gives the cold-path enough room without holding
/// dispatcher resources indefinitely if a mirror hangs.
const HTTP_TIMEOUT_SECS: u64 = 60;

/// Polite identification — mirrors often gate aggressive abuse via UA
/// patterns, so identifying ourselves consistently keeps us out of the
/// generic-bot bucket.
const USER_AGENT: &str = concat!(
    "meta-share/",
    env!("CARGO_PKG_VERSION"),
    " (gateway:scihub)"
);

/// Sci-Hub gateway plugin. Cheap to construct; `configure()` reads the
/// mirror list from env (unless pre-set by a test constructor) and opens
/// the per-plugin redb cache.
pub struct ScihubPlugin {
    http: reqwest::Client,
    /// Operator-supplied mirror bases, scheme included, trailing slash
    /// stripped (e.g. `https://sci-hub.se`, `https://sci-hub.ru`). Walked
    /// in order on each `compute_outcomes` miss.
    mirrors: Vec<String>,
    cache: Option<MidhashCache>,
}

impl Default for ScihubPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl ScihubPlugin {
    pub fn new() -> Self {
        // Sci-Hub mirrors often redirect resolver → host-specific PDF
        // subdomain; follow up to a small bound. The default reqwest limit
        // (10) is fine but we set it explicitly so a future bump to a
        // stricter default doesn't silently break us.
        let http = common::build_http_client(
            HTTP_TIMEOUT_SECS,
            USER_AGENT,
            Some(reqwest::redirect::Policy::limited(10)),
        );
        Self {
            http,
            mirrors: Vec::new(),
            cache: None,
        }
    }

    /// Set the mirror list from the gateway config (or tests). Each entry
    /// is normalised: trimmed, empties dropped, `https://` prepended when
    /// the operator omitted the scheme, and a trailing slash stripped so
    /// the `{base}/{doi}` join is unambiguous. Production path is the
    /// gateway config loader at `instantiate()` time; tests use
    /// [`with_mirrors_for_test`].
    pub fn set_mirrors(&mut self, mirrors: Vec<String>) {
        self.mirrors = normalize_mirrors(mirrors);
    }

    /// Test-only constructor: primes the mirror list so `configure()`
    /// skips the env-var read (mutating env vars in a parallel test
    /// runner is unsafe — other tests racing on the same var would see
    /// torn reads).
    #[cfg(test)]
    pub fn with_mirrors_for_test(mirrors: Vec<String>) -> Self {
        let mut p = Self::new();
        p.mirrors = mirrors;
        p
    }

    fn cache(&self) -> Result<&MidhashCache, GatewayError> {
        common::require_cache(self.cache.as_ref(), "scihub")
    }

    /// Fetch the mirror's DOI resolver page. Sci-Hub mirrors put the DOI
    /// directly in the URL path: `https://<mirror>/<doi>` — the DOI's
    /// `/` becomes part of the path. No percent-encoding needed for the
    /// standard DOI alphabet (alnum + `.` + `/` + `-` + `_`).
    async fn fetch_resolver_html(&self, mirror: &str, doi: &str) -> Result<String, GatewayError> {
        let url = format!("{}/{doi}", mirror.trim_end_matches('/'));
        common::fetch_text(&self.http, &url).await
    }

    /// GET an absolute or already-resolved PDF URL. Body is verified to
    /// start with `%PDF` upstream of this in `try_fetch_from_mirror` —
    /// here we just hand back the raw bytes.
    async fn fetch_pdf_bytes(&self, url: &str) -> Result<bytes::Bytes, GatewayError> {
        common::fetch_bytes(&self.http, url).await
    }

    /// Try one mirror end-to-end: resolver → PDF URL extraction → PDF
    /// fetch → magic-byte check. Returns the PDF bytes on success, or a
    /// `GatewayError` the caller's outer loop uses to decide whether to
    /// try the next mirror.
    async fn try_fetch_from_mirror(
        &self,
        mirror: &str,
        doi: &str,
    ) -> Result<bytes::Bytes, GatewayError> {
        let html = self.fetch_resolver_html(mirror, doi).await?;
        let Some(pdf_url) = extract_pdf_url_from_html(&html, mirror) else {
            // Sci-Hub mirrors return 200 with a "paper not found" stub
            // when the DOI is unknown. The resolver HTML has no PDF
            // embed in that case, so missing extract = upstream
            // not-found. Caller's loop falls through to the next
            // mirror.
            debug!(
                target: "meta-share::gateway",
                upstream = "scihub",
                mirror,
                doi,
                "mirror returned resolver page without a PDF link"
            );
            return Err(GatewayError::NotFound);
        };
        let bytes = self.fetch_pdf_bytes(&pdf_url).await?;
        // Sci-Hub mirrors occasionally serve an HTML challenge / takedown
        // notice with 200 status instead of a PDF. Cheapest defense: the
        // `%PDF` magic. Permanent so we don't burn retries on a broken
        // mirror response shape (the loop falls through to the next
        // mirror via the outer error-discriminate match).
        if !bytes.starts_with(b"%PDF") {
            return Err(GatewayError::Permanent(format!(
                "mirror {mirror} returned non-PDF body for DOI {doi}"
            )));
        }
        Ok(bytes)
    }
}

#[async_trait]
impl FeederPlugin for ScihubPlugin {
    fn upstream_id(&self) -> &'static str {
        "scihub"
    }

    fn configure(&mut self, cache_dir: &Path) -> Result<(), ConfigError> {
        // The mirror list comes from `gateway-config.json` (pre-applied and
        // normalised in `plugins::instantiate` via `set_mirrors`) or, in
        // tests, the constructor. Empty → MissingConfig, which the registry
        // builder soft-skips with a warning (same pattern as giphy without
        // an api key).
        if self.mirrors.is_empty() {
            return Err(ConfigError::MissingConfig {
                plugin: "scihub",
                what: "mirrors (set them in the dashboard or gateway-config.json plugins.scihub.mirrors)",
            });
        }
        self.cache = Some(common::open_midhash_cache(cache_dir, "scihub")?);
        Ok(())
    }

    async fn handle_query(
        &self,
        query: &GatewayQuery,
        _max_results: usize,
    ) -> Result<Vec<DiscoveryRecord>, GatewayError> {
        // Layer A early-return: Sci-Hub only serves `document` /
        // `paper`. A non-paper filter can never match — skip the
        // DOI extraction entirely.
        if !meta_feeder_sdk::query_eval::query_accepts_plugin(
            query,
            self.served_file_types(),
            self.served_content_kinds(),
        ) {
            return Ok(Vec::new());
        }
        // Sci-Hub is a DOI resolver, not a search engine. We answer only
        // when the user's raw query carries a `doi:<value>` token. This
        // is the per-plugin gate that pairs with the gateway tier's
        // outer `type:` filter gate — even a query like `type:paper`
        // gets nothing from us without an explicit DOI.
        //
        // `extract_doi_filter` parses `doi:` tokens out of free text;
        // pass the raw text so the existing extractor behavior is
        // preserved exactly. A later pass could pivot to consuming
        // `query.filters.get("doi")` directly.
        let q = query.raw_text.as_str();
        let Some(doi) = extract_doi_filter(q) else {
            return Ok(Vec::new());
        };
        if !is_valid_doi(&doi) {
            // The user typed `doi:something-not-a-doi`. Don't synthesize
            // a record we'll fail to resolve later — return empty so the
            // UI shows no result rather than a broken "add" button.
            debug!(
                target: "meta-share::gateway",
                upstream = "scihub",
                value = doi.as_str(),
                "rejected doi: filter value (not DOI-shaped)"
            );
            return Ok(Vec::new());
        }
        Ok(vec![into_discovery_record(&doi)])
    }

    async fn compute_outcomes(&self, record_id: &str) -> Result<Vec<HashOutcome>, GatewayError> {
        let cache = self.cache()?;
        if let Some(hit) = common::cached_outcome(cache, record_id, "scihub")? {
            return Ok(hit);
        }

        // Defensive: a remote peer could pass us any record_id. Verify
        // the shape before walking mirrors so a malformed value doesn't
        // burn a request budget against every mirror in the list.
        if !is_valid_doi(record_id) {
            return Err(GatewayError::Permanent(format!(
                "scihub record_id `{record_id}` is not DOI-shaped (expected 10.NNNN/...)"
            )));
        }

        // Walk mirrors in operator-specified order. First one to yield
        // PDF bytes wins. Track the most informative error to surface if
        // every mirror fails — `Permanent` and `Transient` outrank
        // `NotFound` because they describe a *real* failure (auth wall,
        // server error) whereas `NotFound` may just mean "this mirror
        // doesn't have this DOI".
        let mut best_err: Option<GatewayError> = None;
        for mirror in &self.mirrors {
            match self.try_fetch_from_mirror(mirror, record_id).await {
                Ok(bytes) => {
                    let cid = meta_feeder_sdk::hash::compute_ipfs_cid(&bytes);
                    common::store_midhash(cache, record_id, "scihub", &cid);
                    let record = into_discovery_record(record_id);
                    return Ok(common::single_outcome(
                        cid,
                        bytes,
                        record,
                        Some("pdf".to_string()),
                    ));
                }
                Err(e) => {
                    best_err = Some(merge_err(best_err, e));
                }
            }
        }
        Err(best_err.unwrap_or(GatewayError::NotFound))
    }

    fn health(&self) -> PluginHealth {
        match (self.cache.is_some(), self.mirrors.is_empty()) {
            (true, false) => PluginHealth::Ok,
            (false, _) => PluginHealth::Degraded {
                reason: "configure() not yet called".to_string(),
            },
            (true, true) => PluginHealth::Degraded {
                reason: "no mirrors configured — set plugins.scihub.mirrors".to_string(),
            },
        }
    }

    fn served_file_types(&self) -> &'static [&'static str] {
        &["document"]
    }

    fn served_content_kinds(&self) -> &'static [&'static str] {
        &["paper"]
    }
}

/// Pick the "more informative" of two errors when walking the mirror list.
/// `Permanent` / `Transient` / `RateLimited` win over `NotFound` because
/// they describe an actual failure mode the operator can act on; among
/// non-NotFound errors, the latest wins (mirrors are tried in order,
/// later mirrors are typically the operator's fallback choices).
fn merge_err(prev: Option<GatewayError>, new: GatewayError) -> GatewayError {
    match (prev, &new) {
        (None, _) => new,
        (Some(p), GatewayError::NotFound) => p,
        _ => new,
    }
}

/// Normalize a mirror list into canonical base URLs. Normalization: trim,
/// drop empties, prepend `https://` when the operator omitted the scheme,
/// strip a trailing slash so the `{base}/{doi}` join is unambiguous.
fn normalize_mirrors(mirrors: Vec<String>) -> Vec<String> {
    mirrors
        .iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| {
            let with_scheme = if s.starts_with("http://") || s.starts_with("https://") {
                s.to_string()
            } else {
                format!("https://{s}")
            };
            with_scheme.trim_end_matches('/').to_string()
        })
        .collect()
}

/// Find a `doi:<value>` (or `doi=<value>`) token in the raw query.
/// Whitespace-split, case-insensitive prefix match — the same shape
/// `is_gateway_filter_token` uses for `fileType:` / `contentKind:`
/// over in `crate::query`. The returned value preserves the user's
/// original case (DOIs are
/// case-insensitive by spec but some mirrors differ on the suffix, so
/// don't normalize).
fn extract_doi_filter(q: &str) -> Option<String> {
    for tok in q.split_whitespace() {
        for sep in [':', '='] {
            let prefix = format!("doi{sep}");
            if tok.len() <= prefix.len() {
                continue;
            }
            let head = &tok[..prefix.len()];
            if head.eq_ignore_ascii_case(&prefix) {
                let value = &tok[prefix.len()..];
                if !value.is_empty() {
                    return Some(value.to_string());
                }
            }
        }
    }
    None
}

/// Validate a DOI string against the canonical shape `10.<reg>/<suffix>`
/// where `<reg>` is 4+ ASCII digits (registrant code per the DOI
/// Handbook) and `<suffix>` is any non-empty non-whitespace string. The
/// stricter "suffix MUST NOT contain `:` `?` `#`" rule from RFC-flavored
/// guides is intentionally relaxed — real-world DOIs use those characters
/// in publisher-defined suffix conventions.
fn is_valid_doi(s: &str) -> bool {
    let Some(rest) = s.strip_prefix("10.") else {
        return false;
    };
    let Some(slash) = rest.find('/') else {
        return false;
    };
    let registrant = &rest[..slash];
    if registrant.len() < 4 || !registrant.chars().all(|c| c.is_ascii_digit()) {
        return false;
    }
    let suffix = &rest[slash + 1..];
    !suffix.is_empty() && !suffix.chars().any(char::is_whitespace)
}

/// Substring-scan the resolver HTML for a PDF embed/iframe URL.
/// Sci-Hub layouts vary across mirrors (`<iframe id="pdf" src="…">`,
/// `<embed type="application/pdf" src="…">`, sometimes wrapped in
/// `<div id="article">`), but all of them put the PDF URL in a `src="…"`
/// attribute whose value ends in `.pdf` (optionally with `?token=…` or
/// `#page=…` trailing). The cheapest reliable heuristic is: walk every
/// `src="…"` token, pick the first whose value looks like a PDF, resolve
/// it against the mirror base.
///
/// Returns `None` when no PDF-looking src is found — caller treats that
/// as upstream not-found for this mirror.
fn extract_pdf_url_from_html(html: &str, mirror: &str) -> Option<String> {
    for quote in ['"', '\''] {
        let pat_with_eq = format!("src={quote}");
        let mut search_from = 0;
        while let Some(rel) = html[search_from..].find(&pat_with_eq) {
            let url_start = search_from + rel + pat_with_eq.len();
            let Some(end_off) = html[url_start..].find(quote) else {
                // Malformed attribute (unclosed quote). Stop scanning
                // this quote variant and try the other.
                break;
            };
            let url = &html[url_start..url_start + end_off];
            if url_looks_like_pdf(url) {
                return Some(resolve_url(url, mirror));
            }
            search_from = url_start + end_off + 1;
        }
    }
    None
}

fn url_looks_like_pdf(url: &str) -> bool {
    let lower = url.to_ascii_lowercase();
    lower.ends_with(".pdf") || lower.contains(".pdf?") || lower.contains(".pdf#")
}

/// Resolve a (possibly relative) URL against the mirror base.
/// Three shapes Sci-Hub mirrors emit in practice:
///   - absolute (`https://dl.host.tld/foo.pdf`) → use as-is
///   - protocol-relative (`//dl.host.tld/foo.pdf`) → inherit mirror's scheme
///   - root-relative (`/downloads/foo.pdf`) → join against mirror
fn resolve_url(url: &str, mirror: &str) -> String {
    let url = url.trim();
    if url.starts_with("http://") || url.starts_with("https://") {
        return url.to_string();
    }
    if let Some(rest) = url.strip_prefix("//") {
        let scheme = if mirror.starts_with("http://") {
            "http"
        } else {
            "https"
        };
        return format!("{scheme}://{rest}");
    }
    if url.starts_with('/') {
        return format!("{}{url}", mirror.trim_end_matches('/'));
    }
    format!("{}/{url}", mirror.trim_end_matches('/'))
}

/// Build the wire-shape `DiscoveryRecord` for a DOI. The same shape is
/// returned at search time (synthetic, no upstream call) and from a
/// fresh `compute_outcomes` (so meta-core's auto-store sees the same
/// fields it would have seen via search → add). Required fields per
/// gateway-feature.md §6.4: `title`, `type`, `sourceUrl`, `fileName`,
/// canonical `<upstream_id>id` (`scihubid` here). The semantic `doi`
/// alias is emitted alongside — operator UIs can prefer it for display.
fn into_discovery_record(doi: &str) -> DiscoveryRecord {
    let mut fields: BTreeMap<String, String> = BTreeMap::new();
    fields.insert("title".to_string(), format!("DOI: {doi}"));
    fields.insert("fileType".to_string(), "document".to_string());
    fields.insert("contentKind".to_string(), "paper".to_string());
    fields.insert("format".to_string(), "pdf".to_string());
    fields.insert("sourceUrl".to_string(), format!("https://doi.org/{doi}"));
    fields.insert(
        "fileName".to_string(),
        format!("scihub-{}.pdf", safe_filename(doi)),
    );
    fields.insert("doi".to_string(), doi.to_string());
    fields.insert(upstream_id_field("scihub"), doi.to_string());
    DiscoveryRecord {
        upstream_id: "scihub".to_string(),
        record_id: doi.to_string(),
        fields,
    }
}

/// Slugify a DOI for use in a filesystem-safe filename. DOIs legitimately
/// contain `/`, which would create nested paths in meta-core's
/// `plugin/share/` directory. Replace anything outside `[A-Za-z0-9.-_]`
/// with `_` — collision-free in practice because the DOI registrant +
/// suffix structure is unique.
fn safe_filename(doi: &str) -> String {
    doi.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    // -- Unit tests for the pure helpers --------------------------------

    #[test]
    fn extract_doi_filter_finds_token() {
        assert_eq!(
            extract_doi_filter("type:paper doi:10.1038/s41586-021-03819-2").as_deref(),
            Some("10.1038/s41586-021-03819-2")
        );
        // Equals form.
        assert_eq!(
            extract_doi_filter("doi=10.1126/science.aam9744").as_deref(),
            Some("10.1126/science.aam9744")
        );
        // Case-insensitive prefix.
        assert_eq!(
            extract_doi_filter("DOI:10.1038/foo").as_deref(),
            Some("10.1038/foo")
        );
        // No DOI token.
        assert_eq!(extract_doi_filter("type:paper crispr"), None);
        // Bare `doi:` is ignored (no value).
        assert_eq!(extract_doi_filter("doi:"), None);
    }

    #[test]
    fn is_valid_doi_recognises_canonical_shape() {
        assert!(is_valid_doi("10.1038/s41586-021-03819-2"));
        assert!(is_valid_doi("10.1126/science.aam9744"));
        assert!(is_valid_doi("10.12345/long-suffix/with/slashes"));
        // Bad: missing 10. prefix.
        assert!(!is_valid_doi("doi:10.1038/foo"));
        // Bad: registrant < 4 digits.
        assert!(!is_valid_doi("10.12/foo"));
        // Bad: registrant non-numeric.
        assert!(!is_valid_doi("10.abcd/foo"));
        // Bad: no suffix.
        assert!(!is_valid_doi("10.1038/"));
        // Bad: whitespace in suffix.
        assert!(!is_valid_doi("10.1038/foo bar"));
    }

    #[test]
    fn normalize_mirrors_normalises_and_strips_trailing_slash() {
        let mirrors = normalize_mirrors(vec![
            "https://sci-hub.se/".to_string(),
            " sci-hub.ru".to_string(),
            "http://example.test".to_string(),
        ]);
        assert_eq!(
            mirrors,
            vec![
                "https://sci-hub.se".to_string(),
                "https://sci-hub.ru".to_string(),
                "http://example.test".to_string(),
            ]
        );
        // Empty / whitespace entries are dropped.
        assert!(normalize_mirrors(vec![]).is_empty());
        assert!(normalize_mirrors(vec!["".to_string(), "  ".to_string()]).is_empty());
    }

    #[test]
    fn extract_pdf_url_handles_iframe_embed_and_relative_forms() {
        let mirror = "https://sci-hub.test";

        // Absolute https URL in iframe.
        let html = r#"<html><body>
            <iframe id="pdf" src="https://dl.host/paper.pdf"></iframe>
        </body></html>"#;
        assert_eq!(
            extract_pdf_url_from_html(html, mirror).as_deref(),
            Some("https://dl.host/paper.pdf")
        );

        // Protocol-relative — inherits scheme from mirror.
        let html = r#"<embed type="application/pdf" src="//dl.host/paper.pdf">"#;
        assert_eq!(
            extract_pdf_url_from_html(html, mirror).as_deref(),
            Some("https://dl.host/paper.pdf")
        );

        // Root-relative.
        let html = r#"<iframe src='/downloads/paper.pdf'></iframe>"#;
        assert_eq!(
            extract_pdf_url_from_html(html, mirror).as_deref(),
            Some("https://sci-hub.test/downloads/paper.pdf")
        );

        // PDF with query-string token (auth/expiry).
        let html = r#"<iframe src="https://dl.host/paper.pdf?token=abc&expires=1">"#;
        assert_eq!(
            extract_pdf_url_from_html(html, mirror).as_deref(),
            Some("https://dl.host/paper.pdf?token=abc&expires=1")
        );

        // Non-PDF src first, PDF src second — the scanner skips past
        // and picks the PDF.
        let html = r#"<img src="/logo.png"><iframe src="https://dl.host/paper.pdf">"#;
        assert_eq!(
            extract_pdf_url_from_html(html, mirror).as_deref(),
            Some("https://dl.host/paper.pdf")
        );

        // No PDF embed at all (not-found stub page).
        let html = r#"<html><body><h1>article not found</h1></body></html>"#;
        assert_eq!(extract_pdf_url_from_html(html, mirror), None);
    }

    #[test]
    fn safe_filename_strips_path_separators() {
        assert_eq!(
            safe_filename("10.1038/s41586-021-03819-2"),
            "10.1038_s41586-021-03819-2"
        );
        // Already safe characters survive.
        assert_eq!(safe_filename("10.1038.suffix-only"), "10.1038.suffix-only");
    }

    #[test]
    fn into_discovery_record_emits_required_fields() {
        let rec = into_discovery_record("10.1038/s41586-021-03819-2");
        assert_eq!(rec.upstream_id, "scihub");
        assert_eq!(rec.record_id, "10.1038/s41586-021-03819-2");
        assert_eq!(
            rec.fields.get("fileType").map(String::as_str),
            Some("document")
        );
        assert_eq!(
            rec.fields.get("contentKind").map(String::as_str),
            Some("paper")
        );
        assert_eq!(rec.fields.get("format").map(String::as_str), Some("pdf"));
        // Canonical <upstream_id>id field (§6.4).
        assert_eq!(
            rec.fields.get("scihubid").map(String::as_str),
            Some("10.1038/s41586-021-03819-2")
        );
        // Semantic alias.
        assert_eq!(
            rec.fields.get("doi").map(String::as_str),
            Some("10.1038/s41586-021-03819-2")
        );
        // DOI-derived doi.org resolver as sourceUrl.
        assert_eq!(
            rec.fields.get("sourceUrl").map(String::as_str),
            Some("https://doi.org/10.1038/s41586-021-03819-2")
        );
        // fileName is filesystem-safe.
        assert_eq!(
            rec.fields.get("fileName").map(String::as_str),
            Some("scihub-10.1038_s41586-021-03819-2.pdf")
        );
    }

    // -- handle_query: pure logic (no HTTP) -----------------------------

    fn configured_plugin_no_http() -> (ScihubPlugin, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut plugin =
            ScihubPlugin::with_mirrors_for_test(vec!["https://example.test".to_string()]);
        plugin.configure(dir.path()).expect("configure");
        (plugin, dir)
    }

    #[tokio::test]
    async fn handle_query_returns_record_for_doi_filter() {
        let (plugin, _dir) = configured_plugin_no_http();
        let records = plugin
            .handle_query(
                &GatewayQuery::from_free_text("type:paper doi:10.1038/s41586-021-03819-2"),
                10,
            )
            .await
            .expect("handle_query");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record_id, "10.1038/s41586-021-03819-2");
    }

    #[tokio::test]
    async fn handle_query_returns_empty_without_doi_filter() {
        let (plugin, _dir) = configured_plugin_no_http();
        // Bare keyword query — no DOI, no record.
        let records = plugin
            .handle_query(&GatewayQuery::from_free_text("crispr"), 10)
            .await
            .expect("handle_query");
        assert!(records.is_empty());
        // `type:paper` alone (the gateway tier's gate) still doesn't
        // satisfy *us* without a DOI.
        let records = plugin
            .handle_query(&GatewayQuery::from_free_text("type:paper"), 10)
            .await
            .expect("handle_query");
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn handle_query_rejects_malformed_doi() {
        let (plugin, _dir) = configured_plugin_no_http();
        let records = plugin
            .handle_query(&GatewayQuery::from_free_text("doi:not-a-doi"), 10)
            .await
            .expect("handle_query");
        assert!(records.is_empty());
        // Too-short registrant code.
        let records = plugin
            .handle_query(&GatewayQuery::from_free_text("doi:10.12/foo"), 10)
            .await
            .expect("handle_query");
        assert!(records.is_empty());
    }

    // -- compute_outcomes against a wiremock mirror ----------------------
    //
    // The mock serves both the resolver HTML at `/<doi>` and the PDF
    // bytes at `/paper.pdf`. The plugin's HTML-scrape picks up the
    // absolute PDF URL from the resolver page.

    fn configured_plugin_against(server: &MockServer) -> (ScihubPlugin, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut plugin = ScihubPlugin::with_mirrors_for_test(vec![server.uri()]);
        plugin.configure(dir.path()).expect("configure");
        (plugin, dir)
    }

    fn resolver_html_pointing_at(pdf_url: &str) -> String {
        format!(r#"<html><body><iframe id="pdf" src="{pdf_url}"></iframe></body></html>"#)
    }

    #[tokio::test]
    async fn compute_outcomes_fetches_pdf_and_caches() {
        let server = MockServer::start().await;
        let doi = "10.1038/s41586-021-03819-2";
        let pdf_bytes = b"%PDF-1.7 fake scihub body\n".to_vec();
        let pdf_url = format!("{}/paper.pdf", server.uri());
        let expected_cid = meta_feeder_sdk::hash::compute_ipfs_cid(&pdf_bytes);

        Mock::given(method("GET"))
            .and(path(format!("/{doi}")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(resolver_html_pointing_at(&pdf_url))
                    .insert_header("content-type", "text/html"),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/paper.pdf"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(pdf_bytes.clone())
                    .insert_header("content-type", "application/pdf"),
            )
            .mount(&server)
            .await;

        let (plugin, _dir) = configured_plugin_against(&server);
        let mut outcomes = plugin.compute_outcomes(doi).await.expect("compute");
        assert_eq!(outcomes.len(), 1, "scihub is single-outcome");
        let outcome = outcomes.remove(0);
        assert_eq!(outcome.hash.as_str(), expected_cid);
        assert_eq!(outcome.hash_kind, HashKind::Sha2_256);
        assert_eq!(outcome.bytes.as_deref(), Some(pdf_bytes.as_slice()));
        assert_eq!(outcome.file_extension.as_deref(), Some("pdf"));
        let rec = outcome.record.expect("record present on fresh compute");
        assert_eq!(rec.record_id, doi);
        assert_eq!(rec.fields.get("doi").map(String::as_str), Some(doi));
    }

    #[tokio::test]
    async fn compute_outcomes_cache_hit_skips_http() {
        let server = MockServer::start().await;
        // No mocks mounted — any HTTP call would 404 the wiremock
        // default and surface as NotFound. The test asserts we don't
        // hit HTTP at all.
        let (plugin, _dir) = configured_plugin_against(&server);
        plugin
            .cache
            .as_ref()
            .unwrap()
            .put_midhash("10.1038/cached", "bafyCACHED")
            .unwrap();
        let mut outcomes = plugin
            .compute_outcomes("10.1038/cached")
            .await
            .expect("cache hit");
        assert_eq!(outcomes.len(), 1);
        let outcome = outcomes.remove(0);
        assert_eq!(outcome.hash.as_str(), "bafyCACHED");
        assert!(outcome.bytes.is_none());
        assert!(outcome.record.is_none());
    }

    #[tokio::test]
    async fn compute_outcomes_falls_through_to_next_mirror_on_not_found() {
        let server_a = MockServer::start().await;
        let server_b = MockServer::start().await;
        let doi = "10.1038/found-on-b";
        let pdf_bytes = b"%PDF-1.4 body\n".to_vec();
        let pdf_url_b = format!("{}/paper.pdf", server_b.uri());

        // Mirror A: resolver returns "not found" stub (no PDF iframe).
        Mock::given(method("GET"))
            .and(path(format!("/{doi}")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("<html><body>article not found</body></html>"),
            )
            .mount(&server_a)
            .await;
        // Mirror B: resolver points at a PDF URL B also serves.
        Mock::given(method("GET"))
            .and(path(format!("/{doi}")))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(resolver_html_pointing_at(&pdf_url_b)),
            )
            .mount(&server_b)
            .await;
        Mock::given(method("GET"))
            .and(path("/paper.pdf"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(pdf_bytes.clone()))
            .mount(&server_b)
            .await;

        let dir = tempfile::tempdir().expect("tempdir");
        let mut plugin = ScihubPlugin::with_mirrors_for_test(vec![server_a.uri(), server_b.uri()]);
        plugin.configure(dir.path()).expect("configure");

        let mut outcomes = plugin.compute_outcomes(doi).await.expect("compute");
        assert_eq!(outcomes.len(), 1);
        let outcome = outcomes.remove(0);
        assert_eq!(
            outcome.hash.as_str(),
            meta_feeder_sdk::hash::compute_ipfs_cid(&pdf_bytes)
        );
    }

    #[tokio::test]
    async fn compute_outcomes_rejects_non_doi_record_id() {
        let server = MockServer::start().await;
        let (plugin, _dir) = configured_plugin_against(&server);
        let err = plugin
            .compute_outcomes("not-a-doi")
            .await
            .expect_err("expected permanent");
        assert!(matches!(err, GatewayError::Permanent(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn compute_outcomes_rejects_non_pdf_body() {
        // A mirror that responds with 200 + HTML challenge page where
        // the PDF should be. Plugin must NOT cache that as a midhash.
        let server = MockServer::start().await;
        let doi = "10.1038/html-challenge";
        let pdf_url = format!("{}/paper.pdf", server.uri());
        Mock::given(method("GET"))
            .and(path(format!("/{doi}")))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(resolver_html_pointing_at(&pdf_url)),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/paper.pdf"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string("<html>cloudflare challenge</html>"),
            )
            .mount(&server)
            .await;
        let (plugin, _dir) = configured_plugin_against(&server);
        let err = plugin
            .compute_outcomes(doi)
            .await
            .expect_err("expected permanent");
        assert!(matches!(err, GatewayError::Permanent(_)), "got {err:?}");
    }

    // No `scihub_can_be_built` through `build_plugin_registry` — without
    // mirrors in `gateway-config.json` the plugin soft-skips and never
    // enters the registry. Coverage lives above via the
    // `with_mirrors_for_test` constructor.
}
