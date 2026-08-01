"""
BuyerBehaviorAggregator — builds a complete behavioral picture of each GoGlo buyer.

Sources:
  CRM2 / mysql_ui:  tracking_sessions, tracking_page_views, tracking_click_events,
                    tracking_scroll_events, enquiries, searched_keywords_list, messages
  CRM / mysql_crm:  click_tracking (button_identity — commercial clicks)
                    page_event_trackings (button_identity — PDP commercial actions)
                    page_visits (page_name, utm_data — anonymous + paid-ad attribution)
                    rfqs + rfq_items (RFQ draft/submitted leads)

Output nodes (Neo4j):
  BuyerProfile            → one per buyer user_id, full aggregated behavioral data
  Lead (engaged_person)   → buyers with 6+ high-intent click-weight, no enquiry
  Lead (known_account_interest) → buyers with 2–5 high-intent click-weight, no enquiry
  Enriches existing Lead nodes with behavioral_score, click context, scroll depth

Run via: python3 main.py build-buyer-profiles --config config.yaml
"""

import logging
from datetime import datetime
from collections import defaultdict
from app.core.logger import info, ok, warn, banner
from app.db.neo4j_client import Neo4jClient

logger = logging.getLogger(__name__)

# ─── High-intent element weights ──────────────────────────────────────────────
# These are button/section names from GoGlo product pages.
# Weight = purchase-intent signal strength (1-3).
HIGH_INTENT = {
    "Get Quote Now":              3,
    "Get Quote":                  3,
    "Send a Query":               2,
    "Contact Supplier":           2,
    "Chat with Seller":           2,
    "Technical Specifications":   1,
    "Packaging Details":          1,
    "Certifications and Compliance": 1,
    "Company Profile":            1,
}

# ─── High-intent button_identity weights (crm.click_tracking + crm.page_event_trackings) ──
# These are the actual CSS/HTML identifiers stored in the crm database.
# Correction per Saif (Jul 2026): commercial actions live in click_tracking +
# page_event_trackings.button_identity, NOT in page_visits.page_name.
HIGH_INTENT_BUTTON_ID = {
    # Quote / purchase intent (weight 3)
    "getQuoteButton":                                   3,
    "getquotebutton":                                   3,
    # Contact / query intent (weight 2)
    "contact-seller":                                   2,
    "contact_seller":                                   2,
    "send-query":                                       2,
    "Chat with Seller":                                 2,
    "add-to-contact":                                   2,
    # Product research — strong (weight 2)
    "catalog-download":                                 2,
    # Product research — moderate (weight 1)
    "company-profile":                                  1,
    "Technical Specifications-product-specifciation-Tab": 1,
    "Packaging Details-product-specifciation-Tab":      1,
    "Certifications and Compliance-product-specifciation-Tab": 1,
    "Shipping option-product-specifciation-Tab":        1,
    "Warranty Information-product-specifciation-Tab":   1,
    "Product Description-product-specifciation-Tab":    1,
    "Return Policy-product-specifciation-Tab":          1,
    "product-image-click":                              1,
    "visit-store-link":                                 1,
    "view-more-pdp":                                    1,
}


def _now() -> str:
    return datetime.utcnow().isoformat()


def _crm2_conn(settings):
    import pymysql
    return pymysql.connect(
        host=settings.mysql_ui.host, port=settings.mysql_ui.port,
        user=settings.mysql_ui.user, password=settings.mysql_ui.password,
        database=settings.mysql_ui.database, charset="utf8mb4",
        connect_timeout=10,
    )



def _crm_conn(settings):
    """Connect to the main CRM MySQL (mysql_crm config, port 3307 via tunnel).
    This has click_tracking, page_event_trackings, page_visits, rfqs, etc.
    """
    import pymysql
    return pymysql.connect(
        host=settings.mysql_crm.host, port=settings.mysql_crm.port,
        user=settings.mysql_crm.user, password=settings.mysql_crm.password,
        database=settings.mysql_crm.database, charset="utf8mb4",
        connect_timeout=10,
    )

def _fetch(cur, sql):
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


# ─── Step 1: Pull raw behavioral tables from CRM2 ─────────────────────────────
def _pull_sessions(cur):
    return _fetch(cur, """
        SELECT user_id, session_id, ip_address,
               user_agent, created_at
        FROM tracking_sessions
        WHERE user_id IS NOT NULL AND user_id > 0
    """)


def _pull_clicks(cur):
    """One row per click event, with user_id from the joined session."""
    return _fetch(cur, """
        SELECT tce.id, tce.session_id, ts.user_id,
               tce.element_type, tce.element_id,
               tce.created_at
        FROM tracking_click_events tce
        INNER JOIN tracking_sessions ts ON ts.session_id = tce.session_id
        WHERE ts.user_id IS NOT NULL AND ts.user_id > 0
    """)


def _pull_scrolls(cur):
    """Scroll depth events per session."""
    return _fetch(cur, """
        SELECT tse.session_id, ts.user_id,
               CAST(tse.average AS DECIMAL(10,2)) AS scroll_percentage,
               tse.created_at
        FROM tracking_scroll_events tse
        INNER JOIN tracking_sessions ts ON ts.session_id = tse.session_id
        WHERE ts.user_id IS NOT NULL AND ts.user_id > 0
    """)


def _pull_pageviews(cur):
    """Aggregated page views per session (for product intent)."""
    return _fetch(cur, """
        SELECT tpv.session_id, ts.user_id,
               COUNT(tpv.id)                                             AS pv_count,
               SUM(CASE WHEN tpv.type = 'ProductDetail' THEN 1 ELSE 0 END) AS product_pv,
               MAX(tpv.created_at)                                       AS last_seen
        FROM tracking_page_views tpv
        INNER JOIN tracking_sessions ts ON ts.session_id = tpv.session_id
        WHERE tpv.deleted_at IS NULL
          AND ts.user_id IS NOT NULL AND ts.user_id > 0
        GROUP BY tpv.session_id, ts.user_id
    """)


def _pull_enquiries(cur):
    """Per-user enquiry summary (enquiries already imported as Lead nodes;
    we need this for profile scoring and enrichment linkage)."""
    return _fetch(cur, """
        SELECT enquiry_user_id                                    AS user_id,
               COUNT(*)                                           AS enq_count,
               SUM(CASE WHEN enquiry_type = 'quote' THEN 1 ELSE 0 END) AS quote_count,
               MAX(created_at)                                    AS last_enquiry
        FROM enquiries
        WHERE deleted_at IS NULL
          AND enquiry_user_id IS NOT NULL AND enquiry_user_id > 0
        GROUP BY enquiry_user_id
    """)


def _pull_searches(cur):
    return _fetch(cur, """
        SELECT user_id,
               COUNT(DISTINCT name)  AS keyword_count,
               SUM(searched_total)   AS total_searches,
               GROUP_CONCAT(DISTINCT name ORDER BY searched_total DESC SEPARATOR '|') AS keywords
        FROM searched_keywords_list
        WHERE deleted_at IS NULL AND user_id IS NOT NULL AND user_id > 0
        GROUP BY user_id
    """)


def _pull_messages(cur):
    """Best-effort: different CRM2 schemas use different column names."""
    # Try sender_id first, fall back to user_id
    for col in ("sender_id", "user_id", "from_user_id"):
        try:
            return _fetch(cur, f"""
                SELECT {col} AS user_id, COUNT(*) AS msg_count, MAX(created_at) AS last_message
                FROM messages
                WHERE {col} IS NOT NULL AND {col} > 0
                GROUP BY {col}
            """)
        except Exception:
            continue
    return []



# ─── CRM MySQL pull functions (mysql_crm, scenarios D/H/A/C/M/F/G) ────────────

def _pull_crm_clicks(cur):
    """Pull commercial click events from crm.click_tracking.
    Correction (Saif Jul 2026): commercial actions are in click_tracking.button_identity,
    not page_visits.page_name.  Covers scenarios D (PDP Commercial Path) and H (Quote Request).
    """
    return _fetch(cur, """
        SELECT ct.id, ct.user_id, ct.session_id,
               ct.button_identity, ct.page_url,
               ct.utm_source, ct.fingerprint,
               ct.created_at
        FROM click_tracking ct
        WHERE ct.user_id IS NOT NULL AND ct.user_id <> ''
          AND ct.button_identity IS NOT NULL AND ct.button_identity <> ''
    """)


def _pull_crm_page_events(cur):
    """Pull commercial action events from crm.page_event_trackings.
    Covers scenarios D (PDP Commercial Path), H (Quote Request Send Quote Click).
    Has seller_id, product_id, category_id context alongside button_identity.
    """
    return _fetch(cur, """
        SELECT pet.id, pet.user_id, pet.session_id,
               pet.button_identity, pet.page_name, pet.page_url,
               pet.seller_id, pet.product_id, pet.category_id,
               pet.utm_source, pet.created_at
        FROM page_event_trackings pet
        WHERE pet.user_id IS NOT NULL AND pet.user_id <> ''
          AND pet.button_identity IS NOT NULL AND pet.button_identity <> ''
    """)


def _pull_page_visits(cur):
    """Pull page visit records from crm.page_visits.
    Covers scenarios A (anonymous), C (category research), M (paid ads via utm_data).
    page_name values: home, plp, pdp, category, etc.
    utm_data is JSON with paid campaign attribution.
    """
    return _fetch(cur, """
        SELECT pv.id, pv.user_id, pv.session_id, pv.fingerprint,
               pv.page_name, pv.page_url,
               pv.seller_id, pv.product_id, pv.category_id,
               pv.utm_data, pv.max_scrol_depth,
               pv.no_of_element_clicks, pv.stay_on_page,
               pv.created_at
        FROM page_visits pv
        WHERE pv.created_at IS NOT NULL
    """)


def _pull_rfqs(cur):
    """Pull RFQ records from crm.rfqs for scenarios F (RFQ Draft) and G (RFQ Submitted)."""
    return _fetch(cur, """
        SELECT r.id, r.user_id, r.rfq_number, r.status,
               r.title, r.category, r.source_type,
               r.organization_id, r.confidence_score,
               r.created_at,
               GROUP_CONCAT(ri.item_name SEPARATOR '|') AS item_names,
               GROUP_CONCAT(ri.category SEPARATOR '|')  AS item_categories
        FROM rfqs r
        LEFT JOIN rfq_items ri ON ri.rfq_id = r.id
        WHERE r.user_id IS NOT NULL
        GROUP BY r.id
    """)

# ─── Step 2: Aggregate into per-user profiles ─────────────────────────────────
def _build_profiles(sessions, clicks, scrolls, pageviews, enquiries, searches, messages,
                    crm_clicks=None, crm_page_events=None, rfqs=None):
    """Returns (profiles dict keyed by str(user_id), enq_user_ids set)."""

    def _empty():
        return {
            "user_id":             None,
            "total_sessions":      0,
            "total_page_views":    0,
            "product_page_views":  0,
            "total_clicks":        0,
            "high_intent_score":   0,    # weighted sum of high-intent clicks
            "high_intent_detail":  {},   # element_type → raw count
            "sessions_deep_scroll": 0,   # sessions with max scroll >= 75%
            "max_scroll_depth":    0.0,
            "enquiries_sent":      0,
            "quote_requests":      0,
            "messages_sent":       0,
            "searches_done":       0,
            "search_keywords":     "",
            "products_viewed":     set(),
            "last_activity_at":    "",
            "countries":           set(),
            "devices":             set(),
        }

    profiles = defaultdict(_empty)

    # Build session → user_id lookup
    session_user: dict[str, str] = {}

    # Sessions
    for s in sessions:
        uid = str(s["user_id"])
        session_user[str(s["session_id"])] = uid
        p = profiles[uid]
        p["user_id"] = uid
        p["total_sessions"] += 1
        ts = str(s.get("created_at") or "")
        if ts > p["last_activity_at"]:
            p["last_activity_at"] = ts

    # Clicks
    for c in clicks:
        uid = str(c["user_id"])
        p = profiles[uid]
        p["user_id"] = uid
        p["total_clicks"] += 1
        etype = (c.get("element_type") or "").strip()
        weight = HIGH_INTENT.get(etype, 0)
        if weight:
            p["high_intent_score"] += weight
            p["high_intent_detail"][etype] = p["high_intent_detail"].get(etype, 0) + 1

    # Scroll: find max depth per session, then attribute to user
    session_max_scroll: dict[str, float] = defaultdict(float)
    scroll_session_user: dict[str, str] = {}
    for s in scrolls:
        sid = str(s.get("session_id") or "")
        depth = float(s.get("scroll_percentage") or 0)
        if depth > session_max_scroll[sid]:
            session_max_scroll[sid] = depth
        uid = str(s.get("user_id") or "")
        if uid:
            scroll_session_user[sid] = uid

    seen_deep: set[str] = set()
    for sid, depth in session_max_scroll.items():
        uid = scroll_session_user.get(sid) or session_user.get(sid)
        if not uid:
            continue
        p = profiles[uid]
        if depth > p["max_scroll_depth"]:
            p["max_scroll_depth"] = depth
        if depth >= 75.0:
            key = f"{uid}:{sid}"
            if key not in seen_deep:
                seen_deep.add(key)
                p["sessions_deep_scroll"] += 1

    # Page views
    for pv in pageviews:
        uid = str(pv["user_id"])
        p = profiles[uid]
        p["user_id"] = uid
        p["total_page_views"] += int(pv.get("pv_count") or 0)
        p["product_page_views"] += int(pv.get("product_pv") or 0)
        ts = str(pv.get("last_seen") or "")
        if ts > p["last_activity_at"]:
            p["last_activity_at"] = ts

    # Enquiries
    enq_users: set[str] = set()
    for e in enquiries:
        uid = str(e["user_id"])
        enq_users.add(uid)
        p = profiles[uid]
        p["user_id"] = uid
        p["enquiries_sent"] = int(e.get("enq_count") or 0)
        p["quote_requests"] = int(e.get("quote_count") or 0)
        ts = str(e.get("last_enquiry") or "")
        if ts > p["last_activity_at"]:
            p["last_activity_at"] = ts

    # Searches
    for s in searches:
        uid = str(s["user_id"])
        p = profiles[uid]
        p["user_id"] = uid
        p["searches_done"] = int(s.get("total_searches") or 0)
        p["search_keywords"] = (s.get("keywords") or "")[:500]

    # Messages
    for m in messages:
        uid = str(m.get("user_id") or "")
        if uid:
            p = profiles[uid]
            p["user_id"] = uid
            p["messages_sent"] = int(m.get("msg_count") or 0)


    # CRM click_tracking — button_identity-based commercial clicks (scenarios D, H)
    for c in (crm_clicks or []):
        uid = str(c.get("user_id") or "")
        if not uid:
            continue
        p = profiles[uid]
        p["user_id"] = uid
        p["total_clicks"] += 1
        btn = str(c.get("button_identity") or "").strip()
        weight = HIGH_INTENT_BUTTON_ID.get(btn, 0)
        if weight:
            p["high_intent_score"] += weight
            p["high_intent_detail"][btn] = p["high_intent_detail"].get(btn, 0) + 1
        ts = str(c.get("created_at") or "")
        if ts > p["last_activity_at"]:
            p["last_activity_at"] = ts

    # CRM page_event_trackings — button_identity with seller/product context (scenarios D, H)
    for e in (crm_page_events or []):
        uid = str(e.get("user_id") or "")
        if not uid:
            continue
        p = profiles[uid]
        p["user_id"] = uid
        p["total_clicks"] += 1
        btn = str(e.get("button_identity") or "").strip()
        weight = HIGH_INTENT_BUTTON_ID.get(btn, 0)
        if weight:
            p["high_intent_score"] += weight
            p["high_intent_detail"][btn] = p["high_intent_detail"].get(btn, 0) + 1
        ts = str(e.get("created_at") or "")
        if ts > p["last_activity_at"]:
            p["last_activity_at"] = ts

    # CRM rfqs — RFQ submitted/draft (scenarios F, G)
    rfq_users: set[str] = set()
    for r in (rfqs or []):
        uid = str(r.get("user_id") or "")
        if not uid:
            continue
        rfq_users.add(uid)
        p = profiles[uid]
        p["user_id"] = uid
        status = str(r.get("status") or "draft")
        # submitted RFQ is a strong signal — treat as an enquiry
        if status in ("submitted", "active", "open"):
            p["enquiries_sent"] += 1
            if status == "submitted":
                p["quote_requests"] += 1
        ts = str(r.get("created_at") or "")
        if ts > p["last_activity_at"]:
            p["last_activity_at"] = ts
    enq_users.update(rfq_users)

    return dict(profiles), enq_users


# ─── Step 3: Score and classify profiles ──────────────────────────────────────
def _behavioral_score(p: dict) -> int:
    """
    Compute intent score 0–100.
    Enquiries dominate — direct purchase signal.
    Clicks on high-intent buttons are secondary.
    Sessions + scroll + search add context weight.
    """
    score = 30                                          # base: any platform activity
    score += min(p["enquiries_sent"] * 8, 40)          # max +40
    score += min(p["high_intent_score"] * 2, 20)       # max +20
    score += min(p["total_sessions"] * 2, 10)          # max +10
    score += min(p["sessions_deep_scroll"] * 2, 8)     # max +8
    score += min(p["searches_done"], 5)                 # max +5
    score += min(p["messages_sent"] * 3, 9)            # max +9
    return min(int(score), 100)


def _lead_type_for_profile(p: dict, score: int) -> tuple[str, int]:
    """Map a buyer profile to the most appropriate lead type and score."""
    if p["enquiries_sent"] > 0 and p["quote_requests"] > 0:
        return "quote_ready", min(score + 5, 100)
    if p["enquiries_sent"] > 0:
        return "rfq_submitted", score
    if p["high_intent_score"] >= 6 or p["messages_sent"] > 0:
        return "engaged_person", min(score, 80)
    if p["high_intent_score"] >= 2:
        return "known_account_interest", min(score, 70)
    if p["product_page_views"] >= 3 or p["total_sessions"] >= 2:
        return "visit_only", max(score, 40)
    return "visit_only", 40


# ─── Step 4: Neo4j upserts ────────────────────────────────────────────────────
def _upsert_profiles(neo: Neo4jClient, profiles: dict, enq_users: set) -> int:
    """Create or update BuyerProfile nodes — always overwrites behavioral data."""
    batch = []
    for uid, p in profiles.items():
        if not p["user_id"]:
            continue
        score = _behavioral_score(p)
        lt, _ = _lead_type_for_profile(p, score)
        click_detail = "|".join(
            f"{k}:{v}" for k, v in
            sorted(p["high_intent_detail"].items(), key=lambda x: -x[1])
        )[:500]

        batch.append({
            "profile_uid":          f"buyer_profile:{uid}",
            "user_id":              uid,
            "behavioral_score":     score,
            "lead_type_signal":     lt,
            "total_sessions":       p["total_sessions"],
            "total_page_views":     p["total_page_views"],
            "product_page_views":   p["product_page_views"],
            "total_clicks":         p["total_clicks"],
            "high_intent_score":    p["high_intent_score"],
            "high_intent_detail":   click_detail,
            "sessions_deep_scroll": p["sessions_deep_scroll"],
            "max_scroll_depth":     p["max_scroll_depth"],
            "enquiries_sent":       p["enquiries_sent"],
            "quote_requests":       p["quote_requests"],
            "messages_sent":        p["messages_sent"],
            "searches_done":        p["searches_done"],
            "search_keywords":      p["search_keywords"],
            "products_viewed_count": len(p["products_viewed"]),
            "products_top":         ",".join(list(p["products_viewed"])[:20]),
            "buyer_countries":      ",".join(sorted(p["countries"])),
            "buyer_devices":        ",".join(sorted(p["devices"])),
            "last_activity_at":     p["last_activity_at"],
            "has_enquiry":          uid in enq_users,
        })

    if not batch:
        return 0

    # Upsert in batches of 500 to avoid memory pressure
    chunk = 500
    for i in range(0, len(batch), chunk):
        neo.run("""
            UNWIND $rows AS row
            MERGE (bp:BuyerProfile {profile_uid: row.profile_uid})
            SET
                bp.user_id              = row.user_id,
                bp.behavioral_score     = CASE WHEN coalesce(bp.score_override, false) = true THEN bp.behavioral_score ELSE row.behavioral_score END,
                bp.lead_type_signal     = row.lead_type_signal,
                bp.total_sessions       = row.total_sessions,
                bp.total_page_views     = row.total_page_views,
                bp.product_page_views   = row.product_page_views,
                bp.total_clicks         = row.total_clicks,
                bp.high_intent_score    = row.high_intent_score,
                bp.high_intent_detail   = row.high_intent_detail,
                bp.sessions_deep_scroll = row.sessions_deep_scroll,
                bp.max_scroll_depth     = row.max_scroll_depth,
                bp.enquiries_sent       = row.enquiries_sent,
                bp.quote_requests       = row.quote_requests,
                bp.messages_sent        = row.messages_sent,
                bp.searches_done        = row.searches_done,
                bp.search_keywords      = row.search_keywords,
                bp.products_viewed_count = row.products_viewed_count,
                bp.products_top         = row.products_top,
                bp.buyer_countries      = row.buyer_countries,
                bp.buyer_devices        = row.buyer_devices,
                bp.last_activity_at     = row.last_activity_at,
                bp.has_enquiry          = row.has_enquiry,
                bp.updated_at           = $now
        """, {"rows": batch[i:i + chunk], "now": _now()})

    return len(batch)


def _create_click_intent_leads(neo: Neo4jClient, profiles: dict, enq_users: set) -> dict:
    """
    Create Lead nodes for buyers who clicked high-intent buttons
    but haven't sent an enquiry yet — the "almost converted" segment.
    These buyers are not captured by the enquiry-based pipeline.
    """
    batch = []
    for uid, p in profiles.items():
        if uid in enq_users:
            continue                       # enquiry-based lead already exists
        if p["high_intent_score"] < 2:
            continue                       # not enough intent signal

        score = _behavioral_score(p)
        lt, score_adj = _lead_type_for_profile(p, score)
        if lt in ("visit_only",):
            continue                       # too weak for a new lead

        click_detail = "|".join(
            f"{k}:{v}" for k, v in
            sorted(p["high_intent_detail"].items(), key=lambda x: -x[1])
        )[:300]

        src = f"click_behavior:{uid}"
        batch.append({
            "lead_uid":           f"behavioral:{lt}:{src}",
            "lead_type":          lt,
            "score_final":        score_adj,
            "visibility_level":   "seller_visible",
            "source":             "goglo_behavior",
            "source_ref":         src,
            "buyer_user_id":      uid,
            "high_intent_score":  p["high_intent_score"],
            "click_detail":       click_detail,
            "total_sessions":     p["total_sessions"],
            "product_page_views": p["product_page_views"],
            "max_scroll_depth":   p["max_scroll_depth"],
            "searches_done":      p["searches_done"],
            "behavioral_score":   score,
            "last_activity_at":   p["last_activity_at"],
        })

    if batch:
        neo.run("""
            UNWIND $rows AS row
            MERGE (l:Lead {lead_uid: row.lead_uid})
            ON CREATE SET
                l.lead_type          = row.lead_type,
                l.score_final        = row.score_final,
                l.visibility_level   = row.visibility_level,
                l.source             = row.source,
                l.source_ref         = row.source_ref,
                l.synced_from_sql    = true,
                l.buyer_user_id      = row.buyer_user_id,
                l.high_intent_score  = row.high_intent_score,
                l.click_detail       = row.click_detail,
                l.total_sessions     = row.total_sessions,
                l.product_page_views = row.product_page_views,
                l.max_scroll_depth   = row.max_scroll_depth,
                l.searches_done      = row.searches_done,
                l.behavioral_score   = row.behavioral_score,
                l.last_activity_at   = row.last_activity_at,
                l.classified_at      = $now
        """, {"rows": batch, "now": _now()})

    counts = {}
    for b in batch:
        counts[b["lead_type"]] = counts.get(b["lead_type"], 0) + 1
    return counts


def _enrich_existing_leads(neo: Neo4jClient, profiles: dict) -> int:
    """
    Write behavioral_score + click context back onto existing Lead nodes
    that share a buyer_user_id, and link them to their BuyerProfile.
    """
    batch = [
        {
            "user_id":           uid,
            "behavioral_score":  _behavioral_score(p),
            "high_intent_score": p["high_intent_score"],
            "total_sessions":    p["total_sessions"],
            "max_scroll_depth":  p["max_scroll_depth"],
        }
        for uid, p in profiles.items()
        if p["user_id"]
    ]
    if not batch:
        return 0

    rows = neo.run("""
        UNWIND $rows AS row
        MATCH (l:Lead)
        WHERE l.buyer_user_id = row.user_id
          AND coalesce(l.score_override, false) = false
        SET l.behavioral_score  = row.behavioral_score,
            l.high_intent_score = row.high_intent_score,
            l.total_sessions    = row.total_sessions,
            l.max_scroll_depth  = row.max_scroll_depth
        WITH l, row
        OPTIONAL MATCH (bp:BuyerProfile {profile_uid: 'buyer_profile:' + row.user_id})
        FOREACH (x IN CASE WHEN bp IS NOT NULL THEN [1] ELSE [] END |
            MERGE (bp)-[:HAS_LEAD]->(l)
        )
        RETURN count(l) AS c
    """, {"rows": batch})
    return rows[0]["c"] if rows else 0


# ─── Real-time event handlers (called by KafkaEventConsumer) ─────────────────

def update_single_profile(neo: Neo4jClient, pg, payload: dict):
    """
    Update BuyerProfile for a single session event (crm.buyer_sessions topic).
    Payload is a raw row from tracking_sessions / session_engagements / page_visits etc.
    Increments session count and refreshes last_activity_at on the BuyerProfile node.
    pg is the PostgresClient — not needed here but passed for interface consistency.
    """
    user_id = str(
        payload.get("user_id") or payload.get("userId") or
        payload.get("session_user_id") or ""
    )
    if not user_id or user_id == "0":
        return   # anonymous / unknown session — nothing to update

    session_id = str(payload.get("session_id") or payload.get("id") or "")
    polled_at  = payload.get("_polled_at") or _now()

    neo.run("""
        MERGE (bp:BuyerProfile {profile_uid: 'buyer_profile:' + $uid})
        ON CREATE SET
            bp.user_id           = $uid,
            bp.total_sessions    = 1,
            bp.behavioral_score  = 30,
            bp.last_activity_at  = $ts,
            bp.updated_at        = $ts
        ON MATCH SET
            bp.total_sessions    = coalesce(bp.total_sessions, 0) + 1,
            bp.last_activity_at  = CASE
                WHEN $ts > coalesce(bp.last_activity_at, '') THEN $ts
                ELSE bp.last_activity_at
            END,
            bp.updated_at        = $ts
    """, {"uid": user_id, "ts": polled_at})

    logger.debug(f"update_single_profile: BuyerProfile updated user_id={user_id} session={session_id}")


def record_click_signal(neo: Neo4jClient, payload: dict):
    """
    Record a high-intent click event on BuyerProfile (crm.buyer_clicks topic).
    Payload is a raw row from tracking_click_events (joined with session user_id).
    Increments high_intent_score when element_type matches known high-intent buttons.
    """
    user_id = str(
        payload.get("user_id") or payload.get("userId") or ""
    )
    if not user_id or user_id == "0":
        return

    element_type = str(payload.get("element_type") or payload.get("element_id") or "")
    button_id    = str(payload.get("button_identity") or "")
    # Check both: display-name HIGH_INTENT (crm2) and button_identity HIGH_INTENT_BUTTON_ID (crm)
    weight = HIGH_INTENT.get(element_type, 0) or HIGH_INTENT_BUTTON_ID.get(button_id, 0)
    polled_at    = payload.get("_polled_at") or _now()

    neo.run("""
        MERGE (bp:BuyerProfile {profile_uid: 'buyer_profile:' + $uid})
        ON CREATE SET
            bp.user_id          = $uid,
            bp.total_clicks     = 1,
            bp.high_intent_score = $weight,
            bp.behavioral_score  = 30,
            bp.last_activity_at  = $ts,
            bp.updated_at        = $ts
        ON MATCH SET
            bp.total_clicks      = coalesce(bp.total_clicks, 0) + 1,
            bp.high_intent_score = coalesce(bp.high_intent_score, 0) + $weight,
            bp.last_activity_at  = CASE
                WHEN $ts > coalesce(bp.last_activity_at, '') THEN $ts
                ELSE bp.last_activity_at
            END,
            bp.updated_at        = $ts
    """, {"uid": user_id, "weight": weight, "ts": polled_at})

    logger.debug(f"record_click_signal: user={user_id} element={element_type!r} weight={weight}")


# ─── Orchestrator ─────────────────────────────────────────────────────────────
def run(config_path: str = "config.yaml") -> dict:
    banner("GoGlo Buyer Behavior Aggregator")
    from app.core.config import load_settings
    settings = load_settings(config_path)
    neo = Neo4jClient(settings.neo4j.uri, settings.neo4j.user, settings.neo4j.password)

    results = {}
    try:
        # ── Pull raw data from CRM2 (tracking_* legacy tables) ────────────────
        info("Step 1a: Pulling behavioral data from CRM2 (tracking tables)...")
        conn = _crm2_conn(settings)
        try:
            cur = conn.cursor()
            sessions  = _pull_sessions(cur)
            clicks    = _pull_clicks(cur)
            scrolls   = _pull_scrolls(cur)
            pageviews = _pull_pageviews(cur)
            enquiries = _pull_enquiries(cur)
            searches  = _pull_searches(cur)
            messages  = _pull_messages(cur)
            cur.close()
        finally:
            conn.close()
        ok(f"  → sessions={len(sessions):,}  clicks={len(clicks):,}  scrolls={len(scrolls):,}")
        ok(f"  → pageviews={len(pageviews):,}  enquiries={len(enquiries):,}  searches={len(searches):,}  messages={len(messages):,}")

        # ── Pull from CRM mysql_crm (click_tracking, page_event_trackings, rfqs) ──
        crm_clicks = crm_page_events = rfqs_data = []
        info("Step 1b: Pulling CRM commercial signals (click_tracking, page_event_trackings, rfqs)...")
        try:
            crm_conn = _crm_conn(settings)
            try:
                cur2 = crm_conn.cursor()
                crm_clicks      = _pull_crm_clicks(cur2)
                crm_page_events = _pull_crm_page_events(cur2)
                rfqs_data       = _pull_rfqs(cur2)
                cur2.close()
            finally:
                crm_conn.close()
            ok(f"  → crm_clicks={len(crm_clicks):,}  crm_page_events={len(crm_page_events):,}  rfqs={len(rfqs_data):,}")
        except Exception as _e:
            warn(f"  CRM mysql_crm pull failed (non-fatal): {_e}")

        # ── Build per-user profiles ────────────────────────────────────────────
        info("Step 2: Aggregating into per-user behavioral profiles...")
        profiles, enq_users = _build_profiles(
            sessions, clicks, scrolls, pageviews, enquiries, searches, messages,
            crm_clicks=crm_clicks, crm_page_events=crm_page_events, rfqs=rfqs_data
        )
        ok(f"  → {len(profiles):,} unique buyers profiled  |  {len(enq_users):,} with enquiries")

        # Score distribution (informational)
        scores = [_behavioral_score(p) for p in profiles.values()]
        if scores:
            high = sum(1 for s in scores if s >= 70)
            med  = sum(1 for s in scores if 50 <= s < 70)
            low  = sum(1 for s in scores if s < 50)
            ok(f"  → Score distribution: high(>=70)={high}  medium(50-69)={med}  low(<50)={low}")

        # ── Upsert BuyerProfile nodes ──────────────────────────────────────────
        info("Step 3: Upserting BuyerProfile nodes in Neo4j...")
        n_profiles = _upsert_profiles(neo, profiles, enq_users)
        results["buyer_profiles_upserted"] = n_profiles
        ok(f"  → {n_profiles:,} BuyerProfile nodes written")

        # ── Create click-intent leads (non-enquiry buyers) ─────────────────────
        info("Step 4: Creating click-intent Lead nodes (high-intent, no enquiry yet)...")
        click_counts = _create_click_intent_leads(neo, profiles, enq_users)
        results.update(click_counts)
        total_click_leads = sum(click_counts.values())
        ok(f"  → {click_counts}  (total: {total_click_leads})")

        # ── Enrich existing leads with behavioral context ──────────────────────
        info("Step 5: Enriching existing Lead nodes with behavioral scores...")
        enriched = _enrich_existing_leads(neo, profiles)
        results["leads_enriched"] = enriched
        ok(f"  → {enriched:,} existing leads enriched with behavioral_score + click context")

        # ── Summary report ─────────────────────────────────────────────────────
        stats = neo.run("""
            MATCH (bp:BuyerProfile)
            RETURN
                count(bp)                                                          AS total_profiles,
                round(avg(bp.behavioral_score), 1)                                AS avg_score,
                sum(CASE WHEN bp.behavioral_score >= 70 THEN 1 ELSE 0 END)        AS high_intent,
                sum(CASE WHEN bp.enquiries_sent > 0 THEN 1 ELSE 0 END)            AS enquiry_buyers,
                sum(CASE WHEN bp.high_intent_score >= 6 THEN 1 ELSE 0 END)        AS strong_clickers,
                sum(CASE WHEN bp.sessions_deep_scroll >= 1 THEN 1 ELSE 0 END)     AS deep_readers,
                sum(CASE WHEN bp.messages_sent > 0 THEN 1 ELSE 0 END)             AS chatted
        """)

        # Top clicked elements from BuyerProfile nodes (to see platform usage)
        top_clicks = neo.run("""
            MATCH (bp:BuyerProfile)
            WHERE bp.high_intent_detail IS NOT NULL AND bp.high_intent_detail <> ''
            RETURN bp.high_intent_detail AS detail, bp.behavioral_score AS score
            ORDER BY bp.behavioral_score DESC
            LIMIT 5
        """)

        ok("\n=== Buyer Behavior Summary ===")
        if stats:
            s = stats[0]
            info(f"  BuyerProfile nodes:     {s['total_profiles']:,}")
            info(f"  Avg behavioral score:   {s['avg_score']}")
            info(f"  High-intent buyers:     {s['high_intent']:,}  (score >= 70)")
            info(f"  Buyers with enquiries:  {s['enquiry_buyers']:,}")
            info(f"  Strong clickers:        {s['strong_clickers']:,}  (intent-score >= 6)")
            info(f"  Deep readers:           {s['deep_readers']:,}  (scrolled >= 75% at least once)")
            info(f"  Chatted with sellers:   {s['chatted']:,}")

        ok("\n=== Top 5 Most Engaged Buyers (by behavioral score) ===")
        if top_clicks:
            for r in top_clicks:
                info(f"  score={r['score']}  clicks={r['detail'][:80]}")

    finally:
        neo.close()

    return results
