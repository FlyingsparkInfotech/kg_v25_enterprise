"""
LeadClassifier — converts GoGlo platform signals into typed Lead nodes.

Signal sources:
  1. page_view signals   → visit_only / anonymous_account_visit
  2. RFQ nodes           → rfq_draft / rfq_submitted / quote_ready
  3. Meeting nodes       → engaged_account / engaged_person
  4. null Lead nodes     → known_account_interest (reclassify in-place)
  5. cold_market Leads   → active_importer (reclassify in-place)
  6. Intent + news nodes → hot_in_market / intent_only
  7. deal_created signal → engaged_account

Run via:  python3 main.py classify-platform-leads --config config.yaml
"""

import hashlib
from datetime import datetime
from app.core.logger import info, ok, warn, banner
from app.db.neo4j_client import Neo4jClient


# ─── scoring constants ────────────────────────────────────────────────────────
SCORES = {
    "rfq_submitted":                85,
    "rfq_draft":                    65,
    "quote_ready":                  90,
    "engaged_person":               75,
    "engaged_account":              70,
    "known_account_interest":       68,
    "hot_in_market":                72,
    "intent_only":                  55,
    "active_importer":              60,
    "visit_only":                   40,
    "anonymous_account_visit":      30,
    "trade_buyer_candidate":        62,
    "strategic_account_watch":      65,
    "competitor_displacement":      80,
    "known_person_interest":        70,
    "reactivation_candidate":    72,
    "partner_chain_opportunity": 68,
    "suppressed_noise":          10,
    "active_exporter":           60,
}

VISIBILITY = {
    # instant_alert — push notify seller immediately; SLA 4 hours
    "rfq_submitted":                "instant_alert",
    "quote_ready":                  "instant_alert",
    "hot_in_market":                "instant_alert",
    # push_notify — send seller notification; action recommended
    "rfq_draft":                    "push_notify",
    "competitor_displacement":      "push_notify",
    "switch_lead":                  "push_notify",
    # priority — direct route to seller within 24 hours
    "engaged_person":               "priority",
    "engaged_account":              "priority",
    "active_importer":              "priority",
    "trade_buyer_candidate":        "priority",
    # feed — add to nurture sequence / CRM sync
    "known_account_interest":       "feed",
    "known_person_interest":        "feed",
    "intent_only":                  "feed",
    # watchlist — flag and monitor; no active routing
    "strategic_account_watch":      "watchlist",
    "anonymous_account_visit":      "watchlist",
    # count_only — aggregated stats only; not shown to sellers
    "visit_only":                   "count_only",
    "reactivation_candidate":    "push_notify",
    "partner_chain_opportunity": "feed",
    "suppressed_noise":          "count_only",
    "active_exporter":           "feed",
}


PLAYBOOK_TAGS = {
    "rfq_submitted":             ["rfq_respond_now", "procurement_response"],
    "quote_ready":               ["rfq_respond_now", "commercial_path"],
    "hot_in_market":             ["high_intent_abandonment", "account_warming"],
    "competitor_displacement":   ["champion_reengagement", "commercial_path"],
    "engaged_person":            ["contact_qualification", "buying_committee_velocity"],
    "engaged_account":           ["account_warming", "contact_qualification"],
    "known_person_interest":     ["contact_qualification", "outbound_trade"],
    "known_account_interest":    ["account_warming"],
    "active_importer":           ["outbound_trade", "procurement_response"],
    "trade_buyer_candidate":     ["supply_chain_resilience", "outbound_trade"],
    "intent_only":               ["account_warming", "high_intent_abandonment"],
    "strategic_account_watch":   ["strategic_consolidation", "ecosystem_play"],
    "reactivation_candidate":    ["champion_reengagement", "contact_moved_to_new_company"],
    "partner_chain_opportunity": ["ecosystem_play", "cross_sell"],
    "active_exporter":           ["ecosystem_play", "strategic_consolidation"],
    "visit_only":                [],
    "anonymous_account_visit":   [],
    "suppressed_noise":          [],
}


def _uid(lead_type: str, source_key: str) -> str:
    h = hashlib.md5(f"{lead_type}:{source_key}".encode()).hexdigest()
    return f"classified:{lead_type}:{h}"


def _now() -> str:
    return datetime.utcnow().isoformat()


# ─── 1. Reclassify null-type Lead nodes ──────────────────────────────────────
def _reclassify_null_leads(neo: Neo4jClient) -> int:
    """
    Null-type CRM leads: classify based on message presence.
    Leads with a buyer message = known_account_interest.
    Leads without message but with statusId = engaged_account.
    Leads with neither = known_person_interest (placeholder).
    """
    rows = neo.run("""
        MATCH (l:Lead) WHERE l.lead_type IS NULL AND coalesce(l.score_override, false) = false
        SET l.lead_type = CASE
            WHEN l.message IS NOT NULL AND l.message <> '' THEN 'known_account_interest'
            WHEN l.statusId IS NOT NULL                    THEN 'engaged_account'
            ELSE 'known_person_interest'
        END,
        l.score_final    = CASE
            WHEN l.message IS NOT NULL AND l.message <> '' THEN 68
            WHEN l.statusId IS NOT NULL                    THEN 70
            ELSE 70
        END,
        l.visibility_level = CASE
            WHEN l.message IS NOT NULL AND l.message <> '' THEN 'feed'
            WHEN l.statusId IS NOT NULL                    THEN 'priority'
            ELSE 'feed'
        END,
        l.classified_at    = $now
        RETURN count(l) AS c
    """, {"now": _now()})
    return rows[0]["c"] if rows else 0


# ─── 2. Reclassify cold_market → active_importer ─────────────────────────────
def _reclassify_cold_market(neo: Neo4jClient) -> int:
    rows = neo.run("""
        MATCH (l:Lead {lead_type: 'cold_market'})
        SET l.lead_type        = 'active_importer',
            l.score_final      = 60,
            l.visibility_level = 'priority',
            l.classified_at    = $now
        RETURN count(l) AS c
    """, {"now": _now()})
    return rows[0]["c"] if rows else 0


# ─── 3. RFQ-based leads ──────────────────────────────────────────────────────
def _create_rfq_leads(neo: Neo4jClient) -> dict:
    rows = neo.run("""
        MATCH (r:RFQ)
        WHERE NOT EXISTS { MATCH (l:Lead {source_ref: 'rfq:' + r.rfqId}) }
        WITH r,
             CASE
               WHEN r.status IN ['processed', 'processing', 'health_check_process'] THEN 'rfq_submitted'
               WHEN r.status IN ['quoted', 'approved', 'closed_won']                THEN 'quote_ready'
               ELSE 'rfq_draft'
             END AS lt
        MERGE (l:Lead {lead_uid: 'classified:' + lt + ':rfq:' + r.rfqId})
        ON CREATE SET
            l.lead_type        = lt,
            l.score_final      = CASE lt WHEN 'rfq_submitted' THEN 85 WHEN 'quote_ready' THEN 90 ELSE 65 END,
            l.visibility_level = CASE lt WHEN 'rfq_submitted' THEN 'instant_alert' WHEN 'quote_ready' THEN 'instant_alert' ELSE 'push_notify' END,
            l.source           = 'goglo_crm',
            l.source_ref       = 'rfq:' + r.rfqId,
            l.synced_from_sql  = true,
            l.created_at       = $now,
            l.rfq_number       = r.rfqNumber,
            l.rfq_status       = r.status,
            l.classified_at    = $now
        MERGE (r)-[:GENERATES]->(l)
        RETURN count(l) AS c, lt
    """, {"now": _now()})
    counts = {}
    for r in rows:
        counts[r["lt"]] = counts.get(r["lt"], 0) + r["c"]
    return counts


# ─── 4. Meeting-based leads ───────────────────────────────────────────────────
def _create_meeting_leads(neo: Neo4jClient) -> dict:
    # engaged_account: meeting linked to a CRM lead
    rows = neo.run("""
        MATCH (m:Meeting) WHERE m.lead_id IS NOT NULL
        WITH m, 'meeting:' + toString(m.meetingId) AS src
        WHERE NOT EXISTS { MATCH (l:Lead {source_ref: src}) }
        MERGE (l:Lead {lead_uid: 'classified:engaged_account:' + src})
        ON CREATE SET
            l.lead_type        = 'engaged_account',
            l.score_final      = 70,
            l.visibility_level = 'priority',
            l.source           = 'goglo_crm',
            l.source_ref       = src,
            l.synced_from_sql  = true,
            l.created_at       = $now,
            l.meeting_title    = m.title,
            l.meeting_status   = m.status,
            l.meeting_date     = m.fromAt,
            l.classified_at    = $now
        MERGE (m)-[:GENERATES]->(l)
        RETURN count(l) AS c
    """, {"now": _now()})
    ea = rows[0]["c"] if rows else 0

    # engaged_person: meeting with a named host (known person context)
    rows2 = neo.run("""
        MATCH (m:Meeting)
        WHERE m.host_email IS NOT NULL AND m.host_email <> ''
          AND m.lead_id IS NULL
        WITH m, 'meeting_person:' + toString(m.meetingId) AS src
        WHERE NOT EXISTS { MATCH (l:Lead {source_ref: src}) }
        MERGE (l:Lead {lead_uid: 'classified:engaged_person:' + src})
        ON CREATE SET
            l.lead_type        = 'engaged_person',
            l.score_final      = 75,
            l.visibility_level = 'priority',
            l.source           = 'goglo_crm',
            l.source_ref       = src,
            l.synced_from_sql  = true,
            l.created_at       = $now,
            l.contact_email    = m.host_email,
            l.contact_name     = m.host_name,
            l.classified_at    = $now
        MERGE (m)-[:GENERATES]->(l)
        RETURN count(l) AS c
    """, {"now": _now()})
    ep = rows2[0]["c"] if rows2 else 0

    return {"engaged_account": ea, "engaged_person": ep}


# ─── 5. PageView → visit_only / anonymous_account_visit ──────────────────────
def _create_pageview_leads(neo: Neo4jClient) -> dict:
    # visit_only: one lead per unique SESSION for known users (account_hint present).
    # Sessions are the industry standard unit for "a visit" — same user in different
    # sessions = different buying events. This matches how the other team counts.
    rows = neo.run("""
        MATCH (sig:Signal {signal_type: 'page_view'})
        WHERE sig.account_hint IS NOT NULL AND sig.account_hint <> ''
        WITH sig.account_hint AS acct_hint,
             coalesce(sig.signalId, sig.signal_uid, sig.account_hint + ':' + sig.occurred_at) AS session_key,
             count(sig) AS pv_count,
             max(sig.occurred_at) AS last_seen,
             min(sig.occurred_at) AS first_seen
        WITH acct_hint, session_key, pv_count, last_seen, first_seen,
             'visit_only:sess:' + session_key AS src
        WHERE NOT EXISTS { MATCH (l:Lead {source_ref: src}) }
        MERGE (l:Lead {lead_uid: 'classified:visit_only:' + src})
        ON CREATE SET
            l.lead_type        = 'visit_only',
            l.score_final      = CASE WHEN pv_count >= 5 THEN 50 ELSE 40 END,
            l.visibility_level = CASE WHEN pv_count >= 5 THEN 'priority' ELSE 'count_only' END,
            l.source           = 'goglo_website',
            l.source_ref       = src,
            l.synced_from_sql  = true,
            l.account_hint     = acct_hint,
            l.pageview_count   = pv_count,
            l.last_seen        = last_seen,
            l.first_seen       = first_seen,
            l.classified_at    = $now
        RETURN count(l) AS c
    """, {"now": _now()})
    vo = rows[0]["c"] if rows else 0

    # anonymous_account_visit: GhostProfile/AnonymousVisitor sessions
    rows2 = neo.run("""
        MATCH (g:GhostProfile)
        WITH g, 'ghost:' + g.profileId AS src
        WHERE NOT EXISTS { MATCH (l:Lead {source_ref: src}) }
        MERGE (l:Lead {lead_uid: 'classified:anonymous_account_visit:' + src})
        ON CREATE SET
            l.lead_type        = 'anonymous_account_visit',
            l.score_final      = 30,
            l.visibility_level = 'watchlist',
            l.source           = 'goglo_website',
            l.source_ref       = src,
            l.synced_from_sql  = true,
            l.ghost_profile_id = g.profileId,
            l.identity_confidence = g.confidence,
            l.classified_at    = $now
        MERGE (g)-[:GENERATES]->(l)
        RETURN count(l) AS c
    """, {"now": _now()})
    anon = rows2[0]["c"] if rows2 else 0

    return {"visit_only": vo, "anonymous_account_visit": anon}


# ─── 6. Intent / hot_in_market ───────────────────────────────────────────────
def _create_intent_leads(neo: Neo4jClient) -> dict:
    # hot_in_market: news_trigger signals
    rows = neo.run("""
        MATCH (sig:Signal {signal_type: 'news_trigger_detected'})
        WITH sig.account_hint AS acct, count(sig) AS sig_count,
             max(sig.occurred_at) AS last_seen
        WHERE acct IS NOT NULL AND acct <> ''
        WITH acct, sig_count, last_seen, 'hot_in_market:acct:' + acct AS src
        WHERE NOT EXISTS { MATCH (l:Lead {source_ref: src}) }
        MERGE (l:Lead {lead_uid: 'classified:hot_in_market:' + src})
        ON CREATE SET
            l.lead_type        = 'hot_in_market',
            l.score_final      = 72,
            l.visibility_level = 'instant_alert',
            l.source           = 'goglo_intelligence',
            l.source_ref       = src,
            l.synced_from_sql  = true,
            l.account_hint     = acct,
            l.trigger_count    = sig_count,
            l.last_trigger     = last_seen,
            l.classified_at    = $now
        RETURN count(l) AS c
    """, {"now": _now()})
    hot = rows[0]["c"] if rows else 0

    # intent_only: third_party_intent signals
    rows2 = neo.run("""
        MATCH (sig:Signal {signal_type: 'third_party_intent_detected'})
        WITH sig.account_hint AS acct, max(sig.occurred_at) AS last_seen
        WHERE acct IS NOT NULL AND acct <> ''
        WITH acct, last_seen, 'intent_only:acct:' + acct AS src
        WHERE NOT EXISTS { MATCH (l:Lead {source_ref: src}) }
        MERGE (l:Lead {lead_uid: 'classified:intent_only:' + src})
        ON CREATE SET
            l.lead_type        = 'intent_only',
            l.score_final      = 55,
            l.visibility_level = 'feed',
            l.source           = 'goglo_intent',
            l.source_ref       = src,
            l.synced_from_sql  = true,
            l.account_hint     = acct,
            l.classified_at    = $now
        RETURN count(l) AS c
    """, {"now": _now()})
    intent = rows2[0]["c"] if rows2 else 0

    return {"hot_in_market": hot, "intent_only": intent}


# ─── 7. Trade signals → trade_buyer_candidate ────────────────────────────────
def _create_trade_leads(neo: Neo4jClient) -> int:
    rows = neo.run("""
        MATCH (sig:Signal {signal_type: 'active_importer_detected'})
        WHERE sig.account_hint IS NOT NULL OR sig.signalId IS NOT NULL
        WITH coalesce(sig.account_hint, sig.signalId) AS key, sig,
             'trade_buyer:' + coalesce(sig.account_hint, sig.signalId) AS src
        WHERE NOT EXISTS { MATCH (l:Lead {source_ref: src}) }
          AND NOT EXISTS {
              MATCH (l2:Lead)
              WHERE l2.lead_type IN ['active_importer', 'cold_market']
                AND l2.source_ref = src
          }
        MERGE (l:Lead {lead_uid: 'classified:trade_buyer_candidate:' + src})
        ON CREATE SET
            l.lead_type        = 'trade_buyer_candidate',
            l.score_final      = 62,
            l.visibility_level = 'priority',
            l.source           = 'goglo_trade',
            l.source_ref       = src,
            l.synced_from_sql  = true,
            l.classified_at    = $now
        MERGE (sig)-[:GENERATES]->(l)
        RETURN count(l) AS c
    """, {"now": _now()})
    return rows[0]["c"] if rows else 0


# ─── CRM source: crm_lead_buyer_details + lead_engagements → engaged_person ──
def _create_engaged_person_leads(neo: Neo4jClient, settings) -> int:
    """
    CRM crm_lead_buyer_details: named buyers engaged with a specific lead.
    CRM lead_engagements: persons who interacted with a product page.
    Both are person-level engagement signals → engaged_person.
    """
    import pymysql
    conn = pymysql.connect(
        host=settings.mysql_crm.host, port=settings.mysql_crm.port,
        user=settings.mysql_crm.user, password=settings.mysql_crm.password,
        database=settings.mysql_crm.database, charset="utf8mb4"
    )
    cur = conn.cursor()

    # Source 1: named buyer details attached to CRM leads
    cur.execute("""
        SELECT id, crm_lead_id, buyer_id, first_name, last_name,
               email, account_name, industry, country_id, authority,
               annual_revenue, created_at
        FROM crm_lead_buyer_details
        WHERE first_name IS NOT NULL AND first_name <> ''
    """)
    cols = [d[0] for d in cur.description]
    buyer_rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    # Source 2: product engagement events (person interacted with product)
    cur.execute("""
        SELECT id, lead_id, user_id, product_id, seller_id,
               total_time_spent, total_page_views, element_clicks,
               session_id, created_at
        FROM lead_engagements
        WHERE user_id IS NOT NULL AND user_id > 0
    """)
    cols2 = [d[0] for d in cur.description]
    eng_rows = [dict(zip(cols2, r)) for r in cur.fetchall()]
    conn.close()

    batch = []
    for r in buyer_rows:
        src = f"buyer_detail:{r['id']}"
        name = f"{r['first_name'] or ''} {r['last_name'] or ''}".strip()
        batch.append({
            "lead_uid":        f"classified:engaged_person:{src}",
            "lead_type":       "engaged_person",
            "score_final":     75,
            "visibility_level": "priority",
            "source":          "goglo_crm",
            "source_ref":      src,
            "contact_name":    name,
            "contact_email":   r["email"] or "",
            "company_name":    r["account_name"] or "",
            "industry":        r["industry"] or "",
            "country":         r["country_id"] or "",
            "authority":       r["authority"] or "",
            "buyer_id":        str(r["buyer_id"] or ""),
            "crm_lead_id":     str(r["crm_lead_id"] or ""),
        })

    for r in eng_rows:
        src = f"lead_engagement:{r['id']}"
        batch.append({
            "lead_uid":        f"classified:engaged_person:{src}",
            "lead_type":       "engaged_person",
            "score_final":     70,
            "visibility_level": "priority",
            "source":          "goglo_crm",
            "source_ref":      src,
            "contact_name":    "",
            "contact_email":   "",
            "company_name":    "",
            "industry":        "",
            "country":         "",
            "authority":       "",
            "buyer_id":        str(r["user_id"]),
            "crm_lead_id":     str(r["lead_id"] or ""),
        })

    if batch:
        neo.run("""
            UNWIND $rows AS row
            MERGE (l:Lead {lead_uid: row.lead_uid})
            ON CREATE SET
                l.lead_type        = row.lead_type,
                l.score_final      = row.score_final,
                l.visibility_level = row.visibility_level,
                l.source           = row.source,
                l.source_ref       = row.source_ref,
                l.synced_from_sql  = true,
                l.contact_name     = row.contact_name,
                l.contact_email    = row.contact_email,
                l.company_name     = row.company_name,
                l.buyer_industry   = row.industry,
                l.buyer_country    = row.country,
                l.authority        = row.authority,
                l.buyer_user_id    = row.buyer_id,
                l.crm_lead_id      = row.crm_lead_id,
                l.classified_at    = $now
        """, {"rows": batch, "now": _now()})

    return len(batch)


# ─── CRM source: quotation table → quote_ready ───────────────────────────────
def _create_quotation_leads(neo: Neo4jClient, settings) -> dict:
    """
    CRM quotation: actual price quotes sent by sellers to buyers.
    status=accepted → quote_ready (highest priority, buyer said yes)
    status=sent     → quote_ready (buyer has the quote in hand)
    status=initiated/draft → rfq_submitted (seller started quoting)
    """
    import pymysql
    conn = pymysql.connect(
        host=settings.mysql_crm.host, port=settings.mysql_crm.port,
        user=settings.mysql_crm.user, password=settings.mysql_crm.password,
        database=settings.mysql_crm.database, charset="utf8mb4"
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT id, productName, productId, leadId,
               sellerId, buyerId, status, currency,
               unitPrice, totalCost, created_at
        FROM quotation
    """)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.close()

    counts = {"quote_ready": 0, "rfq_submitted": 0}
    batch = []
    for r in rows:
        st = (r["status"] or "").lower()
        if st in ("accepted",):
            lt, score = "quote_ready", 95
        elif st in ("sent",):
            lt, score = "quote_ready", 85
        elif st in ("initiated", "draft"):
            lt, score = "rfq_submitted", 75
        else:
            lt, score = "rfq_submitted", 70

        src = f"quotation:{r['id']}"
        batch.append({
            "lead_uid":        f"classified:{lt}:{src}",
            "lead_type":       lt,
            "score_final":     score,
            "visibility_level": "instant_alert" if lt == "quote_ready" else "instant_alert",
            "source":          "goglo_crm",
            "source_ref":      src,
            "product_name":    (r["productName"] or "")[:200],
            "product_id":      str(r["productId"] or ""),
            "seller_id":       str(r["sellerId"] or ""),
            "buyer_id":        str(r["buyerId"] or ""),
            "currency":        r["currency"] or "",
            "total_cost":      float(r["totalCost"] or 0),
            "quotation_status": st,
        })
        counts[lt] = counts.get(lt, 0) + 1

    if batch:
        neo.run("""
            UNWIND $rows AS row
            MERGE (l:Lead {lead_uid: row.lead_uid})
            ON CREATE SET
                l.lead_type        = row.lead_type,
                l.score_final      = row.score_final,
                l.visibility_level = row.visibility_level,
                l.source           = row.source,
                l.source_ref       = row.source_ref,
                l.synced_from_sql  = true,
                l.product_name     = row.product_name,
                l.product_id       = row.product_id,
                l.seller_id        = row.seller_id,
                l.buyer_id         = row.buyer_id,
                l.currency         = row.currency,
                l.total_cost       = row.total_cost,
                l.quotation_status = row.quotation_status,
                l.classified_at    = $now
        """, {"rows": batch, "now": _now()})

    return counts


# ─── CRM2 source: enquiries → rfq_submitted / quote_ready / known_account_interest
def _create_enquiry_leads(neo: Neo4jClient, settings) -> dict:
    """
    CRM2 enquiries table: 20K+ real buyer-to-seller product inquiries.
    query   → rfq_submitted
    bid     → rfq_submitted (price negotiation)
    quote   → quote_ready
    seller created → engaged_account
    """
    import pymysql
    conn = pymysql.connect(
        host=settings.mysql_ui.host, port=settings.mysql_ui.port,
        user=settings.mysql_ui.user, password=settings.mysql_ui.password,
        database=settings.mysql_ui.database, charset="utf8mb4"
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT id, enquiry_user_id, seller_user_id, product_id,
               enquiry_type, status, message, country, industry,
               competitor, purpose_of_inquiry, created_at
        FROM enquiries
        WHERE deleted_at IS NULL
    """)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.close()

    counts = {"rfq_submitted": 0, "quote_ready": 0, "engaged_account": 0}
    batch = []
    for r in rows:
        etype = (r["enquiry_type"] or "").strip().lower()
        if etype in ("quote",):
            lt = "quote_ready"
            score = 90
        elif etype in ("bid",):
            lt = "rfq_submitted"
            score = 88
        elif etype == "seller created":
            lt = "engaged_account"
            score = 70
        else:
            lt = "rfq_submitted"
            score = 85

        src = f"enquiry:{r['id']}"
        batch.append({
            "lead_uid":         f"classified:{lt}:{src}",
            "lead_type":        lt,
            "score_final":      score,
            "visibility_level": "instant_alert" if lt == "quote_ready" else ("instant_alert" if lt == "rfq_submitted" else "priority"),
            "source":           "goglo_crm2",
            "source_ref":       src,
            "buyer_user_id":    str(r["enquiry_user_id"] or ""),
            "seller_user_id":   str(r["seller_user_id"] or ""),
            "product_id":       str(r["product_id"] or ""),
            "message":          (r["message"] or "")[:500],
            "buyer_country":    r["country"] or "",
            "buyer_industry":   r["industry"] or "",
            "competitor":       r["competitor"] or "",
            "enquiry_type":     etype,
        })
        counts[lt] = counts.get(lt, 0) + 1

    if batch:
        neo.run("""
            UNWIND $rows AS row
            MERGE (l:Lead {lead_uid: row.lead_uid})
            ON CREATE SET
                l.lead_type        = row.lead_type,
                l.score_final      = row.score_final,
                l.visibility_level = row.visibility_level,
                l.source           = row.source,
                l.source_ref       = row.source_ref,
                l.synced_from_sql  = true,
                l.buyer_user_id    = row.buyer_user_id,
                l.seller_user_id   = row.seller_user_id,
                l.product_id       = row.product_id,
                l.message          = row.message,
                l.buyer_country    = row.buyer_country,
                l.buyer_industry   = row.buyer_industry,
                l.competitor       = row.competitor,
                l.enquiry_type     = row.enquiry_type,
                l.classified_at    = $now
        """, {"rows": batch, "now": _now()})

    return counts


# ─── CRM2 source: tracking_page_views → visit_only (product-specific)
def _create_product_pageview_leads(neo: Neo4jClient, settings) -> dict:
    """
    CRM2 tracking_page_views: real buyers browsing GoGlo.com products.
    One visit_only lead per unique session — product-specific.
    """
    import pymysql
    conn = pymysql.connect(
        host=settings.mysql_ui.host, port=settings.mysql_ui.port,
        user=settings.mysql_ui.user, password=settings.mysql_ui.password,
        database=settings.mysql_ui.database, charset="utf8mb4"
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT tpv.session_id, ts.user_id,
               COUNT(tpv.id) AS pv_count,
               MAX(tpv.created_at) AS last_seen,
               MIN(tpv.created_at) AS first_seen,
               SUM(CASE WHEN tpv.type = 'ProductDetail' THEN 1 ELSE 0 END) AS product_views,
               GROUP_CONCAT(DISTINCT tpv.type ORDER BY tpv.type SEPARATOR ',') AS page_types
        FROM tracking_page_views tpv
        LEFT JOIN tracking_sessions ts ON ts.session_id = tpv.session_id
        WHERE tpv.deleted_at IS NULL
        GROUP BY tpv.session_id, ts.user_id
    """)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.close()

    visit_batch, anon_batch = [], []
    for r in rows:
        src = f"crm2_session:{r['session_id']}"
        uid = int(r["user_id"] or 0)
        is_known = uid > 0
        pv_count = int(r["pv_count"] or 0)
        score = 50 if (pv_count >= 3 or int(r.get("product_views") or 0) >= 1) else 40

        node = {
            "lead_uid":         f"classified:{'visit_only' if is_known else 'anonymous_account_visit'}:{src}",
            "lead_type":        "visit_only" if is_known else "anonymous_account_visit",
            "score_final":      score if is_known else 30,
            "visibility_level": "priority" if (is_known and pv_count >= 3) else ("count_only" if is_known else "watchlist"),
            "source":           "goglo_website_crm2",
            "source_ref":       src,
            "session_id":       str(r["session_id"]),
            "user_id":          str(uid),
            "pageview_count":   pv_count,
            "product_views":    int(r.get("product_views") or 0),
            "page_types":       r.get("page_types") or "",
            "last_seen":        str(r["last_seen"] or ""),
            "first_seen":       str(r["first_seen"] or ""),
        }
        if is_known:
            visit_batch.append(node)
        else:
            anon_batch.append(node)

    q = """
        UNWIND $rows AS row
        MERGE (l:Lead {lead_uid: row.lead_uid})
        ON CREATE SET
            l.lead_type        = row.lead_type,
            l.score_final      = row.score_final,
            l.visibility_level = row.visibility_level,
            l.source           = row.source,
            l.source_ref       = row.source_ref,
            l.synced_from_sql  = true,
            l.session_id       = row.session_id,
            l.user_id          = row.user_id,
            l.pageview_count   = row.pageview_count,
            l.product_views    = row.product_views,
            l.page_types       = row.page_types,
            l.last_seen        = row.last_seen,
            l.first_seen       = row.first_seen,
            l.classified_at    = $now
    """
    if visit_batch:
        neo.run(q, {"rows": visit_batch, "now": _now()})
    if anon_batch:
        neo.run(q, {"rows": anon_batch, "now": _now()})

    return {"visit_only": len(visit_batch), "anonymous_account_visit": len(anon_batch)}


# ─── CRM2 source: searched_keywords_list → intent_only
def _create_search_intent_leads(neo: Neo4jClient, settings) -> int:
    """
    CRM2 searched_keywords_list: buyers who searched specific terms.
    Each unique user+keyword = intent_only lead.
    """
    import pymysql
    conn = pymysql.connect(
        host=settings.mysql_ui.host, port=settings.mysql_ui.port,
        user=settings.mysql_ui.user, password=settings.mysql_ui.password,
        database=settings.mysql_ui.database, charset="utf8mb4"
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT user_id, name AS keyword, ANY_VALUE(category_id) AS category_id,
               SUM(searched_total) AS total_searches,
               MAX(created_at) AS last_searched
        FROM searched_keywords_list
        WHERE deleted_at IS NULL AND user_id IS NOT NULL AND user_id > 0
        GROUP BY user_id, name
    """)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.close()

    batch = []
    for r in rows:
        src = f"search:{r['user_id']}:{(r['keyword'] or '')[:40]}"
        batch.append({
            "lead_uid":       f"classified:intent_only:{src}",
            "lead_type":      "intent_only",
            "score_final":    55 + min(int(r["total_searches"] or 0) * 2, 20),
            "visibility_level": "feed",
            "source":         "goglo_search",
            "source_ref":     src,
            "user_id":        str(r["user_id"]),
            "keyword":        r["keyword"] or "",
            "category_id":    str(r["category_id"] or ""),
            "search_count":   int(r["total_searches"] or 0),
            "last_searched":  str(r["last_searched"] or ""),
        })

    if batch:
        neo.run("""
            UNWIND $rows AS row
            MERGE (l:Lead {lead_uid: row.lead_uid})
            ON CREATE SET
                l.lead_type        = row.lead_type,
                l.score_final      = row.score_final,
                l.visibility_level = row.visibility_level,
                l.source           = row.source,
                l.source_ref       = row.source_ref,
                l.synced_from_sql  = true,
                l.user_id          = row.user_id,
                l.search_keyword   = row.keyword,
                l.category_id      = row.category_id,
                l.search_count     = row.search_count,
                l.last_searched    = row.last_searched,
                l.classified_at    = $now
        """, {"rows": batch, "now": _now()})

    return len(batch)


# ─── 8. Enquiries with competitor field → competitor_displacement ─────────────
def _create_competitor_displacement_leads(neo: Neo4jClient, settings) -> int:
    """
    CRM2 enquiries where buyer explicitly names a competitor they are
    currently buying from. This is the strongest supplier-switch signal
    on the platform — buyer is comparing us against a named rival.
    Source: enquiries.competitor (non-null, non-empty)
    """
    import pymysql
    conn = pymysql.connect(
        host=settings.mysql_ui.host, port=settings.mysql_ui.port,
        user=settings.mysql_ui.user, password=settings.mysql_ui.password,
        database=settings.mysql_ui.database, charset="utf8mb4"
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT id, enquiry_user_id, seller_user_id, product_id,
               enquiry_type, competitor, message, country, created_at
        FROM enquiries
        WHERE deleted_at IS NULL
          AND competitor IS NOT NULL
          AND TRIM(competitor) <> ''
    """)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.close()

    batch = []
    for r in rows:
        src = f"competitor_enquiry:{r['id']}"
        batch.append({
            "lead_uid":         f"classified:competitor_displacement:{src}",
            "lead_type":        "competitor_displacement",
            "score_final":      80,
            "visibility_level": "push_notify",
            "source":           "goglo_crm2",
            "source_ref":       src,
            "buyer_user_id":    str(r["enquiry_user_id"] or ""),
            "seller_user_id":   str(r["seller_user_id"] or ""),
            "product_id":       str(r["product_id"] or ""),
            "current_supplier": (r["competitor"] or "")[:200],
            "message":          (r["message"] or "")[:500],
            "buyer_country":    r["country"] or "",
            "enquiry_type":     (r["enquiry_type"] or "").lower(),
        })

    if batch:
        neo.run("""
            UNWIND $rows AS row
            MERGE (l:Lead {lead_uid: row.lead_uid})
            ON CREATE SET
                l.lead_type         = row.lead_type,
                l.score_final       = row.score_final,
                l.visibility_level  = row.visibility_level,
                l.source            = row.source,
                l.source_ref        = row.source_ref,
                l.synced_from_sql   = true,
                l.buyer_user_id     = row.buyer_user_id,
                l.seller_user_id    = row.seller_user_id,
                l.product_id        = row.product_id,
                l.current_supplier  = row.current_supplier,
                l.message           = row.message,
                l.buyer_country     = row.buyer_country,
                l.enquiry_type      = row.enquiry_type,
                l.classified_at     = $now
        """, {"rows": batch, "now": _now()})

    return len(batch)


# ─── 9. High-value TradeRelationship accounts → strategic_account_watch ───────
def _create_strategic_account_watch_leads(neo: Neo4jClient) -> int:
    """
    Strategic accounts: buyers with large documented trade volumes (from
    TradeRelationship nodes) who are NOT yet active on the GoGlo platform
    (no existing high-priority Lead). These are tier-1 prospects worth
    monitoring closely and approaching with a tailored pitch.

    Criteria:
      - TradeRelationship with buyer_monthly_volume >= 10000 (significant importer)
      - No existing Lead with score_final >= 70 for this buyer
      - Not already a SwitchLead (they already have a higher-priority classification)
    """
    rows = neo.run("""
        MATCH (tr:TradeRelationship)
        WHERE tr.buyer_monthly_volume IS NOT NULL
          AND tr.buyer_monthly_volume >= 10000
        WITH tr.buyer_org_name AS org_name,
             tr.buyer_org_id   AS org_id,
             max(tr.buyer_monthly_volume) AS max_vol,
             max(tr.health_score)         AS health_score,
             count(tr)                    AS tr_count
        WHERE org_name IS NOT NULL AND org_name <> ''
        AND NOT EXISTS {
            MATCH (l:Lead)
            WHERE (l.buyer_org_id = org_id OR l.company_name = org_name)
              AND l.score_final >= 70
        }
        AND NOT EXISTS {
            MATCH (sl:SwitchLead)
            WHERE sl.buyer_org_id = org_id OR sl.buyer_org_name = org_name
        }
        WITH org_name, org_id, max_vol, health_score, tr_count,
             'strategic_watch:' + coalesce(org_id, org_name) AS src
        WHERE NOT EXISTS { MATCH (l:Lead {source_ref: src}) }
        MERGE (l:Lead {lead_uid: 'classified:strategic_account_watch:' + src})
        ON CREATE SET
            l.lead_type         = 'strategic_account_watch',
            l.score_final       = 65,
            l.visibility_level  = 'watchlist',
            l.source            = 'goglo_trade',
            l.source_ref        = src,
            l.synced_from_sql   = true,
            l.company_name      = org_name,
            l.buyer_org_id      = org_id,
            l.buyer_monthly_volume = max_vol,
            l.trade_relationship_count = tr_count,
            l.trade_health_score = health_score,
            l.classified_at     = $now
        RETURN count(l) AS c
    """, {"now": _now()})
    return rows[0]["c"] if rows else 0


# ─── 10. reactivation_candidate: re-engaged closed deals ─────────────────────
def _create_reactivation_candidates(neo: Neo4jClient, settings) -> int:
    import pymysql
    import pymysql.cursors
    cfg = settings.mysql_crm
    conn = pymysql.connect(
        host=cfg.host, port=cfg.port, user=cfg.user, password=cfg.password,
        charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, deal_name, deal_owner, contact_id, amount,
                       deal_creation_source, created_at, updated_at
                FROM crm.deals
                WHERE updated_at > DATE_SUB(NOW(), INTERVAL 30 DAY)
                  AND created_at < DATE_SUB(NOW(), INTERVAL 90 DAY)
            """)
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return 0

    batch = []
    for r in rows:
        src = f"deal:{r['id']}"
        batch.append({
            "lead_uid":        f"classified:reactivation_candidate:{src}",
            "lead_type":       "reactivation_candidate",
            "score_final":     72,
            "visibility_level": "push_notify",
            "source":          "goglo_crm",
            "source_ref":      src,
            "deal_name":       (r.get("deal_name") or "")[:200],
            "deal_owner":      str(r.get("deal_owner") or ""),
            "contact_id":      str(r.get("contact_id") or ""),
            "deal_amount":     float(r.get("amount") or 0),
            "playbook_tags":   ["champion_reengagement", "contact_moved_to_new_company"],
        })

    neo.run("""
        UNWIND $rows AS row
        MERGE (l:Lead {lead_uid: row.lead_uid})
        ON CREATE SET
            l.lead_type        = row.lead_type,
            l.score_final      = row.score_final,
            l.visibility_level = row.visibility_level,
            l.source           = row.source,
            l.source_ref       = row.source_ref,
            l.synced_from_sql  = true,
            l.deal_name        = row.deal_name,
            l.deal_owner       = row.deal_owner,
            l.contact_id       = row.contact_id,
            l.deal_amount      = row.deal_amount,
            l.playbook_tags    = row.playbook_tags,
            l.classified_at    = $now
    """, {"rows": batch, "now": _now()})
    return len(batch)


# ─── 11. partner_chain_opportunity: resellers / distributors ─────────────────
def _create_partner_chain_opportunities(neo: Neo4jClient, settings) -> int:
    import pymysql
    import pymysql.cursors
    cfg = settings.mysql_crm
    conn = pymysql.connect(
        host=cfg.host, port=cfg.port, user=cfg.user, password=cfg.password,
        charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, enquiry_user_id, seller_user_id, product_id,
                       purpose_of_inquiry, message, country, created_at
                FROM goglo_staging.enquiries
                WHERE deleted_at IS NULL
                  AND (
                    LOWER(COALESCE(purpose_of_inquiry,'')) LIKE '%resell%'
                    OR LOWER(COALESCE(purpose_of_inquiry,'')) LIKE '%distribut%'
                    OR LOWER(COALESCE(purpose_of_inquiry,'')) LIKE '%wholesale%'
                    OR LOWER(COALESCE(message,'')) LIKE '%resell%'
                    OR LOWER(COALESCE(message,'')) LIKE '%distribut%'
                    OR LOWER(COALESCE(message,'')) LIKE '%wholesale%'
                  )
            """)
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return 0

    batch = []
    for r in rows:
        purpose = (r.get("purpose_of_inquiry") or "").lower()
        msg = (r.get("message") or "").lower()
        if "distribut" in purpose or "distribut" in msg:
            partner_type = "distributor"
        elif "wholesale" in purpose or "wholesale" in msg:
            partner_type = "wholesaler"
        else:
            partner_type = "reseller"

        src = f"enquiry_partner:{r['id']}"
        batch.append({
            "lead_uid":        f"classified:partner_chain_opportunity:{src}",
            "lead_type":       "partner_chain_opportunity",
            "score_final":     68,
            "visibility_level": "feed",
            "source":          "goglo_crm2",
            "source_ref":      src,
            "buyer_user_id":   str(r.get("enquiry_user_id") or ""),
            "seller_user_id":  str(r.get("seller_user_id") or ""),
            "product_id":      str(r.get("product_id") or ""),
            "partner_type":    partner_type,
            "buyer_country":   r.get("country") or "",
            "playbook_tags":   ["ecosystem_play", "cross_sell"],
        })

    neo.run("""
        UNWIND $rows AS row
        MERGE (l:Lead {lead_uid: row.lead_uid})
        ON CREATE SET
            l.lead_type        = row.lead_type,
            l.score_final      = row.score_final,
            l.visibility_level = row.visibility_level,
            l.source           = row.source,
            l.source_ref       = row.source_ref,
            l.synced_from_sql  = true,
            l.buyer_user_id    = row.buyer_user_id,
            l.seller_user_id   = row.seller_user_id,
            l.product_id       = row.product_id,
            l.partner_type     = row.partner_type,
            l.buyer_country    = row.buyer_country,
            l.playbook_tags    = row.playbook_tags,
            l.classified_at    = $now
    """, {"rows": batch, "now": _now()})
    return len(batch)


# ─── 12. suppressed_noise: risk flags → suppress from seller routing ──────────
def _create_suppressed_noise_leads(neo: Neo4jClient, settings) -> int:
    import pymysql
    import pymysql.cursors
    cfg = settings.mysql_crm
    conn = pymysql.connect(
        host=cfg.host, port=cfg.port, user=cfg.user, password=cfg.password,
        charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, account_uid, risk_type, severity, source, reference_id, created_at
                FROM crm.account_risk_flags
                WHERE risk_type IN (
                    'bot', 'spam', 'fake_rfq', 'duplicate',
                    'tracking_anomaly', 'compliance_risk', 'do_not_contact'
                )
            """)
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return 0

    batch = []
    for r in rows:
        src = f"risk_flag:{r['id']}"
        batch.append({
            "lead_uid":          f"classified:suppressed_noise:{src}",
            "lead_type":         "suppressed_noise",
            "score_final":       10,
            "visibility_level":  "count_only",
            "source":            "crm_risk",
            "source_ref":        src,
            "account_uid":       str(r.get("account_uid") or ""),
            "suppression_reason": r.get("risk_type") or "unknown",
            "risk_severity":     r.get("severity") or "medium",
        })

    neo.run("""
        UNWIND $rows AS row
        MERGE (l:Lead {lead_uid: row.lead_uid})
        ON CREATE SET
            l.lead_type         = row.lead_type,
            l.score_final       = row.score_final,
            l.visibility_level  = row.visibility_level,
            l.source            = row.source,
            l.source_ref        = row.source_ref,
            l.synced_from_sql   = true,
            l.account_uid       = row.account_uid,
            l.suppressed        = true,
            l.suppression_reason = row.suppression_reason,
            l.risk_severity     = row.risk_severity,
            l.distribution_status = 'suppressed',
            l.seller_visible    = false,
            l.classified_at     = $now
    """, {"rows": batch, "now": _now()})
    return len(batch)


# ─── 13. active_exporter: companies with strong export activity ───────────────
def _create_active_exporter_leads(neo: Neo4jClient) -> int:
    rows = neo.run("""
        MATCH (tr:TradeRelationship)
        WHERE tr.supplier_org_name IS NOT NULL AND tr.supplier_org_name <> ''
        WITH tr.supplier_org_name AS org_name,
             tr.supplier_org_id   AS org_id,
             count(tr)            AS tr_count,
             avg(tr.health_score) AS avg_health,
             sum(tr.shipment_count) AS total_shipments
        WHERE tr_count >= 3
        AND NOT EXISTS {
            MATCH (s:Seller)
            WHERE s.name = org_name OR s.org_id = org_id
        }
        AND NOT EXISTS {
            MATCH (l:Lead {lead_type: 'active_exporter'})
            WHERE l.company_name = org_name OR l.buyer_org_id = org_id
        }
        WITH org_name, org_id, tr_count, avg_health, total_shipments,
             'active_exporter:' + coalesce(org_id, org_name) AS src
        WHERE NOT EXISTS { MATCH (l:Lead {source_ref: src}) }
        MERGE (l:Lead {lead_uid: 'classified:active_exporter:' + src})
        ON CREATE SET
            l.lead_type              = 'active_exporter',
            l.score_final            = 60,
            l.visibility_level       = 'feed',
            l.source                 = 'goglo_trade',
            l.source_ref             = src,
            l.synced_from_sql        = true,
            l.company_name           = org_name,
            l.buyer_org_id           = org_id,
            l.trade_relationship_count = tr_count,
            l.avg_trade_health       = avg_health,
            l.total_shipments        = total_shipments,
            l.playbook_tags          = ['ecosystem_play', 'strategic_consolidation'],
            l.classified_at          = $now
        RETURN count(l) AS c
    """, {"now": _now()})
    return rows[0]["c"] if rows else 0


# ─── Playbook tag assignment ──────────────────────────────────────────────────
def _assign_playbook_tags(neo: Neo4jClient):
    """Assign playbook_tags to all leads that don't have them yet."""
    tag_rows = [
        {"lead_type": lt, "tags": tags}
        for lt, tags in PLAYBOOK_TAGS.items()
        if tags  # skip empty tag lists
    ]
    if tag_rows:
        neo.run("""
            UNWIND $rows AS row
            MATCH (l:Lead {lead_type: row.lead_type})
            WHERE l.playbook_tags IS NULL OR size(l.playbook_tags) = 0
            SET l.playbook_tags = row.tags
        """, {"rows": tag_rows})


# ─── Real-time event handlers (called by KafkaEventConsumer) ─────────────────

def run_single_rfq(neo: Neo4jClient, payload: dict):
    """
    Process a single RFQ event from Kafka (crm.rfq_submitted topic).
    Payload is a raw row from crm.rfqs or goglo_staging.enquiries.
    Creates / updates a Lead node for this RFQ in Neo4j.
    """
    rfq_id = str(
        payload.get("rfq_id") or payload.get("id") or payload.get("rfqId") or ""
    )
    if not rfq_id:
        logger.warning("run_single_rfq: no rfq_id in payload, skipping")
        return

    source_table = payload.get("_source_table", "crm.rfqs")
    status = str(payload.get("status") or "").lower()

    if "enquir" in source_table:
        etype = str(payload.get("enquiry_type") or "").lower()
        if etype == "quote":
            lt, score = "quote_ready", 90
        elif etype == "bid":
            lt, score = "rfq_submitted", 88
        else:
            lt, score = "rfq_submitted", 85
        src = f"enquiry:{rfq_id}"
    else:
        if status in ("quoted", "approved", "closed_won"):
            lt, score = "quote_ready", 90
        elif status in ("processed", "processing", "health_check_process"):
            lt, score = "rfq_submitted", 85
        else:
            lt, score = "rfq_draft", 65
        src = f"rfq:{rfq_id}"

    neo.run("""
        MERGE (l:Lead {source_ref: $src})
        ON CREATE SET
            l.lead_uid        = 'classified:' + $lt + ':' + $src,
            l.lead_type       = $lt,
            l.score_final     = $score,
            l.visibility_level = CASE $lt
                WHEN 'quote_ready'   THEN 'instant_alert'
                WHEN 'rfq_submitted' THEN 'instant_alert'
                ELSE 'push_notify'
            END,
            l.source          = 'goglo_crm',
            l.source_ref      = $src,
            l.synced_from_sql = true,
            l.classified_at   = $now,
            l.created_at      = $now
        ON MATCH SET
            l.lead_type       = $lt,
            l.score_final     = $score,
            l.classified_at   = $now
    """, {"lt": lt, "score": score, "src": src, "now": _now()})

    logger.info(f"run_single_rfq: upserted Lead {src} as {lt} (score={score})")


def reclassify_lead(neo: Neo4jClient, payload: dict):
    """
    Re-classify an existing Lead after a CRM status update (crm.lead_updates topic).
    Payload is a raw row from crm.crm_leads or crm.lead_master.
    """
    lead_id = str(
        payload.get("lead_id") or payload.get("id") or payload.get("leadId") or ""
    )
    if not lead_id:
        logger.warning("reclassify_lead: no lead_id in payload, skipping")
        return

    status = str(payload.get("status") or payload.get("statusId") or "").lower()
    message = str(payload.get("message") or "")

    # Determine new classification
    if status in ("converted", "won", "closed_won"):
        lt, score = "quote_ready", 90
    elif message:
        lt, score = "known_account_interest", 68
    elif status:
        lt, score = "engaged_account", 70
    else:
        lt, score = "known_person_interest", 68

    src = f"crm_lead:{lead_id}"
    neo.run("""
        MERGE (l:Lead {source_ref: $src})
        ON CREATE SET
            l.lead_uid        = 'classified:' + $lt + ':' + $src,
            l.lead_type       = $lt,
            l.score_final     = $score,
            l.visibility_level = CASE $lt
                WHEN 'quote_ready'            THEN 'instant_alert'
                WHEN 'engaged_account'        THEN 'priority'
                WHEN 'known_account_interest' THEN 'feed'
                ELSE 'feed'
            END,
            l.source          = 'goglo_crm',
            l.source_ref      = $src,
            l.synced_from_sql = true,
            l.classified_at   = $now,
            l.created_at      = $now
        ON MATCH SET
            l.lead_type       = $lt,
            l.score_final     = $score,
            l.classified_at   = $now
    """, {"lt": lt, "score": score, "src": src, "now": _now()})

    logger.info(f"reclassify_lead: upserted Lead {src} as {lt} (score={score})")


# ─── main orchestrator ────────────────────────────────────────────────────────
def run(config_path: str = "config.yaml") -> dict:
    banner("GoGlo Platform Lead Classifier")
    from app.core.config import load_settings
    settings = load_settings(config_path)
    neo = Neo4jClient(settings.neo4j.uri, settings.neo4j.user, settings.neo4j.password)

    results = {}
    try:
        info("Step 1: Reclassifying null-type CRM leads...")
        c = _reclassify_null_leads(neo)
        results["reclassified_null"] = c
        ok(f"  → {c} null leads classified (known_account_interest / engaged_account)")

        info("Step 2: Reclassifying cold_market → active_importer...")
        c = _reclassify_cold_market(neo)
        results["reclassified_cold_market"] = c
        ok(f"  → {c} cold_market leads → active_importer")

        info("Step 3: Creating RFQ-based leads (CRM)...")
        rfq_counts = _create_rfq_leads(neo)
        results.update(rfq_counts)
        ok(f"  → {rfq_counts}")

        info("Step 4: Creating Meeting-based leads...")
        mtg_counts = _create_meeting_leads(neo)
        results.update(mtg_counts)
        ok(f"  → {mtg_counts}")

        info("Step 5: Creating PageView leads (CRM graph signals)...")
        pv_counts = _create_pageview_leads(neo)
        results.update(pv_counts)
        ok(f"  → visit_only={pv_counts.get('visit_only', 0)}, anonymous={pv_counts.get('anonymous_account_visit', 0)}")

        info("Step 6: Creating Intent / Hot-in-Market leads...")
        intent_counts = _create_intent_leads(neo)
        results.update(intent_counts)
        ok(f"  → {intent_counts}")

        info("Step 7: Creating trade_buyer_candidate leads...")
        c = _create_trade_leads(neo)
        results["trade_buyer_candidate"] = c
        ok(f"  → {c} trade buyer candidates")

        info("Step 8a: Creating engaged_person leads (buyer details + engagements)...")
        c = _create_engaged_person_leads(neo, settings)
        results["engaged_person"] = results.get("engaged_person", 0) + c
        ok(f"  → {c} engaged person leads")

        info("Step 8b: Creating quote_ready leads from CRM quotation table...")
        qt_counts = _create_quotation_leads(neo, settings)
        for k, v in qt_counts.items():
            results[k] = results.get(k, 0) + v
        ok(f"  → {qt_counts}")

        # ── CRM2 sources ──────────────────────────────────────────────────────
        info("Step 8: Creating enquiry leads from CRM2 (20K+ buyer inquiries)...")
        enq_counts = _create_enquiry_leads(neo, settings)
        for k, v in enq_counts.items():
            results[k] = results.get(k, 0) + v
        ok(f"  → {enq_counts}")

        info("Step 9: Creating product page-view leads from CRM2 tracking...")
        crm2_pv = _create_product_pageview_leads(neo, settings)
        for k, v in crm2_pv.items():
            results[k] = results.get(k, 0) + v
        ok(f"  → visit_only={crm2_pv.get('visit_only', 0)}, anonymous={crm2_pv.get('anonymous_account_visit', 0)}")

        info("Step 10: Creating search-intent leads from CRM2 keyword searches...")
        c = _create_search_intent_leads(neo, settings)
        results["intent_only"] = results.get("intent_only", 0) + c
        ok(f"  → {c} search-intent leads")

        info("Step 11: Creating competitor_displacement leads (buyer named a rival)...")
        c = _create_competitor_displacement_leads(neo, settings)
        results["competitor_displacement"] = c
        ok(f"  → {c} competitor displacement leads")

        info("Step 12: Creating strategic_account_watch leads (high-volume trade accounts)...")
        c = _create_strategic_account_watch_leads(neo)
        results["strategic_account_watch"] = c
        ok(f"  → {c} strategic account watch leads")

        info("Step 13: Creating reactivation_candidate leads (re-engaged closed deals)...")
        c = _create_reactivation_candidates(neo, settings)
        results["reactivation_candidate"] = c
        ok(f"  → {c} reactivation candidates")

        info("Step 14: Creating partner_chain_opportunity leads (resellers/distributors)...")
        c = _create_partner_chain_opportunities(neo, settings)
        results["partner_chain_opportunity"] = c
        ok(f"  → {c} partner chain opportunities")

        info("Step 15: Creating suppressed_noise leads (account risk flags)...")
        c = _create_suppressed_noise_leads(neo, settings)
        results["suppressed_noise"] = c
        ok(f"  → {c} suppressed noise leads flagged")

        info("Step 16: Creating active_exporter leads (trade data)...")
        c = _create_active_exporter_leads(neo)
        results["active_exporter"] = c
        ok(f"  → {c} active exporter leads")

        info("Step 17: Assigning playbook tags...")
        _assign_playbook_tags(neo)
        ok("  → playbook tags assigned")

        # Final count
        final = neo.run("MATCH (l:Lead) RETURN coalesce(l.lead_type,'null') AS lt, count(l) AS c ORDER BY c DESC")
        ok("\n=== Final Lead Type Distribution ===")
        total = 0
        for r in final:
            info(f"  {r['lt']}: {r['c']}")
            total += r["c"]
        switch_count = neo.run("MATCH (l:SwitchLead) RETURN count(l) AS c")[0]["c"]
        ok(f"  [SwitchLead]: {switch_count}")
        ok(f"  TOTAL: {total + switch_count}")

    finally:
        neo.close()

    return results
