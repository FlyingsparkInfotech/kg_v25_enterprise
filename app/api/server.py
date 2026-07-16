"""
KG V25.2 Switch Lead API
FastAPI server exposing enriched supplier switch leads, seller dashboard,
feedback recording, and credit management.
"""

import os
import psycopg2
from typing import Optional
from fastapi import FastAPI, HTTPException, Query, Body
from pydantic import BaseModel, Field
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from app.core.config import load_settings
from app.db.neo4j_client import Neo4jClient
from app.api.lead_service import LeadService
from app.api.seller_service import SellerService
from app.api.intelligence_service import IntelligenceService

app = FastAPI(
    title='KG V25.2 Switch Lead API',
    description='Supplier switch leads, seller dashboard, feedback, and credit management.',
    version='25.2.0',
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=['*'],
    allow_headers=['*'],
)

# Serve the React/HTML dashboard from app/static/
_static_dir = os.path.join(os.path.dirname(__file__), '..', 'static')
if os.path.isdir(_static_dir):
    app.mount('/static', StaticFiles(directory=_static_dir), name='static')


@app.get('/ui', include_in_schema=False)
def dashboard_ui():
    """Serve the lead intelligence dashboard."""
    index = os.path.join(_static_dir, 'index.html')
    return FileResponse(index)

# ── lazy singletons ───────────────────────────────────────────────────────────
_settings = None
_neo      = None
_pg_conn  = None


def _get_neo():
    global _settings, _neo
    if _settings is None:
        _settings = load_settings('config.yaml')
    if _neo is None:
        _neo = Neo4jClient(
            _settings.neo4j.uri,
            _settings.neo4j.user,
            _settings.neo4j.password,
        )
    return _neo


def _get_pg():
    global _settings, _pg_conn
    if _settings is None:
        _settings = load_settings('config.yaml')
    if _pg_conn is None or _pg_conn.closed:
        pg = _settings.postgres
        _pg_conn = psycopg2.connect(
            host=pg.host, port=pg.port,
            user=pg.user, password=pg.password,
            dbname=pg.database,
        )
    return _pg_conn


def _get_service() -> LeadService:
    return LeadService(_get_neo(), _get_pg())


def _get_seller_service() -> SellerService:
    return SellerService(_get_neo())


def _get_intelligence_service() -> IntelligenceService:
    return IntelligenceService(_get_neo(), _get_pg())


# ── endpoints ─────────────────────────────────────────────────────────────────

@app.get('/')
def root():
    return {'service': 'KG V25.2 Switch Lead API', 'status': 'ok', 'version': '25.2.0'}


@app.get('/summary')
def summary():
    """Graph-level stats — total leads, relationships monitored, stressed buyers."""
    return _get_service().get_summary()


@app.get('/leads')
def list_leads(
    limit:       int   = Query(default=20, ge=1, le=200, description='Max leads to return'),
    min_score:   float = Query(default=0.0, ge=0, le=100, description='Minimum final_score'),
    deduplicate: bool  = Query(default=False, description='Return only the top-ranked supplier per unique buyer'),
):
    """
    List all switch leads ordered by score, each enriched with:
    - Buyer + supplier info
    - Trade volume and HS code description
    - Stress signals and switch probability
    - Decision-maker contacts from ZoomInfo
    - Recommended action steps

    Set deduplicate=true to return only the best candidate supplier per buyer.
    """
    return _get_service().get_leads(limit=limit, min_score=min_score, deduplicate=deduplicate)


@app.get('/leads/{lead_id}')
def get_lead(lead_id: str):
    """Get a single enriched lead by ID."""
    lead = _get_service().get_lead(lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail=f'Lead {lead_id} not found')
    return lead


@app.get('/leads/top/urgent')
def top_urgent(
    limit:       int  = Query(default=10, ge=1, le=50),
    deduplicate: bool = Query(default=True, description='One lead per buyer (default true for urgent view)'),
):
    """Return only HIGH urgency leads — act on these first."""
    all_leads = _get_service().get_leads(limit=200, deduplicate=deduplicate)
    urgent = [l for l in all_leads if l.get('urgency') == 'HIGH']
    return urgent[:limit]


# ── Seller dashboard ──────────────────────────────────────────────────────────

@app.get('/sellers')
def list_sellers():
    """List all registered seller accounts."""
    return _get_seller_service().list_sellers()


@app.get('/sellers/{seller_id}')
def get_seller(seller_id: str):
    """Get a single seller's profile."""
    seller = _get_seller_service().get_seller(seller_id)
    if not seller:
        raise HTTPException(status_code=404, detail=f'Seller {seller_id} not found')
    return seller


@app.get('/sellers/{seller_id}/dashboard')
def seller_dashboard(seller_id: str):
    """
    Full seller dashboard: KPIs, credit balance, conversion rates,
    and performance breakdown by lead type.
    """
    dashboard = _get_seller_service().get_dashboard(seller_id)
    if not dashboard:
        raise HTTPException(status_code=404, detail=f'Seller {seller_id} not found')
    return dashboard


@app.get('/sellers/{seller_id}/leads')
def seller_leads(
    seller_id:     str,
    status_filter: Optional[str] = Query(default=None, description='Filter by assignment status: assigned/viewed/contacted/converted/rejected/archived'),
    page:          int           = Query(default=0, ge=0),
    page_size:     int           = Query(default=20, ge=1, le=100),
):
    """Return leads assigned to this seller, enriched with action steps."""
    return _get_seller_service().get_assigned_leads(
        seller_id=seller_id,
        status_filter=status_filter,
        page=page,
        page_size=page_size,
    )


@app.get('/sellers/{seller_id}/credits')
def seller_credits(seller_id: str):
    """Return current credit balance and billing cycle info for a seller."""
    from app.engines.monetization_engine import MonetizationEngine
    global _settings
    if _settings is None:
        _settings = load_settings('config.yaml')
    engine = MonetizationEngine(_get_neo(), _settings)
    return engine.get_balance(seller_id)


@app.post('/sellers/{seller_id}/credits/topup')
def top_up_credits(
    seller_id: str,
    amount:    int  = Body(..., embed=True, description='Number of credits to add'),
    reason:    str  = Body(default='purchase', embed=True),
):
    """Top up credits for a seller (admin or payment webhook)."""
    from app.engines.monetization_engine import MonetizationEngine
    global _settings
    if _settings is None:
        _settings = load_settings('config.yaml')
    engine = MonetizationEngine(_get_neo(), _settings)
    return engine.top_up_credits(seller_id, amount, reason)


@app.post('/sellers')
def upsert_seller(seller: dict = Body(...)):
    """
    Create or update a seller account.

    Required fields: seller_id, name
    Optional: tier (basic/professional/enterprise), hs_chapters, target_countries,
              max_leads_per_day, credits_available
    """
    from app.engines.distribution_engine import DistributionEngine
    global _settings
    if _settings is None:
        _settings = load_settings('config.yaml')
    engine = DistributionEngine(_get_neo(), _settings)
    ok = engine.upsert_seller(seller)
    return {'success': ok, 'seller_id': seller.get('seller_id')}


# ── Feedback & conversion ─────────────────────────────────────────────────────

@app.post('/assignments/{assignment_id}/feedback')
def record_feedback(
    assignment_id: str,
    action:        str  = Body(..., embed=True, description='viewed|contacted|converted|rejected|archived'),
    notes:         Optional[str] = Body(default=None, embed=True),
):
    """
    Record a seller action on a SellerLeadAssignment.
    Automatically triggers ConversionFact creation when action='converted'.
    """
    from app.engines.feedback_engine import FeedbackEngine
    global _settings
    if _settings is None:
        _settings = load_settings('config.yaml')
    engine = FeedbackEngine(_get_neo(), _settings)
    success = engine.record_action(assignment_id, action, notes)
    if not success:
        raise HTTPException(status_code=400, detail=f'Invalid action "{action}" or assignment not found')
    result = {'success': True, 'assignment_id': assignment_id, 'action': action}
    # Auto-trigger conversion tracking
    if action == 'converted':
        from app.engines.conversion_engine import ConversionEngine
        conv = ConversionEngine(_get_neo(), _settings)
        conv_result = conv.run()
        result['conversions_recorded'] = conv_result.get('recorded', 0)
    return result


@app.get('/conversions/stats')
def conversion_stats():
    """Return conversion rates and average days-to-convert by lead type."""
    from app.engines.conversion_engine import ConversionEngine
    global _settings
    if _settings is None:
        _settings = load_settings('config.yaml')
    engine = ConversionEngine(_get_neo(), _settings)
    return engine.get_stats()


# ── Admin: distribute switch leads to sellers ─────────────────────────────────

_FETCH_SWITCH_LEADS_Q = """
MATCH (sl:SwitchLead)
WHERE coalesce(sl.distribution_status, 'pending') = 'pending'
RETURN sl.lead_id          AS lead_id,
       sl.hs_code          AS hs_code,
       sl.lead_priority    AS priority,
       sl.buyer_country    AS buyer_country,
       sl.final_lead_score AS score
ORDER BY
  CASE sl.lead_priority
    WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END,
  sl.final_lead_score DESC
"""

_FETCH_SELLERS_Q = """
MATCH (s:Seller)
WHERE s.active = true
RETURN s.seller_id         AS seller_id,
       s.hs_chapters       AS hs_chapters,
       s.target_countries  AS target_countries,
       s.credits_available AS credits_available,
       s.max_leads_per_day AS max_leads_per_day,
       coalesce(s.leads_today, 0) AS leads_today
"""

_ASSIGN_SWITCH_LEADS_Q = """
UNWIND $rows AS row
MATCH (sl:SwitchLead {lead_id: row.lead_id})
MATCH (s:Seller      {seller_id: row.seller_id})
MERGE (a:SellerLeadAssignment {assignment_id: row.assignment_id})
SET a.seller_id    = row.seller_id,
    a.lead_id      = row.lead_id,
    a.lead_type    = 'switch_lead',
    a.lead_priority= row.priority,
    a.match_reason = row.match_reason,
    a.status       = 'assigned',
    a.credits_cost = row.credits_cost,
    a.assigned_at  = toString(datetime())
MERGE (s)-[:ASSIGNED]->(a)
MERGE (a)-[:FOR_LEAD]->(sl)
SET sl.distribution_status = 'distributed',
    sl.distributed_at      = toString(datetime())
RETURN count(a) AS c
"""

_CREDIT_COST = {'critical': 5, 'high': 3, 'medium': 1, 'low': 0}


@app.post('/admin/distribute')
def admin_distribute():
    """
    Assign pending SwitchLead nodes to matching Seller accounts based on HS code chapters.
    Idempotent — re-running skips already-distributed leads.
    Returns counts of leads processed and assignments created.
    """
    neo = _get_neo()

    # 1. Fetch undistributed switch leads
    lead_rows = neo.run(_FETCH_SWITCH_LEADS_Q) or []
    leads = [dict(r) for r in lead_rows]

    if not leads:
        return {'assigned': 0, 'message': 'No pending switch leads to distribute'}

    # 2. Fetch active sellers
    seller_rows = neo.run(_FETCH_SELLERS_Q) or []
    sellers = []
    for r in seller_rows:
        d = dict(r)
        d['hs_chapters']      = list(d.get('hs_chapters') or [])
        d['target_countries'] = list(d.get('target_countries') or [])
        d['leads_today']      = int(d.get('leads_today') or 0)
        d['credits_available']= float(d.get('credits_available') or 0)
        d['max_leads_per_day']= int(d.get('max_leads_per_day') or 50)
        sellers.append(d)

    if not sellers:
        return {'assigned': 0, 'message': 'No active sellers registered'}

    # 3. Match leads → sellers  (cap: max 3 sellers per lead)
    MAX_SELLERS_PER_LEAD = 3
    capacity = {s['seller_id']: s['max_leads_per_day'] - s['leads_today'] for s in sellers}
    assignments = []
    unmatched = []

    for lead in leads:
        lead_id     = lead['lead_id']
        if not lead_id:                  # skip if lead_id is blank
            continue
        hs_raw      = str(lead.get('hs_code') or '')
        # Strip list-bracket noise e.g. "[6802991000]" → "68"
        import re as _re
        hs_chapter  = _re.sub(r'[\[\]\s]', '', hs_raw)[:2]
        buyer_cty   = str(lead.get('buyer_country') or '').upper()
        priority    = lead.get('priority', 'medium') or 'medium'
        cost        = _CREDIT_COST.get(priority, 1)

        sellers_for_lead = 0
        lead_matched = False
        for seller in sellers:
            if sellers_for_lead >= MAX_SELLERS_PER_LEAD:
                break                    # hard cap reached for this lead

            sid = seller.get('seller_id', '')
            if not sid:                  # skip sellers with blank seller_id
                continue
            if capacity.get(sid, 0) <= 0:
                continue
            if seller['credits_available'] < cost:
                continue

            chapter_match = (not seller['hs_chapters'] or hs_chapter in seller['hs_chapters'])
            country_match = (not seller['target_countries']
                             or buyer_cty in seller['target_countries']
                             or '*' in seller['target_countries'])

            if not chapter_match or not country_match:
                continue

            reason = []
            if chapter_match and seller['hs_chapters']:
                reason.append(f'hs_{hs_chapter}')
            if country_match and seller['target_countries']:
                reason.append(f'geo_{buyer_cty}')
            if not reason:
                reason.append('open_match')

            assignments.append({
                'seller_id':     sid,
                'lead_id':       lead_id,
                'assignment_id': f'sla:{sid}:{lead_id}',
                'priority':      priority,
                'match_reason':  ','.join(reason),
                'credits_cost':  cost,
            })
            capacity[sid] -= 1
            seller['credits_available'] -= cost
            sellers_for_lead += 1
            lead_matched = True

        if not lead_matched:
            unmatched.append(lead_id)

    # 4. Write assignments
    total = 0
    if assignments:
        for i in range(0, len(assignments), 500):
            chunk = assignments[i:i + 500]
            rows = neo.run(_ASSIGN_SWITCH_LEADS_Q, {'rows': chunk}) or [{'c': 0}]
            total += int(rows[0].get('c', 0))

    return {
        'leads_pending':    len(leads),
        'sellers_active':   len(sellers),
        'assigned':         total,
        'unmatched_leads':  len(unmatched),
        'unmatched_ids':    unmatched,
    }


# ══════════════════════════════════════════════════════════════════════════════
# TRADE INTELLIGENCE  — KG-exclusive, no equivalent in admin.goglo.com UI
# ══════════════════════════════════════════════════════════════════════════════

@app.get('/trade-intelligence/stats',
         summary='Trade graph summary — volume, health distribution, data source')
def trade_stats():
    """
    Aggregate stats for all TradeRelationship nodes.
    Returns total $volume monitored, avg health score, and a breakdown of
    CHURNED / DORMANT / STRESSED / IRREGULAR / HEALTHY relationships.
    Data source: Trademo verified customs import/export records.
    """
    return _get_intelligence_service().get_trade_stats()


@app.get('/trade-intelligence/relationships',
         summary='List TradeRelationship nodes with buyer $volume and health status')
def list_trade_relationships(
    limit:              int   = Query(default=50, ge=1, le=500),
    min_monthly_volume: float = Query(default=0.0, description='Filter by minimum monthly USD volume'),
    health_status:      str   = Query(default=None, description='CHURNED | DORMANT | STRESSED | IRREGULAR | HEALTHY'),
    buyer_country:      str   = Query(default=None, description='Filter by buyer country code e.g. US, AE, IN'),
):
    """
    Returns all TradeRelationship nodes ordered by monthly volume descending.
    Each relationship shows both rule-based stress assessment (rule_health_status)
    and ML-based health score (ml_health_score) side by side.
    """
    return _get_intelligence_service().get_trade_relationships(
        limit=limit,
        min_monthly_volume=min_monthly_volume,
        health_status=health_status,
        buyer_country=buyer_country,
    )


@app.get('/trade-intelligence/relationships/{relationship_id}',
         summary='Single TradeRelationship detail')
def get_trade_relationship(relationship_id: str):
    """
    Full detail for one TradeRelationship node: both rule-based and ML assessments,
    full shipment history, and linked SupplierSwitchOpportunity IDs.
    """
    rel = _get_intelligence_service().get_trade_relationship(relationship_id)
    if not rel:
        raise HTTPException(status_code=404, detail=f'TradeRelationship {relationship_id} not found')
    return rel


# ══════════════════════════════════════════════════════════════════════════════
# BUYER PROFILES  — KG-exclusive, no equivalent in admin.goglo.com UI
# ══════════════════════════════════════════════════════════════════════════════

@app.get('/buyer-profiles/stats',
         summary='BuyerProfile aggregate stats — intent bands, engagement totals')
def buyer_profile_stats():
    """
    Platform-wide buyer behaviour summary.
    Returns total profiles (17,747 in production), intent band breakdown
    (high/medium/low), and engagement counts (enquiries, chats, strong clickers).
    Data source: GoGlo CRM2 tracking tables.
    """
    return _get_intelligence_service().get_buyer_profile_stats()


@app.get('/buyer-profiles',
         summary='List BuyerProfile nodes ordered by behavioral score')
def list_buyer_profiles(
    limit:         int  = Query(default=50, ge=1, le=500),
    min_score:     int  = Query(default=0, ge=0, le=100, description='Minimum behavioral score'),
    has_enquiries: bool = Query(default=None, description='True = only buyers who submitted enquiries'),
    intent_band:   str  = Query(default=None, description='high (score≥70) | medium (50-69) | low (<50)'),
):
    """
    Returns BuyerProfile nodes ranked by behavioral_score desc.
    Each profile shows: click activity, session count, scroll depth,
    enquiry count, intent score, and the top CTAs the buyer clicked.
    """
    return _get_intelligence_service().get_buyer_profiles(
        limit=limit,
        min_score=min_score,
        has_enquiries=has_enquiries,
        intent_band=intent_band,
    )


@app.get('/buyer-profiles/{user_id}',
         summary='Single BuyerProfile with linked lead types')
def get_buyer_profile(user_id: str):
    """
    Full BuyerProfile for one user — all activity counts, top CTAs clicked,
    scroll depth, and the Lead node types this buyer generated.
    """
    profile = _get_intelligence_service().get_buyer_profile(user_id)
    if not profile:
        raise HTTPException(status_code=404, detail=f'BuyerProfile for user {user_id} not found')
    return profile


# ══════════════════════════════════════════════════════════════════════════════
# SWITCH OPPORTUNITIES — RULE vs ML COMPARISON
# ══════════════════════════════════════════════════════════════════════════════

@app.get('/switch-opportunities/detection-summary',
         summary='Rule-based vs ML detection comparison — counts, avg scores, overlap')
def detection_comparison():
    """
    Side-by-side comparison of the two supplier stress detection engines:
    - Rule-based (Stage 3A): 7 explicit thresholds on shipment gaps and volume drops
    - ML-based (Stage 3B): IsolationForest anomaly detection

    Returns total opportunities found by each engine, average health scores,
    health status breakdown, and notes on when each approach is strongest.
    Both engines write SupplierSwitchOpportunity nodes tagged with detection_method.
    """
    return _get_intelligence_service().get_detection_comparison()


@app.get('/switch-opportunities',
         summary='List SupplierSwitchOpportunity nodes (rule + ML)')
def list_switch_opportunities(
    limit:            int   = Query(default=50, ge=1, le=500),
    detection_method: str   = Query(default=None, description='rule_based | ml_based | None for all'),
    min_health_score: float = Query(default=None, description='Filter by minimum ML health score'),
):
    """
    Returns SupplierSwitchOpportunity nodes ordered by health score ascending
    (most at-risk first). Each record shows both the rule-based assessment
    (rule_health_status, rule_stress_reason) and the ML score side by side.
    """
    return _get_intelligence_service().get_switch_opportunities(
        limit=limit,
        detection_method=detection_method,
        min_health_score=min_health_score,
    )


# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE STATUS  — KG-exclusive
# ══════════════════════════════════════════════════════════════════════════════

@app.get('/pipeline/status',
         summary='KG pipeline health — node counts, last run, lead type breakdown')
def pipeline_status():
    """
    Live snapshot of the knowledge graph state.
    Returns counts for every node type (Lead, BuyerProfile, TradeRelationship,
    SwitchLead, SupplierSwitchOpportunity, Seller, SellerLeadAssignment,
    ConversionFact), the last lead creation timestamp, and a full breakdown
    of lead types and detection methods. Also lists all pipeline stages and
    data sources feeding the graph.
    """
    return _get_intelligence_service().get_pipeline_status()


# ══════════════════════════════════════════════════════════════════════════════
# SCORE UPDATES — manual overrides with audit trail
# Supports: Lead, SwitchLead, BuyerProfile, TradeRelationship
# All overrides set score_override=true so the pipeline skips them on next run
# ══════════════════════════════════════════════════════════════════════════════

class ScoreUpdate(BaseModel):
    score:    float = Field(..., ge=0, le=100, description='New score value (0–100)')
    reason:   str   = Field(default='manual_override', description='Why this score was changed')
    changed_by: str = Field(default='admin', description='Who made the change')


class SwitchLeadScoreUpdate(BaseModel):
    final_lead_score:   Optional[float] = Field(default=None, ge=0,  le=200, description='Override final_lead_score (0–200)')
    switch_probability: Optional[float] = Field(default=None, ge=0.0, le=1.0, description='Override switch_probability (0.0–1.0)')
    reason:    str = Field(default='manual_override')
    changed_by: str = Field(default='admin')


class TradeHealthUpdate(BaseModel):
    health_score: float = Field(..., ge=0, le=100, description='Override ML health score (0=worst, 100=healthy)')
    reason:    str = Field(default='manual_override')
    changed_by: str = Field(default='admin')


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


# ── Lead nodes ────────────────────────────────────────────────────────────────

@app.patch('/leads/{lead_id}/score',
           summary='Override score_final on a Lead node')
def update_lead_score(lead_id: str, body: ScoreUpdate):
    """
    Manually set score_final on a Lead node.

    Sets score_override=true so the pipeline will NOT overwrite this score
    on the next classify-platform-leads run. Also saves the original score
    in score_final_original and logs who changed it and when.

    Connected to: /admin/goglo/leads/master (Lead Master),
                  /admin/goglo/leads/scoring-config (Scoring Config)
    """
    neo = _get_neo()
    cypher = """
        MATCH (l:Lead {lead_id: $lid})
        SET l.score_final_original = CASE
              WHEN l.score_override IS NULL OR l.score_override = false
              THEN l.score_final
              ELSE l.score_final_original
            END,
            l.score_final       = $score,
            l.score_override    = true,
            l.score_reason      = $reason,
            l.score_changed_by  = $by,
            l.score_changed_at  = $ts
        RETURN l.lead_id AS id, l.score_final AS new_score,
               l.score_final_original AS original_score, l.lead_type AS lead_type
    """
    rows = neo.run(cypher, {
        'lid': lead_id, 'score': body.score,
        'reason': body.reason, 'by': body.changed_by, 'ts': _now_iso(),
    })
    if not rows:
        raise HTTPException(status_code=404, detail=f'Lead {lead_id} not found')
    r = rows[0]
    return {
        'updated': True,
        'lead_id': r.get('id'),
        'lead_type': r.get('lead_type'),
        'original_score': r.get('original_score'),
        'new_score': r.get('new_score'),
        'score_override': True,
        'reason': body.reason,
        'changed_by': body.changed_by,
        'changed_at': _now_iso(),
        'note': 'Pipeline will skip this lead on next classify-platform-leads run.',
    }


@app.delete('/leads/{lead_id}/score/override',
            summary='Remove score override — let pipeline recalculate on next run')
def remove_lead_score_override(lead_id: str):
    """
    Clears score_override and restores score_final to score_final_original.
    After this, the next pipeline run will recalculate the score normally.
    """
    neo = _get_neo()
    cypher = """
        MATCH (l:Lead {lead_id: $lid})
        SET l.score_final    = coalesce(l.score_final_original, l.score_final),
            l.score_override = false,
            l.score_reason   = null,
            l.score_changed_by = null,
            l.score_changed_at = null
        RETURN l.lead_id AS id, l.score_final AS restored_score
    """
    rows = neo.run(cypher, {'lid': lead_id})
    if not rows:
        raise HTTPException(status_code=404, detail=f'Lead {lead_id} not found')
    r = rows[0]
    return {
        'updated': True,
        'lead_id': r.get('id'),
        'restored_score': r.get('restored_score'),
        'score_override': False,
        'note': 'Pipeline will recalculate score on next run.',
    }


# ── SwitchLead nodes ──────────────────────────────────────────────────────────

@app.patch('/switch-leads/{lead_id}/score',
           summary='Override final_lead_score and/or switch_probability on a SwitchLead')
def update_switch_lead_score(lead_id: str, body: SwitchLeadScoreUpdate):
    """
    Manually adjust the scoring on a SwitchLead node.
    You can override final_lead_score, switch_probability, or both.

    Sets score_override=true so the pipeline will NOT overwrite on next
    score-switch-probability run.

    Connected to: /admin/goglo/leads/master (Lead Master — switch_lead type rows)
    """
    if body.final_lead_score is None and body.switch_probability is None:
        raise HTTPException(status_code=422, detail='Provide at least one of: final_lead_score, switch_probability')

    neo = _get_neo()
    sets = [
        "sl.score_override   = true",
        "sl.score_reason     = $reason",
        "sl.score_changed_by = $by",
        "sl.score_changed_at = $ts",
    ]
    params: dict = {'lid': lead_id, 'reason': body.reason, 'by': body.changed_by, 'ts': _now_iso()}

    if body.final_lead_score is not None:
        sets.append("""
            sl.score_final_original = CASE
              WHEN sl.score_override IS NULL OR sl.score_override = false
              THEN sl.final_lead_score ELSE sl.score_final_original END,
            sl.final_lead_score = $fls
        """)
        params['fls'] = body.final_lead_score

    if body.switch_probability is not None:
        sets.append("sl.switch_probability = $sp")
        params['sp'] = body.switch_probability

    cypher = f"""
        MATCH (sl:SwitchLead {{lead_id: $lid}})
        SET {', '.join(sets)}
        RETURN sl.lead_id AS id,
               sl.final_lead_score   AS new_final_score,
               sl.switch_probability AS new_switch_prob,
               sl.score_final_original AS original_score
    """
    rows = neo.run(cypher, params)
    if not rows:
        raise HTTPException(status_code=404, detail=f'SwitchLead {lead_id} not found')
    r = rows[0]
    return {
        'updated': True,
        'lead_id': r.get('id'),
        'original_score': r.get('original_score'),
        'new_final_lead_score': r.get('new_final_score'),
        'new_switch_probability': r.get('new_switch_prob'),
        'score_override': True,
        'reason': body.reason,
        'changed_by': body.changed_by,
        'changed_at': _now_iso(),
    }


# ── BuyerProfile nodes ────────────────────────────────────────────────────────

@app.patch('/buyer-profiles/{user_id}/score',
           summary='Override behavioral_score on a BuyerProfile node')
def update_buyer_profile_score(user_id: str, body: ScoreUpdate):
    """
    Manually set behavioral_score on a BuyerProfile node.

    Useful when automated scoring has missed context — e.g. an offline
    meeting or a direct call that signals strong intent.

    Sets score_override=true so build-buyer-profiles won't overwrite on next run.

    Connected to: /admin/crm/session_index (Session Engagement)
    """
    neo = _get_neo()
    cypher = """
        MATCH (bp:BuyerProfile {user_id: $uid})
        SET bp.behavioral_score_original = CASE
              WHEN bp.score_override IS NULL OR bp.score_override = false
              THEN bp.behavioral_score
              ELSE bp.behavioral_score_original
            END,
            bp.behavioral_score  = $score,
            bp.score_override    = true,
            bp.score_reason      = $reason,
            bp.score_changed_by  = $by,
            bp.score_changed_at  = $ts
        RETURN bp.user_id AS id, bp.behavioral_score AS new_score,
               bp.behavioral_score_original AS original_score
    """
    rows = neo.run(cypher, {
        'uid': user_id, 'score': body.score,
        'reason': body.reason, 'by': body.changed_by, 'ts': _now_iso(),
    })
    if not rows:
        raise HTTPException(status_code=404, detail=f'BuyerProfile for user {user_id} not found')
    r = rows[0]
    return {
        'updated': True,
        'user_id': r.get('id'),
        'original_score': r.get('original_score'),
        'new_score': r.get('new_score'),
        'score_override': True,
        'reason': body.reason,
        'changed_by': body.changed_by,
        'changed_at': _now_iso(),
        'note': 'Pipeline will skip this profile on next build-buyer-profiles run.',
    }


# ── TradeRelationship nodes ───────────────────────────────────────────────────

@app.patch('/trade-intelligence/relationships/{relationship_id}/score',
           summary='Override health_score on a TradeRelationship node')
def update_trade_health_score(relationship_id: str, body: TradeHealthUpdate):
    """
    Manually set health_score on a TradeRelationship node.

    Useful when you have context the ML model doesn't — e.g. you know the
    buyer has already informed their supplier they are switching.

    Sets score_override=true so detect-switch-leads won't overwrite on next run.

    Connected to: /trade-intelligence/relationships (Trade Intelligence panel)
    """
    neo = _get_neo()
    cypher = """
        MATCH (tr:TradeRelationship {relationship_id: $rid})
        SET tr.health_score_original = CASE
              WHEN tr.score_override IS NULL OR tr.score_override = false
              THEN tr.health_score
              ELSE tr.health_score_original
            END,
            tr.health_score      = $score,
            tr.score_override    = true,
            tr.score_reason      = $reason,
            tr.score_changed_by  = $by,
            tr.score_changed_at  = $ts
        RETURN tr.relationship_id AS id,
               tr.health_score AS new_score,
               tr.health_score_original AS original_score,
               tr.buyer_name AS buyer_name
    """
    rows = neo.run(cypher, {
        'rid': relationship_id, 'score': body.health_score,
        'reason': body.reason, 'by': body.changed_by, 'ts': _now_iso(),
    })
    if not rows:
        raise HTTPException(status_code=404, detail=f'TradeRelationship {relationship_id} not found')
    r = rows[0]
    return {
        'updated': True,
        'relationship_id': r.get('id'),
        'buyer_name': r.get('buyer_name'),
        'original_health_score': r.get('original_score'),
        'new_health_score': r.get('new_score'),
        'score_override': True,
        'reason': body.reason,
        'changed_by': body.changed_by,
        'changed_at': _now_iso(),
        'note': 'Pipeline will skip this relationship on next detect-switch-leads run.',
    }


# ── Bulk score query — see all overridden nodes ───────────────────────────────

@app.get('/scores/overrides',
         summary='List all manually overridden scores across all node types')
def list_score_overrides():
    """
    Returns every node where score_override=true — across Lead, SwitchLead,
    BuyerProfile, and TradeRelationship. Useful for audit: see what was changed
    manually, by whom, and when.
    """
    neo = _get_neo()
    results = []

    queries = [
        ('Lead', """
            MATCH (l:Lead) WHERE l.score_override = true
            RETURN 'Lead' AS node_type, l.lead_id AS node_id,
                   l.lead_type AS sub_type,
                   l.score_final_original AS original_score,
                   l.score_final AS current_score,
                   l.score_reason AS reason,
                   l.score_changed_by AS changed_by,
                   l.score_changed_at AS changed_at
            ORDER BY l.score_changed_at DESC
        """),
        ('SwitchLead', """
            MATCH (sl:SwitchLead) WHERE sl.score_override = true
            RETURN 'SwitchLead' AS node_type, sl.lead_id AS node_id,
                   sl.lead_priority AS sub_type,
                   sl.score_final_original AS original_score,
                   sl.final_lead_score AS current_score,
                   sl.score_reason AS reason,
                   sl.score_changed_by AS changed_by,
                   sl.score_changed_at AS changed_at
            ORDER BY sl.score_changed_at DESC
        """),
        ('BuyerProfile', """
            MATCH (bp:BuyerProfile) WHERE bp.score_override = true
            RETURN 'BuyerProfile' AS node_type, bp.user_id AS node_id,
                   'behavioral_score' AS sub_type,
                   bp.behavioral_score_original AS original_score,
                   bp.behavioral_score AS current_score,
                   bp.score_reason AS reason,
                   bp.score_changed_by AS changed_by,
                   bp.score_changed_at AS changed_at
            ORDER BY bp.score_changed_at DESC
        """),
        ('TradeRelationship', """
            MATCH (tr:TradeRelationship) WHERE tr.score_override = true
            RETURN 'TradeRelationship' AS node_type, tr.relationship_id AS node_id,
                   tr.buyer_name AS sub_type,
                   tr.health_score_original AS original_score,
                   tr.health_score AS current_score,
                   tr.score_reason AS reason,
                   tr.score_changed_by AS changed_by,
                   tr.score_changed_at AS changed_at
            ORDER BY tr.score_changed_at DESC
        """),
    ]

    for label, cypher in queries:
        rows = neo.run(cypher, {}) or []
        for r in rows:
            results.append({
                'node_type':      r.get('node_type'),
                'node_id':        r.get('node_id'),
                'sub_type':       r.get('sub_type'),
                'original_score': r.get('original_score'),
                'current_score':  r.get('current_score'),
                'reason':         r.get('reason'),
                'changed_by':     r.get('changed_by'),
                'changed_at':     r.get('changed_at'),
            })

    results.sort(key=lambda x: x.get('changed_at') or '', reverse=True)
    return {
        'total_overrides': len(results),
        'overrides': results,
    }
