"""
SellerService: Seller dashboard, assigned lead management, and credit views.

Provides:
  - get_dashboard(seller_id)  → KPIs + recent activity
  - get_assigned_leads(...)   → paginated assigned leads for a seller
  - get_credit_balance(...)   → current credit position
  - list_sellers()            → all registered sellers
"""

from typing import Optional
from app.core.logger import warn

_DASHBOARD_QUERY = """
MATCH (s:Seller {seller_id: $seller_id})
OPTIONAL MATCH (s)-[:ASSIGNED]->(a:SellerLeadAssignment)
OPTIONAL MATCH (sa:SellerAccount {account_id: 'acct:' + $seller_id})
WITH s, sa,
     count(a)                                                     AS total_assigned,
     sum(CASE WHEN a.status = 'viewed'    THEN 1 ELSE 0 END)      AS viewed,
     sum(CASE WHEN a.status = 'contacted' THEN 1 ELSE 0 END)      AS contacted,
     sum(CASE WHEN a.status = 'converted' THEN 1 ELSE 0 END)      AS converted,
     sum(CASE WHEN a.status = 'rejected'  THEN 1 ELSE 0 END)      AS rejected,
     sum(CASE WHEN a.status = 'assigned'
              AND a.assigned_at >= toString(datetime() - duration('P7D'))
              THEN 1 ELSE 0 END)                                   AS new_last_7d
RETURN s.seller_id           AS seller_id,
       s.name                AS name,
       s.tier                AS tier,
       s.hs_chapters         AS hs_chapters,
       s.target_countries    AS target_countries,
       s.active              AS active,
       s.leads_today         AS leads_today,
       s.max_leads_per_day   AS max_leads_per_day,
       total_assigned,
       viewed,
       contacted,
       converted,
       rejected,
       new_last_7d,
       coalesce(sa.credits_available, 0)   AS credits_available,
       coalesce(sa.credits_used_month, 0)  AS credits_used_month,
       coalesce(sa.credits_total_month, 0) AS credits_total_month
"""

_ASSIGNED_LEADS_QUERY = """
MATCH (s:Seller {seller_id: $seller_id})-[:ASSIGNED]->(a:SellerLeadAssignment)
MATCH (a)-[:FOR_LEAD]->(l)
WHERE (l:Lead OR l:SwitchLead)
  AND (coalesce($status_filter, 'all') = 'all' OR a.status = $status_filter)
OPTIONAL MATCH (l)-[:HAS_HYPOTHESIS]->(oh:OpportunityHypothesis)
OPTIONAL MATCH (oh)-[:TRIGGERED_BY]->(e:Evidence)-[:SUPPORTS]->(ih:IdentityHypothesis)
RETURN a.assignment_id                           AS assignment_id,
       a.status                                  AS assignment_status,
       a.assigned_at                             AS assigned_at,
       a.actioned_at                             AS actioned_at,
       a.credits_cost                            AS credits_cost,
       a.match_reason                            AS match_reason,
       coalesce(l.lead_uid, l.lead_id)           AS lead_id,
       coalesce(l.lead_type, 'switch_lead')      AS lead_type,
       coalesce(l.lead_grain, '')                AS lead_grain,
       coalesce(l.priority, l.lead_priority)     AS priority,
       coalesce(l.final_score, l.final_lead_score, 0.0) AS score,
       coalesce(l.evidence_strength, 0)          AS evidence_strength,
       coalesce(l.lead_stage, l.status, '')      AS stage,
       coalesce(l.visibility, 'seller_visible')  AS visibility,
       coalesce(l.playbook_tags, '')             AS playbook_tags,
       coalesce(l.opportunity_specificity, '')   AS specificity,
       coalesce(l.buyer_account_key, l.buyer_org_id, '') AS buyer_key,
       coalesce(l.account_state, '')             AS account_state,
       l.hs_code                                 AS hs_code,
       l.buyer_name                              AS buyer_name,
       l.buyer_country                           AS buyer_country,
       l.recommended_action                      AS recommended_action,
       l.stress_reason                           AS stress_reason,
       l.switch_probability                      AS switch_probability,
       l.candidate_supplier_name                 AS recommended_supplier,
       ih.candidate_entity_id                    AS entity_id,
       ih.confidence_score                       AS identity_confidence
ORDER BY
  CASE coalesce(l.priority, l.lead_priority)
    WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END,
  coalesce(l.final_score, l.final_lead_score, 0) DESC
SKIP $skip
LIMIT $limit
"""

_SELLER_LIST_QUERY = """
MATCH (s:Seller)
OPTIONAL MATCH (sa:SellerAccount {account_id: 'acct:' + s.seller_id})
RETURN s.seller_id           AS seller_id,
       s.name                AS name,
       s.tier                AS tier,
       s.active              AS active,
       s.max_leads_per_day   AS max_leads_per_day,
       s.leads_today         AS leads_today,
       coalesce(sa.credits_available, 0) AS credits_available
ORDER BY
  CASE s.tier
    WHEN 'enterprise'   THEN 1
    WHEN 'professional' THEN 2
    ELSE 3 END,
  s.name
"""

_PERFORMANCE_QUERY = """
MATCH (s:Seller {seller_id: $seller_id})-[:ASSIGNED]->(a:SellerLeadAssignment)
MATCH (a)-[:FOR_LEAD]->(l:Lead)
WITH l.lead_type AS lead_type,
     count(a)                                                AS total,
     sum(CASE WHEN a.status='converted' THEN 1 ELSE 0 END)  AS converted,
     sum(CASE WHEN a.status='rejected'  THEN 1 ELSE 0 END)  AS rejected,
     avg(l.final_score)                                      AS avg_score
RETURN lead_type, total, converted, rejected, avg_score,
       CASE WHEN total > 0
            THEN round(100.0 * converted / total, 1) ELSE 0 END AS conv_rate_pct
ORDER BY total DESC
"""


class SellerService:

    def __init__(self, neo):
        self.neo = neo

    # ── dashboard ─────────────────────────────────────────────────────────────

    def get_dashboard(self, seller_id: str) -> Optional[dict]:
        rows = self.neo.run(_DASHBOARD_QUERY, {'seller_id': seller_id}) or []
        if not rows:
            return None
        r = dict(rows[0])

        # Conversion rate
        total = r.get('total_assigned', 0) or 1
        r['conversion_rate_pct'] = round(100.0 * (r.get('converted', 0) / total), 1)
        r['contact_rate_pct']    = round(100.0 * (r.get('contacted', 0) / total), 1)

        # Credits
        monthly = r.get('credits_total_month', 0)
        r['credits_unlimited'] = monthly == -1
        if monthly and monthly > 0:
            r['credits_used_pct'] = round(100.0 * r.get('credits_used_month', 0) / monthly, 1)
        else:
            r['credits_used_pct'] = 0

        # Performance by lead type
        perf_rows = self.neo.run(_PERFORMANCE_QUERY, {'seller_id': seller_id}) or []
        r['performance_by_type'] = [dict(p) for p in perf_rows]

        return r

    # ── assigned leads ────────────────────────────────────────────────────────

    def get_assigned_leads(
        self,
        seller_id:     str,
        status_filter: Optional[str] = None,
        page:          int           = 0,
        page_size:     int           = 20,
    ) -> list[dict]:
        rows = self.neo.run(_ASSIGNED_LEADS_QUERY, {
            'seller_id':     seller_id,
            'status_filter': status_filter,
            'skip':          page * page_size,
            'limit':         page_size,
        }) or []
        return [_enrich_lead(dict(r)) for r in rows]

    # ── seller list ───────────────────────────────────────────────────────────

    def list_sellers(self) -> list[dict]:
        rows = self.neo.run(_SELLER_LIST_QUERY) or []
        return [dict(r) for r in rows]

    def get_seller(self, seller_id: str) -> Optional[dict]:
        rows = self.neo.run("""
            MATCH (s:Seller {seller_id: $sid})
            RETURN s.seller_id AS seller_id, s.name AS name, s.tier AS tier,
                   s.active AS active, s.hs_chapters AS hs_chapters,
                   s.target_countries AS target_countries,
                   s.max_leads_per_day AS max_leads_per_day
        """, {'sid': seller_id}) or []
        return dict(rows[0]) if rows else None


# ── helpers ───────────────────────────────────────────────────────────────────

def _enrich_lead(r: dict) -> dict:
    """Add human-readable fields to an assigned lead dict."""
    from app.api.lead_service import HS_CHAPTERS
    import re as _re
    hs_raw = str(r.get('hs_code') or '')
    hs = _re.sub(r'[\[\]\s]', '', hs_raw)[:2]
    r['hs_description'] = HS_CHAPTERS.get(hs, 'Unknown')

    # For switch leads, use the pre-built recommended_action if present
    if r.get('recommended_action'):
        r['action_steps'] = [r['recommended_action']]
    else:
        tags = r.get('playbook_tags', '') or ''
        steps = []
        if 'respond_immediately' in tags:
            steps.append('Respond to RFQ within 2 hours')
        if 'proactive_outreach' in tags:
            steps.append('Send personalised outreach referencing trade data')
        if 'multi_stakeholder' in tags:
            steps.append('Map buying committee before outreach')
        if 'close_now' in tags:
            steps.append('Escalate to account executive for close')
        if 'consolidation' in tags:
            steps.append('Prepare consolidation proposal for executive sponsor')
        if not steps:
            steps.append('Review profile and add to nurture sequence')
        r['action_steps'] = steps

    r['urgency'] = _urgency(r)
    return r


def _urgency(r: dict) -> str:
    priority = r.get('priority', '')
    score    = float(r.get('score') or 0)
    if priority == 'critical' or score >= 85:
        return 'HIGH'
    if priority == 'high' or score >= 70:
        return 'MEDIUM'
    return 'LOW'
