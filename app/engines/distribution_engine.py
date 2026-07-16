"""
DistributionEngine — Lead Distribution Centre (LDC).

Assigns seller-visible Lead nodes to registered Seller accounts based on:
  - HS code chapter matching (seller's declared product categories)
  - Geography matching (seller's target countries vs buyer's country)
  - Seller capacity (max_leads_per_day subscription limit)
  - Subscription tier priority (enterprise > professional > basic)
  - Lead exclusivity (one seller per lead when exclusivity=true)

Creates ``SellerLeadAssignment`` nodes and optionally deducts credits
(requires MonetizationEngine).

Config (config.yaml):
  distribution:
    enabled: true
    max_leads_per_seller_per_day: 50
    exclusivity: false          # if true, each lead assigned to at most 1 seller
    tier_priority: true         # enterprise/professional sellers get first pick
"""

from app.core.logger import info, ok, warn, banner


_ASSIGN_QUERY = """
UNWIND $rows AS row
MATCH (l:Lead {lead_uid: row.lead_id})
MATCH (s:Seller {seller_id: row.seller_id})
MERGE (a:SellerLeadAssignment {
    assignment_id: 'sla:' + row.seller_id + ':' + row.lead_id
})
SET a.seller_id      = row.seller_id,
    a.lead_id        = row.lead_id,
    a.lead_type      = l.lead_type,
    a.lead_priority  = l.priority,
    a.match_reason   = row.match_reason,
    a.status         = 'assigned',
    a.credits_cost   = row.credits_cost,
    a.rank_score     = row.rank_score,
    a.rank_reason    = row.rank_reason,
    a.assigned_at    = toString(datetime()),
    a.viewed_at      = null,
    a.contacted_at   = null,
    a.converted_at   = null
MERGE (s)-[:ASSIGNED]->(a)
MERGE (a)-[:FOR_LEAD]->(l)
SET l.distribution_status = 'distributed',
    l.distributed_at = toString(datetime())
RETURN count(a) AS c
"""

_FETCH_LEADS_QUERY = """
MATCH (l:Lead)
WHERE l.seller_visible = true
  AND coalesce(l.distribution_status, 'pending') = 'pending'
  AND l.lead_type <> 'blocked'
RETURN l.lead_uid       AS lead_id,
       l.lead_type      AS lead_type,
       l.priority       AS priority,
       l.hs_code        AS hs_code,
       l.buyer_country  AS buyer_country,
       l.final_score    AS score
ORDER BY
  CASE l.priority
    WHEN 'critical' THEN 1
    WHEN 'high'     THEN 2
    WHEN 'medium'   THEN 3
    ELSE 4 END,
  l.final_score DESC
LIMIT $batch
"""

_FETCH_SELLERS_QUERY = """
MATCH (s:Seller)
WHERE s.active = true
RETURN s.seller_id          AS seller_id,
       s.name               AS name,
       s.tier               AS tier,
       s.hs_chapters        AS hs_chapters,
       s.target_countries   AS target_countries,
       s.max_leads_per_day  AS max_leads_per_day,
       s.credits_available  AS credits_available,
       s.leads_today        AS leads_today
ORDER BY
  CASE s.tier
    WHEN 'enterprise'    THEN 1
    WHEN 'professional'  THEN 2
    ELSE 3 END,
  s.credits_available DESC
"""

_RESET_DAILY_COUNTS = """
MATCH (s:Seller)
WHERE coalesce(s.leads_today_date, '') <> toString(date())
SET s.leads_today = 0,
    s.leads_today_date = toString(date())
RETURN count(s) AS c
"""

# Credit costs by priority (also used by MonetizationEngine)
CREDIT_COST = {
    'critical': 5,
    'high':     3,
    'medium':   1,
    'low':      0,
}


class DistributionEngine:

    def __init__(self, neo, settings):
        self.neo  = neo
        cfg       = getattr(settings, 'distribution', None)
        self.enabled     = bool(cfg and getattr(cfg, 'enabled', True))
        self.max_daily   = int(getattr(cfg, 'max_leads_per_seller_per_day', 50) if cfg else 50)
        self.exclusivity = bool(getattr(cfg, 'exclusivity', False) if cfg else False)
        self.tier_prio   = bool(getattr(cfg, 'tier_priority', True) if cfg else True)
        self.batch       = int(settings.runtime.batch_size)

    # ── public entry ──────────────────────────────────────────────────────────

    def run(self) -> dict:
        if not self.enabled:
            info('DistributionEngine: disabled in config')
            return {'assigned': 0}

        banner('DistributionEngine: Lead Distribution Centre')
        self._reset_daily_counts()

        leads   = self._fetch_leads()
        sellers = self._fetch_sellers()

        if not leads:
            ok('DistributionEngine: no pending leads to distribute')
            return {'assigned': 0}
        if not sellers:
            warn('DistributionEngine: no active sellers found — create Seller nodes first')
            return {'assigned': 0}

        assignments = self._match(leads, sellers)
        total       = self._write_assignments(assignments)

        ok(f'DistributionEngine: assigned {total} leads to sellers')
        return {'assigned': total, 'leads_processed': len(leads), 'sellers': len(sellers)}

    # ── internal ──────────────────────────────────────────────────────────────

    def _reset_daily_counts(self):
        self.neo.run(_RESET_DAILY_COUNTS)

    def _fetch_leads(self) -> list[dict]:
        rows = self.neo.run(_FETCH_LEADS_QUERY, {'batch': self.batch}) or []
        return [dict(r) for r in rows]

    def _fetch_sellers(self) -> list[dict]:
        rows = self.neo.run(_FETCH_SELLERS_QUERY) or []
        sellers = []
        for r in rows:
            d = dict(r)
            # Normalise list fields
            d['hs_chapters']      = list(d.get('hs_chapters') or [])
            d['target_countries'] = list(d.get('target_countries') or [])
            d['leads_today']      = int(d.get('leads_today') or 0)
            d['credits_available']= float(d.get('credits_available') or 0)
            d['max_leads_per_day']= int(d.get('max_leads_per_day') or self.max_daily)
            sellers.append(d)
        return sellers

    def _match(self, leads: list[dict], sellers: list[dict]) -> list[dict]:
        """
        For each lead, find all eligible sellers and build assignment rows.
        Respects exclusivity (one seller per lead) and capacity limits.
        Uses SellerRankingEngine for 6-dimensional seller prioritisation.
        """
        from app.engines.seller_ranking_engine import SellerRankingEngine
        ranker = SellerRankingEngine(self.neo, self.settings)

        # Mutable capacity tracker keyed by seller_id
        capacity = {s['seller_id']: s['max_leads_per_day'] - s['leads_today']
                    for s in sellers}
        seller_map = {s['seller_id']: s for s in sellers}

        # Track which leads have already been assigned (for exclusivity)
        assigned_leads: set[str] = set()

        assignments: list[dict] = []

        for lead in leads:
            lead_id      = lead['lead_id']
            hs_chapter   = (lead.get('hs_code') or '')[:2]
            buyer_country= (lead.get('buyer_country') or '').upper()
            priority     = lead.get('priority', 'low')
            cost         = CREDIT_COST.get(priority, 0)

            # Rank sellers for this lead using 6-dimensional scoring
            ranked_sellers = ranker.rank_sellers_for_lead(lead, sellers)

            lead_assigned = False
            for seller in ranked_sellers:
                sid = seller['seller_id']

                if capacity.get(sid, 0) <= 0:
                    continue
                if seller_map[sid]['credits_available'] < cost:
                    continue

                chapter_match  = (not seller['hs_chapters']
                                  or hs_chapter in seller['hs_chapters'])
                country_match  = (not seller['target_countries']
                                  or buyer_country in seller['target_countries']
                                  or '*' in seller['target_countries'])

                if not chapter_match or not country_match:
                    continue

                reason = []
                if chapter_match and seller['hs_chapters']:
                    reason.append(f'hs_{hs_chapter}')
                if country_match and seller['target_countries']:
                    reason.append(f'geo_{buyer_country}')
                if not reason:
                    reason.append('open_match')

                assignments.append({
                    'seller_id':    sid,
                    'lead_id':      lead_id,
                    'match_reason': ','.join(reason),
                    'credits_cost': cost,
                    'rank_score':   seller.get('rank_score', 0),
                    'rank_reason':  seller.get('rank_reason', ''),
                })

                capacity[sid] = capacity.get(sid, 0) - 1
                seller_map[sid]['credits_available'] -= cost

                lead_assigned = True
                if self.exclusivity:
                    assigned_leads.add(lead_id)
                    break   # stop after first seller match

            if not lead_assigned:
                info(f'DistributionEngine: no seller matched lead {lead_id} (priority={priority})')

        return assignments

    def _write_assignments(self, assignments: list[dict]) -> int:
        if not assignments:
            return 0
        total = 0
        batch_size = 500
        for i in range(0, len(assignments), batch_size):
            chunk = assignments[i:i + batch_size]
            rows  = self.neo.run(_ASSIGN_QUERY, {'rows': chunk}) or [{'c': 0}]
            total += int(rows[0].get('c', 0))
        return total

    # ── seller management helpers (called from API) ───────────────────────────

    def upsert_seller(self, seller: dict) -> bool:
        """
        Create or update a Seller node.

        Required fields: seller_id, name
        Optional: tier, hs_chapters, target_countries, max_leads_per_day, credits_available
        """
        self.neo.run("""
            MERGE (s:Seller {seller_id: $sid})
            SET s.name               = $name,
                s.tier               = coalesce($tier, s.tier, 'basic'),
                s.hs_chapters        = coalesce($hs_chapters, s.hs_chapters, []),
                s.target_countries   = coalesce($countries, s.target_countries, []),
                s.max_leads_per_day  = coalesce($max_daily, s.max_leads_per_day, 50),
                s.credits_available  = coalesce($credits, s.credits_available, 0),
                s.active             = coalesce($active, s.active, true),
                s.leads_today        = coalesce(s.leads_today, 0),
                s.leads_today_date   = coalesce(s.leads_today_date, toString(date())),
                s.created_at         = coalesce(s.created_at, toString(datetime())),
                s.updated_at         = toString(datetime())
            RETURN s.seller_id AS id
        """, {
            'sid':        seller['seller_id'],
            'name':       seller.get('name', ''),
            'tier':       seller.get('tier'),
            'hs_chapters':seller.get('hs_chapters'),
            'countries':  seller.get('target_countries'),
            'max_daily':  seller.get('max_leads_per_day'),
            'credits':    seller.get('credits_available'),
            'active':     seller.get('active', True),
        })
        return True
