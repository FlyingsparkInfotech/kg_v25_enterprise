"""
ConversionEngine — Closed-loop conversion fact tracking.

When a seller marks a SellerLeadAssignment as 'converted', this engine:
  1. Creates a ``ConversionFact`` node capturing the outcome.
  2. Links it back to the originating Lead, buyer Organization, and
     matched Supplier.
  3. Updates the Lead status to 'converted'.
  4. Computes days_to_convert and stores it for ML label generation.

ConversionFact nodes feed directly into the switch classifier retraining
pipeline (build_dataset.py) as positive labels.

Usage:
    python main.py track-conversions --config config.yaml
"""

from app.core.ids import stable_id, utc_now
from app.core.logger import info, ok, warn, banner


_PENDING_CONVERSIONS_QUERY = """
MATCH (a:SellerLeadAssignment)
WHERE a.status = 'converted'
  AND NOT (a)-[:TRIGGERED_CONVERSION]->(:ConversionFact)
MATCH (a)-[:FOR_LEAD]->(l:Lead)
RETURN a.assignment_id    AS assignment_id,
       a.seller_id        AS seller_id,
       a.lead_id          AS lead_id,
       a.assigned_at      AS assigned_at,
       a.actioned_at      AS converted_at,
       l.lead_type        AS lead_type,
       l.lead_grain       AS lead_grain,
       l.final_score      AS score,
       l.evidence_strength AS evidence_strength,
       l.buyer_account_key AS buyer_key,
       l.identity_confidence AS id_conf,
       l.hs_code          AS hs_code
LIMIT $batch
"""

_WRITE_CONVERSION_QUERY = """
UNWIND $rows AS row
MATCH (a:SellerLeadAssignment {assignment_id: row.assignment_id})
MATCH (a)-[:FOR_LEAD]->(l:Lead)
MERGE (cf:ConversionFact {conversion_id: row.conversion_id})
SET cf.assignment_id     = row.assignment_id,
    cf.seller_id         = row.seller_id,
    cf.lead_id           = row.lead_id,
    cf.lead_type         = row.lead_type,
    cf.lead_grain        = row.lead_grain,
    cf.buyer_key         = row.buyer_key,
    cf.hs_code           = row.hs_code,
    cf.final_score       = row.score,
    cf.evidence_strength = row.evidence_strength,
    cf.identity_confidence = row.id_conf,
    cf.deal_value        = coalesce(row.deal_value, 0),
    cf.days_to_convert   = row.days_to_convert,
    cf.converted_at      = row.converted_at,
    cf.created_at        = toString(datetime())
MERGE (a)-[:TRIGGERED_CONVERSION]->(cf)
MERGE (l)-[:HAS_CONVERSION]->(cf)
SET l.status         = 'converted',
    l.converted_at   = row.converted_at,
    l.days_to_convert = row.days_to_convert
RETURN count(cf) AS c
"""

_LINK_BUYER_SUPPLIER_QUERY = """
MATCH (cf:ConversionFact)
WHERE NOT (cf)-[:CONVERTED_BUYER]->() AND cf.buyer_key IS NOT NULL
WITH cf LIMIT $batch
OPTIONAL MATCH (o:Organization)
WHERE toLower(trim(coalesce(o.orgId, o.domain, ''))) = toLower(trim(cf.buyer_key))
   OR toLower(trim(coalesce(o.name, ''))) = toLower(trim(cf.buyer_key))
FOREACH (_ IN CASE WHEN o IS NOT NULL THEN [1] ELSE [] END |
    MERGE (cf)-[:CONVERTED_BUYER]->(o)
)
RETURN count(cf) AS c
"""

_CONVERSION_STATS_QUERY = """
MATCH (cf:ConversionFact)
RETURN cf.lead_type        AS lead_type,
       count(cf)           AS conversions,
       avg(cf.days_to_convert) AS avg_days,
       avg(cf.final_score)    AS avg_score
ORDER BY conversions DESC
"""


def _parse_dt(ts: str):
    """Parse ISO timestamp to a comparable value (seconds since epoch)."""
    from datetime import datetime, timezone
    if not ts:
        return None
    try:
        ts = ts.replace('Z', '+00:00').replace(' ', 'T')
        return datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)
    except Exception:
        return None


class ConversionEngine:

    def __init__(self, neo, settings):
        self.neo   = neo
        self.batch = int(settings.runtime.batch_size)

    # ── public ────────────────────────────────────────────────────────────────

    def run(self) -> dict:
        banner('ConversionEngine: Tracking seller conversions')
        rows = self._fetch_pending()
        if not rows:
            ok('ConversionEngine: no pending conversions')
            return {'recorded': 0}

        enriched = self._enrich(rows)
        total    = self._write(enriched)
        self._link_buyer_supplier()

        ok(f'ConversionEngine: recorded {total} ConversionFact nodes')
        return {'recorded': total}

    def get_stats(self) -> list[dict]:
        rows = self.neo.run(_CONVERSION_STATS_QUERY) or []
        return [dict(r) for r in rows]

    # ── internal ──────────────────────────────────────────────────────────────

    def _fetch_pending(self) -> list[dict]:
        rows = self.neo.run(_PENDING_CONVERSIONS_QUERY, {'batch': self.batch}) or []
        return [dict(r) for r in rows]

    def _enrich(self, rows: list[dict]) -> list[dict]:
        """Compute conversion_id and days_to_convert for each row."""
        enriched = []
        for r in rows:
            assigned_dt  = _parse_dt(r.get('assigned_at', ''))
            converted_dt = _parse_dt(r.get('converted_at', ''))

            if assigned_dt and converted_dt:
                days = max(0, (converted_dt - assigned_dt).days)
            else:
                days = None

            enriched.append({
                **r,
                'conversion_id':   stable_id(r['assignment_id'], 'conversion'),
                'days_to_convert': days,
                'deal_value':      0,   # populated externally if deal value is known
            })
        return enriched

    def _write(self, rows: list[dict]) -> int:
        if not rows:
            return 0
        total = 0
        batch_size = 500
        for i in range(0, len(rows), batch_size):
            chunk = rows[i:i + batch_size]
            res   = self.neo.run(_WRITE_CONVERSION_QUERY, {'rows': chunk}) or [{'c': 0}]
            total += int(res[0].get('c', 0))
        return total

    def _link_buyer_supplier(self):
        self.neo.run(_LINK_BUYER_SUPPLIER_QUERY, {'batch': self.batch})
        info('ConversionEngine: buyer/supplier links updated')
