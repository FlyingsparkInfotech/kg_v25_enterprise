"""
TrademoRelationshipEnricher — enriches TradeRelationship nodes in Neo4j
with Trademo relationship health scores from raw.trademo_relationship_health.

The switch_lead_engine computes its own health_score via ML/rules from
shipment volume data. This enricher adds trademo_health_score as a
SEPARATE property sourced directly from Trademo's API, giving the stress
detection pipeline an additional authoritative signal.

If no ML score exists yet, trademo_health_score seeds health_score.
"""

from app.core.ids import utc_now
from app.core.logger import info, ok, warn, banner


class TrademoRelationshipEnricher:

    def __init__(self, neo, pg, settings):
        self.neo        = neo
        self.pg         = pg
        self._batch     = max(200, int(settings.runtime.batch_size))

    def run(self) -> dict:
        banner('TrademoRelationshipEnricher: Enriching TradeRelationships')
        rel_count = self._enrich_relationships()
        ok(f'TrademoRelationshipEnricher complete — relationships_enriched={rel_count}')
        return {'relationships_enriched': rel_count}

    # ── helpers ────────────────────────────────────────────────────────────────

    def _enrich_relationships(self) -> int:
        try:
            rows = self.pg.q("""
                SELECT
                    supplier_id,
                    buyer_id,
                    trade_relationship_health,
                    total_shipment_count,
                    shipment_trend,
                    last_shipment_date,
                    trade_from_date,
                    trade_to_date,
                    supplier_name,
                    buyer_name,
                    supplier_country,
                    buyer_country
                FROM raw.trademo_relationship_health
                WHERE trade_relationship_health IS NOT NULL
                  AND supplier_id IS NOT NULL
                  AND buyer_id IS NOT NULL
            """)
        except Exception as e:
            warn(f'TrademoRelationshipEnricher: cannot read raw.trademo_relationship_health: {e}')
            return 0

        if not rows:
            warn('TrademoRelationshipEnricher: no relationship health rows found')
            return 0

        info(f'TrademoRelationshipEnricher: {len(rows)} relationship health rows to apply')

        # Build batch
        now   = utc_now()
        batch = []
        for r in rows:
            try:
                health = float(r.get('trade_relationship_health') or 0)
            except (TypeError, ValueError):
                health = 0.0
            batch.append({
                'supplier_id':              str(r.get('supplier_id') or ''),
                'buyer_id':                 str(r.get('buyer_id')    or ''),
                'trademo_health_score':     health,
                'trademo_shipment_count':   int(r.get('total_shipment_count') or 0),
                'trademo_shipment_trend':   str(r.get('shipment_trend')       or ''),
                'trademo_last_shipment_date': str(r.get('last_shipment_date') or ''),
                'trademo_trade_from_date':  str(r.get('trade_from_date')      or ''),
                'trademo_trade_to_date':    str(r.get('trade_to_date')        or ''),
                'supplier_name':            str(r.get('supplier_name')        or ''),
                'buyer_name':               str(r.get('buyer_name')           or ''),
                'trademo_enriched_at':      now,
            })

        _CYPHER = """
        UNWIND $batch AS row
        MATCH (tr:TradeRelationship)
        WHERE tr.supplier_org_id = row.supplier_id
          AND tr.buyer_org_id    = row.buyer_id
        SET tr.trademo_health_score        = row.trademo_health_score,
            tr.trademo_shipment_count      = row.trademo_shipment_count,
            tr.trademo_shipment_trend      = row.trademo_shipment_trend,
            tr.trademo_last_shipment_date  = row.trademo_last_shipment_date,
            tr.trademo_trade_from_date     = row.trademo_trade_from_date,
            tr.trademo_trade_to_date       = row.trademo_trade_to_date,
            tr.trademo_enriched_at         = row.trademo_enriched_at,
            // Seed health_score from Trademo only when ML hasn't scored yet
            tr.health_score = CASE
                WHEN tr.health_score IS NULL
                THEN row.trademo_health_score
                // Trademo says relationship is WORSE than ML thinks — trust Trademo
                WHEN row.trademo_health_score < tr.health_score
                     AND coalesce(tr.score_override, false) = false
                THEN row.trademo_health_score
                ELSE tr.health_score
            END
        RETURN count(tr) AS c
        """

        total = 0
        for i in range(0, len(batch), self._batch):
            chunk  = batch[i:i + self._batch]
            result = self.neo.run(_CYPHER, {'batch': chunk})
            matched = int(result[0].get('c', 0)) if result else 0
            total  += matched
            info(f'TrademoRelationshipEnricher: +{matched} (chunk {i//self._batch + 1}) total={total}')

        unmatched = len(batch) - total
        if unmatched:
            warn(f'TrademoRelationshipEnricher: {unmatched} rows had no matching TradeRelationship node '
                 f'(run build-trade-relationships first)')

        return total
