"""
TrademoCompanyProfileEnricher — enriches Organization nodes in Neo4j
with Trademo company-level trade intelligence from raw.trademo_company_profile.

Properties written to Organization nodes:
  trademo_trade_health_score  — 0-100 trade health score (from Trademo API)
  trademo_supplier_risk_score — supplier risk score
  trademo_total_shipments     — total shipment count
  trademo_import_volume       — import volume
  trademo_export_volume       — export volume
  trademo_trading_partners    — number of trading partners
  trademo_company_type        — company type string
  trademo_enriched_at         — timestamp of enrichment

Matching strategy (in order of specificity):
  1. orgId = company_id           (exact Trademo ID match)
  2. trademo_company_id = company_id  (if previously set)
  3. toLower(orgName) = toLower(company_name)  (name fallback)
"""

from app.core.ids import utc_now
from app.core.logger import info, ok, warn, banner


class TrademoCompanyProfileEnricher:

    def __init__(self, neo, pg, settings):
        self.neo    = neo
        self.pg     = pg
        self._batch = max(200, int(settings.runtime.batch_size))

    def run(self) -> dict:
        banner('TrademoCompanyProfileEnricher: Enriching Organizations')
        orgs = self._enrich_organizations()
        ok(f'TrademoCompanyProfileEnricher complete — organizations_enriched={orgs}')
        return {'organizations_enriched': orgs}

    # ── helpers ────────────────────────────────────────────────────────────────

    def _enrich_organizations(self) -> int:
        try:
            rows = self.pg.q("""
                SELECT DISTINCT ON (company_id)
                    company_id,
                    company_name,
                    country,
                    state,
                    city,
                    company_type,
                    trade_health_score,
                    supplier_risk_score,
                    total_shipment_count,
                    total_import_count,
                    total_export_count,
                    trading_partner_count,
                    import_volume,
                    export_volume
                FROM raw.trademo_company_profile
                WHERE company_id IS NOT NULL
                ORDER BY company_id, modified_on DESC NULLS LAST
            """)
        except Exception as e:
            warn(f'TrademoCompanyProfileEnricher: cannot read raw.trademo_company_profile: {e}')
            return 0

        if not rows:
            warn('TrademoCompanyProfileEnricher: no company profile rows found')
            return 0

        info(f'TrademoCompanyProfileEnricher: {len(rows)} company profiles to apply')

        def _f(v):
            """Safe float."""
            try:
                return float(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        def _i(v):
            """Safe int."""
            try:
                return int(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        now   = utc_now()
        batch = []
        for r in rows:
            cid = str(r.get('company_id') or '').strip()
            if not cid:
                continue
            batch.append({
                'company_id':           cid,
                'company_name':         str(r.get('company_name') or '').strip(),
                'trade_health_score':   _f(r.get('trade_health_score')),
                'supplier_risk_score':  _f(r.get('supplier_risk_score')),
                'total_shipment_count': _i(r.get('total_shipment_count')),
                'total_import_count':   _i(r.get('total_import_count')),
                'total_export_count':   _i(r.get('total_export_count')),
                'trading_partner_count':_i(r.get('trading_partner_count')),
                'import_volume':        _f(r.get('import_volume')),
                'export_volume':        _f(r.get('export_volume')),
                'company_type':         str(r.get('company_type') or ''),
                'country':              str(r.get('country')      or ''),
                'enriched_at':          now,
            })

        # Three-pass matching: exact ID → previously-tagged → name fallback
        _CYPHER = """
        UNWIND $batch AS row
        OPTIONAL MATCH (o1:Organization {orgId: row.company_id})
        OPTIONAL MATCH (o2:Organization {trademo_company_id: row.company_id})
        OPTIONAL MATCH (o3:Organization)
          WHERE row.company_name <> ''
            AND toLower(o3.orgName) = toLower(row.company_name)
            AND o1 IS NULL AND o2 IS NULL
        WITH row,
             coalesce(o1, o2, o3) AS org
        WHERE org IS NOT NULL
        SET org.trademo_company_id          = row.company_id,
            org.trademo_trade_health_score  = row.trade_health_score,
            org.trademo_supplier_risk_score = row.supplier_risk_score,
            org.trademo_total_shipments     = row.total_shipment_count,
            org.trademo_total_imports       = row.total_import_count,
            org.trademo_total_exports       = row.total_export_count,
            org.trademo_trading_partners    = row.trading_partner_count,
            org.trademo_import_volume       = row.import_volume,
            org.trademo_export_volume       = row.export_volume,
            org.trademo_company_type        = row.company_type,
            org.trademo_enriched_at         = row.enriched_at
        RETURN count(org) AS c
        """

        total = 0
        for i in range(0, len(batch), self._batch):
            chunk  = batch[i:i + self._batch]
            result = self.neo.run(_CYPHER, {'batch': chunk})
            matched = int(result[0].get('c', 0)) if result else 0
            total  += matched
            info(f'TrademoCompanyProfileEnricher: +{matched} (chunk {i//self._batch + 1}) total={total}')

        unmatched = len(batch) - total
        if unmatched:
            warn(f'TrademoCompanyProfileEnricher: {unmatched} company profiles had no matching '
                 f'Organization node in Neo4j')

        return total
