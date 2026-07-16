"""
FeatureExtractor: Builds ML feature vectors from Neo4j graph data.
Used by both training (build_dataset.py) and inference (switch_lead_engine.py).
"""

from datetime import datetime, timezone
from statistics import mean, stdev

from app.core.logger import info, ok, warn, banner


class FeatureExtractor:

    def __init__(self, neo, settings):
        self.neo = neo
        self.settings = settings

    def _days_ago(self, date_str: str) -> int:
        if not date_str:
            return 9999
        try:
            s = str(date_str).strip().replace('Z', '+00:00')
            for fmt in ('%Y-%m-%dT%H:%M:%S+00:00', '%Y-%m-%dT%H:%M:%S',
                        '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
                try:
                    dt = datetime.strptime(s[:len(fmt)], fmt)
                    break
                except ValueError:
                    continue
            else:
                dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return max(0, (datetime.now(timezone.utc) - dt).days)
        except Exception:
            return 9999

    def _encode_industry(self, industry: str) -> int:
        return abs(hash(str(industry or '').lower())) % 100

    def extract_relationship_features(self) -> 'pd.DataFrame':
        import pandas as pd
        banner('FeatureExtractor: extracting relationship features')

        rel_rows = self.neo.run("""
            MATCH (tr:TradeRelationship)
            RETURN tr.rel_id                    AS rel_id,
                   tr.buyer_org_id              AS buyer_org_id,
                   tr.supplier_org_id           AS supplier_org_id,
                   tr.hs_code                   AS hs_code,
                   tr.relationship_age_months   AS relationship_age_months,
                   tr.total_shipments           AS total_shipments,
                   tr.baseline_avg_monthly_qty  AS baseline_avg_monthly_qty,
                   tr.last_shipment_date        AS last_shipment_date,
                   coalesce(tr.anomaly_score, 0.0) AS anomaly_score
        """)
        if not rel_rows:
            warn('FeatureExtractor: no TradeRelationship nodes found')
            return pd.DataFrame()

        info(f'FeatureExtractor: {len(rel_rows)} relationships to featurize')

        # Recent 6-month snapshots per relationship
        snap6 = self.neo.run("""
            MATCH (tr:TradeRelationship)-[:HAS_SNAPSHOT]->(snap:RelationshipSnapshot)
            WHERE snap.year_month >= toString(date() - duration({months: 6}))
            WITH tr.rel_id AS rel_id,
                 collect({ym: snap.year_month, qty: coalesce(snap.total_quantity, 0.0)}) AS snaps
            RETURN rel_id, snaps
        """)
        snap6_map = {r['rel_id']: r['snaps'] for r in snap6}

        # 3-month avg
        snap3 = self.neo.run("""
            MATCH (tr:TradeRelationship)-[:HAS_SNAPSHOT]->(snap:RelationshipSnapshot)
            WHERE snap.year_month >= toString(date() - duration({months: 3}))
            WITH tr.rel_id AS rel_id, avg(coalesce(snap.total_quantity, 0.0)) AS avg_qty_3m
            RETURN rel_id, avg_qty_3m
        """)
        snap3_map = {r['rel_id']: float(r.get('avg_qty_3m') or 0) for r in snap3}

        # Buyer supplier count per hs_code
        bsc = self.neo.run("""
            MATCH (tr:TradeRelationship)
            WITH tr.buyer_org_id AS bid, tr.hs_code AS hc,
                 count(DISTINCT tr.supplier_org_id) AS sc
            RETURN bid, hc, sc
        """)
        bsc_map = {(r['bid'], r['hc']): int(r.get('sc') or 0) for r in bsc}

        # Buyer ZoomInfo attributes
        bzi = self.neo.run("""
            MATCH (o:Organization) WHERE o.orgId IS NOT NULL
            RETURN o.orgId                               AS org_id,
                   coalesce(o.zi_employee_count, 0)      AS employee_count,
                   coalesce(o.zi_industry, '')           AS industry,
                   coalesce(o.zi_intent_signal_count, 0) AS intent_count,
                   CASE WHEN o.zi_has_leadership_change = true THEN 1 ELSE 0 END AS leadership_chg,
                   CASE WHEN o.zi_has_funding_event     = true THEN 1 ELSE 0 END AS funding,
                   CASE WHEN o.zi_has_ma_event          = true THEN 1 ELSE 0 END AS ma_event
        """)
        bzi_map = {r['org_id']: r for r in bzi}

        # Supplier activity (last 90 days)
        sact = self.neo.run("""
            MATCH (tr:TradeRelationship)-[:HAS_SNAPSHOT]->(snap:RelationshipSnapshot)
            WHERE snap.year_month >= toString(date() - duration({days: 90}))
            WITH tr.supplier_org_id AS sid,
                 count(DISTINCT tr.buyer_org_id)        AS active_buyers,
                 sum(coalesce(snap.total_quantity, 0.0)) AS recent_vol
            RETURN sid, active_buyers, recent_vol
        """)
        sact_map = {r['sid']: r for r in sact}

        # HS code market depth
        hsd = self.neo.run("""
            MATCH (tr:TradeRelationship)
            WITH tr.hs_code AS hc, count(DISTINCT tr.supplier_org_id) AS sc
            RETURN hc, sc
        """)
        hsd_map = {r['hc']: int(r.get('sc') or 0) for r in hsd}

        records = []
        for rel in rel_rows:
            rid          = rel['rel_id']
            buyer_id     = rel.get('buyer_org_id') or ''
            supplier_id  = rel.get('supplier_org_id') or ''
            hs_code      = rel.get('hs_code') or ''
            baseline_qty = float(rel.get('baseline_avg_monthly_qty') or 0)
            last_date    = rel.get('last_shipment_date') or ''

            snaps6 = sorted(snap6_map.get(rid, []), key=lambda s: str(s.get('ym') or ''))
            qtys6  = [float(s.get('qty') or 0) for s in snaps6]

            avg_6m       = (sum(qtys6) / len(qtys6)) if qtys6 else 0.0
            qty_trend_6m = (avg_6m / baseline_qty) if baseline_qty > 0 else 0.0
            avg_3m       = snap3_map.get(rid, 0.0)
            qty_trend_3m = (avg_3m / baseline_qty) if baseline_qty > 0 else 0.0

            if len(qtys6) >= 2:
                m = mean(qtys6)
                regularity = (stdev(qtys6) / m) if m > 0 else 0.0
            else:
                regularity = 0.0

            months_zero = max(0, 6 - len(qtys6)) + len([q for q in qtys6 if q == 0])

            zi   = bzi_map.get(buyer_id, {})
            sa   = sact_map.get(supplier_id, {})

            records.append({
                'rel_id':                        rid,
                'buyer_org_id':                  buyer_id,
                'supplier_org_id':               supplier_id,
                'hs_code':                       hs_code,
                'relationship_age_months':       int(rel.get('relationship_age_months') or 0),
                'total_shipments':               int(rel.get('total_shipments') or 0),
                'baseline_avg_monthly_qty':      baseline_qty,
                'last_shipment_date_days_ago':   self._days_ago(last_date),
                'qty_trend_3m':                  round(qty_trend_3m, 4),
                'qty_trend_6m':                  round(qty_trend_6m, 4),
                'shipment_regularity':           round(regularity, 4),
                'months_with_zero_shipments':    months_zero,
                'buyer_total_supplier_count':    bsc_map.get((buyer_id, hs_code), 0),
                'buyer_zi_employee_count':       int(zi.get('employee_count') or 0),
                'buyer_zi_industry_encoded':     self._encode_industry(zi.get('industry') or ''),
                'buyer_zi_intent_signal_count':  int(zi.get('intent_count') or 0),
                'buyer_zi_has_leadership_change': int(zi.get('leadership_chg') or 0),
                'buyer_zi_has_funding_event':    int(zi.get('funding') or 0),
                'buyer_zi_has_ma_event':         int(zi.get('ma_event') or 0),
                'supplier_active_buyer_count':   int(sa.get('active_buyers') or 0),
                'supplier_recent_export_volume': float(sa.get('recent_vol') or 0),
                'hs_code_market_supplier_count': hsd_map.get(hs_code, 0),
                'anomaly_score':                 float(rel.get('anomaly_score') or 0.0),
            })

        df = pd.DataFrame(records)
        ok(f'FeatureExtractor: {len(df)} rows, {len(df.columns)} features')
        return df

    def extract_supplier_candidates(
        self,
        hs_code: str,
        exclude_supplier_id: str,
        buyer_country: str = None,
    ) -> 'pd.DataFrame':
        import pandas as pd

        hs_chapter = hs_code[:4] if len(hs_code) >= 4 else hs_code

        supplier_rows = self.neo.run(
            """
            MATCH (tr:TradeRelationship)
            WHERE (tr.hs_code = $hs_code OR tr.hs_code STARTS WITH $hs_chapter)
              AND tr.supplier_org_id <> $exclude_id
              AND tr.supplier_org_id IS NOT NULL
            WITH tr.supplier_org_id AS supplier_id,
                 tr.supplier_name   AS supplier_name,
                 (tr.hs_code = $hs_code) AS exact_match,
                 tr.hs_code         AS matched_hs
            RETURN supplier_id, supplier_name, exact_match, matched_hs
            """,
            {'hs_code': hs_code, 'hs_chapter': hs_chapter, 'exclude_id': exclude_supplier_id}
        )

        if not supplier_rows:
            return pd.DataFrame()

        # Deduplicate, prefer exact match
        seen: dict = {}
        for row in supplier_rows:
            sid = row['supplier_id']
            if sid not in seen or row['exact_match']:
                seen[sid] = row

        supplier_ids = list(seen.keys())

        activity = self.neo.run(
            """
            UNWIND $ids AS sid
            MATCH (tr:TradeRelationship)
            WHERE tr.supplier_org_id = sid
            OPTIONAL MATCH (tr)-[:HAS_SNAPSHOT]->(snap:RelationshipSnapshot)
            WHERE snap.year_month >= toString(date() - duration({days: 90}))
            WITH sid,
                 count(DISTINCT tr.buyer_org_id)        AS active_buyers,
                 sum(coalesce(snap.total_quantity, 0.0)) AS recent_vol,
                 max(snap.year_month)                    AS last_export
            RETURN sid AS supplier_id, active_buyers, recent_vol, last_export
            """,
            {'ids': supplier_ids}
        )
        act_map = {r['supplier_id']: r for r in activity}

        zi_rows = self.neo.run(
            """
            UNWIND $ids AS sid
            MATCH (o:Organization {orgId: sid})
            RETURN o.orgId AS supplier_id,
                   coalesce(o.zi_employee_count, 0) AS employee_count
            """,
            {'ids': supplier_ids}
        )
        zi_map = {r['supplier_id']: r for r in zi_rows}

        contact_rows = self.neo.run(
            """
            UNWIND $ids AS sid
            MATCH (p:Person)-[:CONTACT_AT]->(o:Organization {orgId: sid})
            RETURN o.orgId AS supplier_id, count(p) AS contact_count
            """,
            {'ids': supplier_ids}
        )
        contact_map = {r['supplier_id']: int(r.get('contact_count') or 0) for r in contact_rows}

        country_set: set = set()
        if buyer_country:
            cr = self.neo.run(
                """
                UNWIND $ids AS sid
                MATCH (tr:TradeRelationship {supplier_org_id: sid})
                MATCH (buyer:Organization {orgId: tr.buyer_org_id})
                WHERE toLower(coalesce(buyer.zi_country, buyer.country, '')) CONTAINS toLower($country)
                RETURN DISTINCT sid AS supplier_id
                """,
                {'ids': supplier_ids, 'country': buyer_country}
            )
            country_set = {r['supplier_id'] for r in cr}

        records = []
        for sid, base in seen.items():
            act = act_map.get(sid, {})
            zi  = zi_map.get(sid, {})
            records.append({
                'supplier_org_id':           sid,
                'supplier_name':             str(base.get('supplier_name') or ''),
                'hs_exact_match':            1.0 if base.get('exact_match') else 0.0,
                'hs_chapter_match':          1.0 if str(base.get('matched_hs') or '')[:4] == hs_chapter else 0.0,
                'active_buyer_count':        int(act.get('active_buyers') or 0),
                'recent_export_volume':      float(act.get('recent_vol') or 0),
                'exports_to_buyer_country':  1 if (buyer_country and sid in country_set) else 0,
                'supplier_zi_employee_count': int(zi.get('employee_count') or 0),
                'has_zi_contact':            1 if contact_map.get(sid, 0) > 0 else 0,
                'export_recency_days':       self._days_ago(str(act.get('last_export') or '')),
            })

        df = pd.DataFrame(records)
        info(f'FeatureExtractor.extract_supplier_candidates: {len(df)} candidates for {hs_code}')
        return df
