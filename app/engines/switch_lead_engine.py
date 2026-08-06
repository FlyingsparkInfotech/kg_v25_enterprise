"""
SwitchLeadEngine: 6-stage pipeline for supplier switch lead detection.

Stage 1: build_trade_relationships()  — TradeRelationship + RelationshipSnapshot
Stage 2: enrich_with_zoominfo()       — ZoomInfo firmographics + contacts + intent
Stage 3: detect_stress()              — health model → SupplierSwitchOpportunity
Stage 4: score_switch_probability()   — switch classifier → probability score
Stage 5: match_suppliers()            — supplier ranker → SupplierMatch nodes
Stage 6: create_switch_leads()        — SwitchLead nodes with recommended_action
"""

import re

from app.core.ids import stable_id, utc_now
from app.core.logger import info, ok, warn, banner
from app.models.model_registry import ModelRegistry


def _clean_hs(hs_raw: str) -> str:
    """Strip list brackets/spaces from raw Neo4j HS code for display."""
    return re.sub(r'[\[\]\s]', '', str(hs_raw or ''))


class SwitchLeadEngine:

    def __init__(self, neo, pg, settings, tracker=None):
        self.neo      = neo
        self.pg       = pg
        self.settings = settings
        self.tracker  = tracker
        self.batch    = max(100, int(settings.runtime.batch_size or 5000))
        self.registry = ModelRegistry(settings)

    def _log(self, name: str, value, step=None):
        if self.tracker:
            try:
                self.tracker.metric(name, float(value), step=step)
            except Exception:
                pass

    # ── Stage 1 ────────────────────────────────────────────────────────────────

    def build_trade_relationships(self) -> int:
        banner('SwitchLeadEngine Stage 1: Build Trade Relationships')
        from app.features.trade_aggregator import TradeAggregator
        result = TradeAggregator(self.neo, self.pg, self.settings).run()
        self._log('trade_relationships', result.get('relationships', 0))
        self._log('relationship_snapshots', result.get('snapshots', 0))
        return result.get('relationships', 0)

    # ── Stage 2 ────────────────────────────────────────────────────────────────

    def enrich_with_zoominfo(self) -> int:
        banner('SwitchLeadEngine Stage 2: ZoomInfo Enrichment')
        from app.features.zoominfo_enricher import ZoomInfoEnricher
        result = ZoomInfoEnricher(self.neo, self.pg, self.settings).run()
        self._log('orgs_enriched', result.get('orgs_enriched', 0))
        return result.get('orgs_enriched', 0)

    # ── Stage 3A: Rule-based stress detection ─────────────────────────────────

    def detect_stress_rules(self) -> int:
        """
        Rule-based supplier stress detection — runs on explicit thresholds derived
        from observable trade data.  Works with as little as 1-2 data points because
        the rules are deterministic; no training data required.

        Rules (evaluated in order, first match wins):

          CHURNED   — no shipments in last 6 months (180 days) AND had prior activity
          DORMANT   — no shipments in last 3 months (90 days)  OR volume dropped ≥ 70%
          STRESSED  — volume dropped 30-69% vs baseline       OR gap 45-89 days
          IRREGULAR — shipment frequency halved vs baseline     (moderate concern)
          HEALTHY   — none of the above apply

        Stress score:
          churned   → 95   dormant → 80   stressed → 60   irregular → 35   healthy → 10

        All results are written to TradeRelationship nodes as rule_* properties so
        they can be compared side-by-side with the ML model output.
        """
        banner('SwitchLeadEngine Stage 3A: Rule-Based Stress Detection')
        from datetime import datetime, timezone

        rows = self.neo.run("""
            MATCH (tr:TradeRelationship)
            OPTIONAL MATCH (tr)-[:HAS_SNAPSHOT]->(snap:RelationshipSnapshot)
            RETURN tr.rel_id                    AS rel_id,
                   tr.last_shipment_date        AS last_shipment_date,
                   tr.baseline_avg_monthly_qty  AS baseline_avg_qty,
                   tr.buyer_org_id              AS buyer_org_id,
                   tr.supplier_org_id           AS supplier_org_id,
                   tr.hs_code                   AS hs_code,
                   tr.buyer_name                AS buyer_name,
                   tr.supplier_name             AS supplier_name,
                   tr.buyer_monthly_volume      AS buyer_monthly_volume,
                   collect({
                       year_month:        snap.year_month,
                       shipment_count:    snap.shipment_count,
                       total_quantity:    snap.total_quantity,
                       qty_vs_baseline:   snap.qty_vs_baseline_pct
                   }) AS snapshots
        """)

        if not rows:
            warn('detect_stress_rules: no TradeRelationship data found')
            return 0

        now_dt  = datetime.now(timezone.utc)
        now_str = utc_now()
        update_batch = []
        opp_batch    = []

        for row in rows:
            rel_id     = row['rel_id']
            last_raw   = str(row.get('last_shipment_date') or '')
            baseline   = float(row.get('baseline_avg_qty') or 0)
            snapshots  = [s for s in (row.get('snapshots') or []) if s.get('year_month')]

            # ── days since last shipment ───────────────────────────────────────
            days_silent = 9999
            if last_raw:
                try:
                    for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
                        try:
                            dt = datetime.strptime(last_raw[:len(fmt)], fmt)
                            break
                        except ValueError:
                            continue
                    else:
                        dt = datetime.fromisoformat(last_raw.replace('Z', '+00:00'))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    days_silent = (now_dt - dt).days
                except Exception:
                    pass

            # ── volume drop vs baseline (most recent 2 snapshots) ─────────────
            recent_snaps = sorted(snapshots, key=lambda s: s.get('year_month', ''), reverse=True)[:2]
            recent_qty   = sum(float(s.get('total_quantity') or 0) for s in recent_snaps)
            recent_count = sum(int(s.get('shipment_count') or 0)  for s in recent_snaps)

            vol_drop_pct = 0.0
            if baseline > 0 and len(recent_snaps) > 0:
                avg_recent   = recent_qty / len(recent_snaps)
                vol_drop_pct = max(0.0, (baseline - avg_recent) / baseline)

            has_history = len(snapshots) > 0

            # ── baseline shipment frequency (avg per month) ───────────────────
            all_counts  = [int(s.get('shipment_count') or 0) for s in snapshots]
            baseline_freq = (sum(all_counts) / len(all_counts)) if all_counts else 0
            recent_freq   = (recent_count / len(recent_snaps)) if recent_snaps else 0
            freq_halved   = baseline_freq > 0 and recent_freq < (baseline_freq * 0.5)

            # ── apply rules ───────────────────────────────────────────────────
            if has_history and days_silent > 180:
                rule_status = 'churned';    rule_score = 95; rule_reason = 'no_shipment_6_months'
            elif days_silent > 90 or (has_history and vol_drop_pct >= 0.70):
                rule_status = 'dormant';   rule_score = 80; rule_reason = (
                    'no_shipment_3_months' if days_silent > 90 else 'volume_collapsed_70pct')
            elif vol_drop_pct >= 0.30 or (45 <= days_silent <= 90):
                rule_status = 'stressed';  rule_score = 60; rule_reason = (
                    f'volume_drop_{int(vol_drop_pct*100)}pct' if vol_drop_pct >= 0.30 else 'shipment_gap_45_90d')
            elif freq_halved:
                rule_status = 'irregular'; rule_score = 35; rule_reason = 'shipment_frequency_halved'
            else:
                rule_status = 'healthy';   rule_score = 10; rule_reason = 'no_stress_signals'

            update_batch.append({
                'rel_id':           rel_id,
                'rule_status':      rule_status,
                'rule_score':       rule_score,
                'rule_reason':      rule_reason,
                'rule_days_silent': days_silent if days_silent < 9999 else None,
                'rule_vol_drop_pct': round(vol_drop_pct * 100, 1),
                'rule_detected_at': now_str,
            })

            if rule_status in ('stressed', 'dormant', 'churned'):
                buyer_id    = str(row.get('buyer_org_id')       or '')
                supplier_id = str(row.get('supplier_org_id')    or '')
                hs_code     = str(row.get('hs_code')            or '')
                opp_batch.append({
                    'opportunity_id':       stable_id(buyer_id, supplier_id, hs_code, 'rule_opp'),
                    'rel_id':               rel_id,
                    'buyer_org_id':         buyer_id,
                    'buyer_name':           str(row.get('buyer_name')          or ''),
                    'supplier_org_id':      supplier_id,
                    'supplier_name':        str(row.get('supplier_name')        or ''),
                    'hs_code':              hs_code,
                    'stress_reason':        rule_reason,
                    'stress_score':         rule_score,
                    'buyer_monthly_volume': float(row.get('buyer_monthly_volume') or 0),
                    'detection_method':     'rule_based',
                    'first_detected_at':    now_str,
                })

        # Write rule results onto TradeRelationship nodes
        for i in range(0, len(update_batch), self.batch):
            chunk = update_batch[i:i + self.batch]
            self.neo.run("""
                UNWIND $batch AS row
                MATCH (tr:TradeRelationship {rel_id: row.rel_id})
                SET tr.rule_health_status  = row.rule_status,
                    tr.rule_stress_score   = row.rule_score,
                    tr.rule_stress_reason  = row.rule_reason,
                    tr.rule_days_silent    = row.rule_days_silent,
                    tr.rule_vol_drop_pct   = row.rule_vol_drop_pct,
                    tr.rule_detected_at    = row.rule_detected_at
                RETURN count(tr) AS c
            """, {'batch': chunk})

        # Write rule-detected opportunities (tagged detection_method='rule_based')
        total_opps = 0
        for i in range(0, len(opp_batch), self.batch):
            chunk = opp_batch[i:i + self.batch]
            self.neo.run("""
                UNWIND $batch AS row
                MERGE (opp:SupplierSwitchOpportunity {opportunity_id: row.opportunity_id})
                SET opp.buyer_org_id              = row.buyer_org_id,
                    opp.buyer_name                = row.buyer_name,
                    opp.hs_code                   = row.hs_code,
                    opp.existing_supplier_org_id  = row.supplier_org_id,
                    opp.existing_supplier_name    = row.supplier_name,
                    opp.stress_reason             = row.stress_reason,
                    opp.stress_score              = row.stress_score,
                    opp.switch_probability        = null,
                    opp.buyer_monthly_volume      = row.buyer_monthly_volume,
                    opp.detection_method          = row.detection_method,
                    opp.status                    = 'open',
                    opp.first_detected_at         = row.first_detected_at
                WITH opp, row
                MATCH (tr:TradeRelationship {rel_id: row.rel_id})
                MERGE (tr)-[:TRIGGERED]->(opp)
                RETURN count(opp) AS c
            """, {'batch': chunk})
            total_opps += len(chunk)

        healthy = sum(1 for u in update_batch if u['rule_status'] == 'healthy')
        stressed = len(opp_batch)
        info(f'  → Rule-based: {healthy} healthy, {stressed} stressed/dormant/churned')
        ok(f'SwitchLeadEngine Stage 3A complete: {stressed} rule-based opportunities')
        self._log('rule_based_opportunities', stressed)
        return stressed

    # ── Stage 3 ────────────────────────────────────────────────────────────────

    def detect_stress(self) -> int:
        banner('SwitchLeadEngine Stage 3: Detect Relationship Stress')
        import pandas as pd

        detector = self.registry.load_health_detector()

        # Fetch all relationships + snapshots
        rows = self.neo.run("""
            MATCH (tr:TradeRelationship)-[:HAS_SNAPSHOT]->(snap:RelationshipSnapshot)
            RETURN tr.rel_id             AS rel_id,
                   tr.buyer_org_id       AS buyer_org_id,
                   tr.supplier_org_id    AS supplier_org_id,
                   tr.hs_code            AS hs_code,
                   tr.buyer_name         AS buyer_name,
                   tr.supplier_name      AS supplier_name,
                   tr.baseline_avg_monthly_qty AS baseline_avg_qty,
                   tr.last_shipment_date AS last_shipment_date,
                   snap.year_month       AS year_month,
                   snap.total_quantity   AS total_quantity,
                   snap.total_value      AS total_value,
                   snap.shipment_count   AS shipment_count,
                   snap.qty_vs_baseline_pct AS qty_vs_baseline_pct
        """)

        if not rows:
            warn('SwitchLeadEngine.detect_stress: no TradeRelationship+Snapshot data')
            return 0

        df = pd.DataFrame(rows)
        df['total_quantity'] = pd.to_numeric(df['total_quantity'], errors='coerce').fillna(0)

        health_df = detector.predict(df)
        if health_df.empty:
            warn('SwitchLeadEngine.detect_stress: health detector returned no results')
            return 0

        # Enrich with relationship metadata
        meta = df.drop_duplicates('rel_id').set_index('rel_id')

        # Update TradeRelationship nodes
        update_batch = []
        opp_batch    = []
        now          = utc_now()

        for _, hrow in health_df.iterrows():
            rid    = hrow['rel_id']
            status = hrow['health_status']
            score  = float(hrow['health_score'])
            ascore = float(hrow['anomaly_score'])
            trend  = hrow['trend_direction']

            update_batch.append({
                'rel_id':          rid,
                'health_score':    score,
                'health_status':   status,
                'anomaly_score':   ascore,
                'trend_direction': trend,
                'updated_at':      now,
            })

            if status in ('stressed', 'dormant', 'churned'):
                mrow = meta.loc[rid] if rid in meta.index else {}
                buyer_id      = str(mrow.get('buyer_org_id')    or '') if hasattr(mrow, 'get') else ''
                supplier_id   = str(mrow.get('supplier_org_id') or '') if hasattr(mrow, 'get') else ''
                buyer_name    = str(mrow.get('buyer_name')       or '') if hasattr(mrow, 'get') else ''
                sup_name      = str(mrow.get('supplier_name')    or '') if hasattr(mrow, 'get') else ''
                hs_code       = str(mrow.get('hs_code')          or '') if hasattr(mrow, 'get') else ''
                base_qty      = float(mrow.get('baseline_avg_qty') or 0) if hasattr(mrow, 'get') else 0.0
                last_ship_raw = str(mrow.get('last_shipment_date') or '') if hasattr(mrow, 'get') else ''

                # Derive days-since-last-shipment for data-driven stress reason override.
                # The IsolationForest trend_direction misses long gaps on thin data;
                # the date check is authoritative.
                days_silent = 9999
                if last_ship_raw:
                    try:
                        from datetime import datetime, timezone
                        for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
                            try:
                                dt = datetime.strptime(last_ship_raw[:len(fmt)], fmt)
                                break
                            except ValueError:
                                continue
                        else:
                            dt = datetime.fromisoformat(last_ship_raw.replace('Z', '+00:00'))
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        days_silent = (datetime.now(timezone.utc) - dt).days
                    except Exception:
                        pass

                if status == 'churned':
                    stress_reason = 'churned'
                elif days_silent > 180:          # 6+ months without a shipment → long gap
                    stress_reason = 'long_gap'
                elif trend == 'declining':
                    stress_reason = 'quantity_drop'
                elif trend == 'collapsed':
                    stress_reason = 'long_gap'
                else:
                    stress_reason = 'irregular'

                opp_batch.append({
                    'opportunity_id':          stable_id(buyer_id, supplier_id, hs_code, 'opp'),
                    'rel_id':                  rid,
                    'buyer_org_id':            buyer_id,
                    'buyer_name':              buyer_name,
                    'supplier_org_id':         supplier_id,
                    'supplier_name':           sup_name,
                    'hs_code':                 hs_code,
                    'stress_reason':           stress_reason,
                    'stress_score':            round(100 - score, 1),
                    'buyer_monthly_volume':    base_qty,
                    'detection_method':        'ml_based',
                    'first_detected_at':       now,
                })

        # Write health updates
        for i in range(0, len(update_batch), self.batch):
            chunk = update_batch[i:i + self.batch]
            self.neo.run("""
                UNWIND $batch AS row
                MATCH (tr:TradeRelationship {rel_id: row.rel_id})
                WHERE coalesce(tr.score_override, false) = false
                SET tr.health_score    = row.health_score,
                    tr.health_status   = row.health_status,
                    tr.anomaly_score   = row.anomaly_score,
                    tr.trend_direction = row.trend_direction,
                    tr.updated_at      = row.updated_at
                RETURN count(tr) AS c
            """, {'batch': chunk})
            info(f'SwitchLeadEngine.detect_stress: updated {i + len(chunk)} relationships')

        # Write opportunities
        total_opps = 0
        for i in range(0, len(opp_batch), self.batch):
            chunk = opp_batch[i:i + self.batch]
            self.neo.run("""
                UNWIND $batch AS row
                MERGE (opp:SupplierSwitchOpportunity {opportunity_id: row.opportunity_id})
                SET opp.buyer_org_id              = row.buyer_org_id,
                    opp.buyer_name                = row.buyer_name,
                    opp.hs_code                   = row.hs_code,
                    opp.existing_supplier_org_id  = row.supplier_org_id,
                    opp.existing_supplier_name    = row.supplier_name,
                    opp.stress_reason             = row.stress_reason,
                    opp.stress_score              = row.stress_score,
                    opp.switch_probability        = null,
                    opp.buyer_monthly_volume      = row.buyer_monthly_volume,
                    opp.detection_method          = row.detection_method,
                    opp.status                    = 'open',
                    opp.first_detected_at         = row.first_detected_at
                WITH opp, row
                MATCH (tr:TradeRelationship {rel_id: row.rel_id})
                MERGE (tr)-[:TRIGGERED]->(opp)
                RETURN count(opp) AS c
            """, {'batch': chunk})
            total_opps += len(chunk)
            info(f'SwitchLeadEngine.detect_stress: created {total_opps} opportunities')

        self._log('switch_opportunities', total_opps)
        ok(f'SwitchLeadEngine Stage 3 complete: {total_opps} opportunities detected')
        return total_opps

    # ── Stage 4 ────────────────────────────────────────────────────────────────

    def score_switch_probability(self) -> int:
        banner('SwitchLeadEngine Stage 4: Score Switch Probability')
        import pandas as pd

        threshold = float(self.settings.models.switch_prob_threshold)

        opp_rows = self.neo.run("""
            MATCH (opp:SupplierSwitchOpportunity {status: 'open'})
            WHERE opp.switch_probability IS NULL
            RETURN opp.opportunity_id          AS opportunity_id,
                   opp.buyer_org_id            AS buyer_org_id,
                   opp.existing_supplier_org_id AS supplier_org_id,
                   opp.hs_code                 AS hs_code,
                   opp.stress_score            AS stress_score
        """)

        if not opp_rows:
            info('SwitchLeadEngine.score_switch_probability: no open opportunities to score')
            return 0

        from app.features.feature_extractor import FeatureExtractor
        fe   = FeatureExtractor(self.neo, self.settings)
        feat = fe.extract_relationship_features()

        # Build lookup: (buyer_org_id, supplier_org_id, hs_code) -> features
        if feat.empty:
            classifier = None
        else:
            classifier = self.registry.load_switch_classifier() if self.registry.models_exist() else None
            feat_idx   = feat.set_index(['buyer_org_id', 'supplier_org_id', 'hs_code'])

        now         = utc_now()
        scored      = 0
        low_prob    = 0
        update_batch = []
        filter_batch = []

        for opp in opp_rows:
            oid          = opp['opportunity_id']
            buyer_id     = str(opp.get('buyer_org_id')     or '')
            supplier_id  = str(opp.get('supplier_org_id')  or '')
            hs_code      = str(opp.get('hs_code')          or '')
            stress_score = float(opp.get('stress_score')   or 0)

            # Try ML model first, else rule-based fallback
            switch_prob = None
            if classifier is not None and not feat.empty:
                key = (buyer_id, supplier_id, hs_code)
                if key in feat_idx.index:
                    row_feat = feat_idx.loc[[key]]
                    proba    = classifier.predict_proba(row_feat)
                    switch_prob = float(proba[0])

            if switch_prob is None:
                # Rule-based fallback: stress_score / 100 * 0.8
                switch_prob = min(0.95, (stress_score / 100) * 0.8)

            if switch_prob >= threshold:
                update_batch.append({
                    'opportunity_id':   oid,
                    'switch_probability': round(switch_prob, 4),
                    'updated_at':       now,
                })
                scored += 1
            else:
                filter_batch.append({'opportunity_id': oid})
                low_prob += 1

        # Write scored probabilities
        for i in range(0, len(update_batch), self.batch):
            chunk = update_batch[i:i + self.batch]
            self.neo.run("""
                UNWIND $batch AS row
                MATCH (opp:SupplierSwitchOpportunity {opportunity_id: row.opportunity_id})
                SET opp.switch_probability = row.switch_probability,
                    opp.updated_at         = row.updated_at
                RETURN count(opp) AS c
            """, {'batch': chunk})

        # Mark low-probability as filtered
        for i in range(0, len(filter_batch), self.batch):
            chunk = filter_batch[i:i + self.batch]
            self.neo.run("""
                UNWIND $batch AS row
                MATCH (opp:SupplierSwitchOpportunity {opportunity_id: row.opportunity_id})
                SET opp.status = 'filtered_low_probability'
                RETURN count(opp) AS c
            """, {'batch': chunk})

        self._log('opportunities_scored', scored)
        ok(f'SwitchLeadEngine Stage 4 complete: {scored} scored, {low_prob} filtered (< threshold)')
        return scored

    # ── Stage 5 ────────────────────────────────────────────────────────────────

    def match_suppliers(self) -> int:
        banner('SwitchLeadEngine Stage 5: Match Alternative Suppliers')
        import pandas as pd

        threshold     = float(self.settings.models.switch_prob_threshold)
        match_min     = float(self.settings.models.match_score_threshold)

        opp_rows = self.neo.run("""
            MATCH (opp:SupplierSwitchOpportunity {status: 'open'})
            WHERE opp.switch_probability >= $threshold
            OPTIONAL MATCH (buyer:Organization {orgId: opp.buyer_org_id})
            RETURN opp.opportunity_id          AS opportunity_id,
                   opp.buyer_org_id            AS buyer_org_id,
                   opp.hs_code                 AS hs_code,
                   opp.existing_supplier_org_id AS existing_supplier_id,
                   opp.buyer_monthly_volume    AS buyer_monthly_volume,
                   opp.switch_probability      AS switch_probability,
                   coalesce(buyer.zi_country, buyer.country, '') AS buyer_country
        """, {'threshold': threshold})

        if not opp_rows:
            info('SwitchLeadEngine.match_suppliers: no opportunities ready for matching')
            return 0

        from app.features.feature_extractor import FeatureExtractor
        ranker = self.registry.load_supplier_ranker()
        fe     = FeatureExtractor(self.neo, self.settings)

        now          = utc_now()
        total_matches = 0
        match_batch  = []

        for opp in opp_rows:
            oid             = opp['opportunity_id']
            hs_code         = str(opp.get('hs_code')                or '')
            existing_sup    = str(opp.get('existing_supplier_id')   or '')
            buyer_vol       = float(opp.get('buyer_monthly_volume') or 0)
            buyer_country   = str(opp.get('buyer_country')          or '')

            candidates = fe.extract_supplier_candidates(hs_code, existing_sup, buyer_country or None)
            if candidates.empty:
                continue

            # Add buyer_volume_ratio feature
            candidates['buyer_volume_ratio'] = (
                buyer_vol / candidates['recent_export_volume'].replace(0, 1)
            ).clip(0, 10)

            raw_scores = ranker.rank(candidates)
            scores_100 = ranker.score_to_100(raw_scores)

            # Keep top 5
            top_idx = scores_100.argsort()[::-1][:5]
            for rank, idx in enumerate(top_idx, start=1):
                row       = candidates.iloc[idx]
                mscore    = float(scores_100[idx])
                if mscore < match_min and rank > 1:
                    continue

                sup_id   = str(row.get('supplier_org_id') or '')
                sup_name = str(row.get('supplier_name')   or '')

                match_batch.append({
                    'match_id':                  stable_id(oid, sup_id),
                    'opportunity_id':            oid,
                    'candidate_supplier_org_id': sup_id,
                    'candidate_supplier_name':   sup_name,
                    'hs_code_match_type':        'exact' if row.get('hs_exact_match') else 'chapter',
                    'match_score':               round(mscore, 1),
                    'rank':                      rank,
                    'active_buyer_count':        int(row.get('active_buyer_count') or 0),
                    'exports_to_buyer_country':  bool(row.get('exports_to_buyer_country')),
                    'has_zi_contact':            bool(row.get('has_zi_contact')),
                    'created_at':                now,
                })

            if len(match_batch) >= self.batch:
                self._flush_matches(match_batch)
                total_matches += len(match_batch)
                match_batch = []

        if match_batch:
            self._flush_matches(match_batch)
            total_matches += len(match_batch)

        self._log('supplier_matches', total_matches)
        ok(f'SwitchLeadEngine Stage 5 complete: {total_matches} supplier matches created')
        return total_matches

    def _flush_matches(self, batch: list):
        self.neo.run("""
            UNWIND $batch AS row
            MERGE (sm:SupplierMatch {match_id: row.match_id})
            SET sm.opportunity_id              = row.opportunity_id,
                sm.candidate_supplier_org_id   = row.candidate_supplier_org_id,
                sm.candidate_supplier_name     = row.candidate_supplier_name,
                sm.hs_code_match_type          = row.hs_code_match_type,
                sm.match_score                 = row.match_score,
                sm.rank                        = row.rank,
                sm.active_buyer_count          = row.active_buyer_count,
                sm.exports_to_buyer_country    = row.exports_to_buyer_country,
                sm.has_zi_contact              = row.has_zi_contact,
                sm.created_at                  = row.created_at
            WITH sm, row
            MATCH (opp:SupplierSwitchOpportunity {opportunity_id: row.opportunity_id})
            MERGE (opp)-[:HAS_MATCH]->(sm)
            WITH sm, row
            OPTIONAL MATCH (org:Organization {orgId: row.candidate_supplier_org_id})
            FOREACH (_ IN CASE WHEN org IS NOT NULL THEN [1] ELSE [] END |
                MERGE (sm)-[:CANDIDATE_SUPPLIER]->(org)
            )
            RETURN count(sm) AS c
        """, {'batch': batch})
        info(f'SwitchLeadEngine: flushed {len(batch)} SupplierMatch nodes')

    # ── Stage 6 ────────────────────────────────────────────────────────────────

    def create_switch_leads(self) -> int:
        banner('SwitchLeadEngine Stage 6: Create Switch Leads')

        prob_threshold  = float(self.settings.models.switch_prob_threshold)
        match_threshold = float(self.settings.models.match_score_threshold)

        result_rows = self.neo.run("""
            MATCH (opp:SupplierSwitchOpportunity {status: 'open'})-[:HAS_MATCH]->(sm:SupplierMatch)
            WHERE sm.match_score >= $match_threshold
              AND opp.switch_probability >= $prob_threshold
            OPTIONAL MATCH (buyer:Organization {orgId: opp.buyer_org_id})
            OPTIONAL MATCH (contact:Person)-[:CONTACT_AT]->(buyer)
            WHERE contact.is_decision_maker = true
            WITH opp, sm, buyer, collect(contact)[0] AS top_contact
            RETURN opp.opportunity_id          AS opportunity_id,
                   opp.buyer_org_id            AS buyer_org_id,
                   opp.buyer_name              AS buyer_name,
                   opp.hs_code                 AS hs_code,
                   opp.stress_reason           AS stress_reason,
                   opp.stress_score            AS stress_score,
                   opp.switch_probability      AS switch_probability,
                   opp.buyer_monthly_volume    AS buyer_monthly_volume,
                   opp.existing_supplier_name  AS existing_supplier_name,
                   sm.match_id                 AS match_id,
                   sm.candidate_supplier_org_id AS candidate_supplier_org_id,
                   sm.candidate_supplier_name  AS candidate_supplier_name,
                   sm.match_score              AS match_score,
                   sm.rank                     AS rank,
                   sm.exports_to_buyer_country AS exports_to_country,
                   sm.active_buyer_count       AS active_buyer_count,
                   sm.has_zi_contact           AS has_zi_contact,
                   coalesce(buyer.zi_country, buyer.country, '') AS buyer_country,
                   coalesce(buyer.zi_industry, '') AS buyer_industry,
                   top_contact.name            AS contact_name,
                   top_contact.title           AS contact_title,
                   top_contact.email           AS contact_email
        """, {'match_threshold': match_threshold, 'prob_threshold': prob_threshold})

        # ── Also fetch dormant/churned opportunities with NO supplier match yet ──
        # These are HIGH urgency (health_score < 40) but lack a pre-identified
        # alternative supplier. We surface them as "sourcing needed" leads so
        # sellers know to find an alternative themselves.
        unmatched_rows = self.neo.run("""
            MATCH (opp:SupplierSwitchOpportunity {status: 'open'})
            WHERE opp.switch_probability >= $prob_threshold
              AND NOT (opp)-[:HAS_MATCH]->(:SupplierMatch)
            OPTIONAL MATCH (tr:TradeRelationship)-[:TRIGGERED]->(opp)
            WHERE tr.health_status IN ['dormant', 'churned']
            WITH opp, tr
            WHERE tr IS NOT NULL
            OPTIONAL MATCH (buyer:Organization {orgId: opp.buyer_org_id})
            OPTIONAL MATCH (contact:Person)-[:CONTACT_AT]->(buyer)
            WHERE contact.is_decision_maker = true
            WITH opp, tr, buyer, collect(contact)[0] AS top_contact
            RETURN opp.opportunity_id         AS opportunity_id,
                   opp.buyer_org_id           AS buyer_org_id,
                   opp.buyer_name             AS buyer_name,
                   opp.hs_code                AS hs_code,
                   opp.stress_reason          AS stress_reason,
                   opp.stress_score           AS stress_score,
                   opp.switch_probability     AS switch_probability,
                   opp.buyer_monthly_volume   AS buyer_monthly_volume,
                   opp.existing_supplier_org_id AS existing_supplier_org_id,
                   opp.existing_supplier_name AS existing_supplier_name,
                   tr.health_score            AS health_score,
                   tr.health_status           AS health_status,
                   coalesce(buyer.zi_country, buyer.country, '') AS buyer_country,
                   coalesce(buyer.zi_industry, '') AS buyer_industry,
                   top_contact.name           AS contact_name,
                   top_contact.title          AS contact_title,
                   top_contact.email          AS contact_email
        """, {'prob_threshold': prob_threshold})

        if not result_rows and not unmatched_rows:
            info('SwitchLeadEngine.create_switch_leads: no leads to create')
            return 0

        now        = utc_now()
        lead_batch = []

        for row in result_rows:
            switch_prob  = float(row.get('switch_probability') or 0)
            match_score  = float(row.get('match_score')        or 0)
            buyer_vol    = float(row.get('buyer_monthly_volume') or 0)

            final_score  = (switch_prob * 40) + (match_score * 0.35) + min(buyer_vol / 10000, 25)
            final_score  = round(min(100.0, final_score), 1)

            lead_priority = (
                'critical' if final_score >= 80 else
                'high'     if final_score >= 65 else
                'medium'   if final_score >= 50 else
                'low'
            )

            buyer_name      = str(row.get('buyer_name')             or 'Unknown Buyer')
            hs_code         = str(row.get('hs_code')                or '')
            hs_display      = _clean_hs(hs_code)
            stress_reason   = str(row.get('stress_reason')          or 'stress_detected')
            existing_sup    = str(row.get('existing_supplier_name') or 'current supplier')
            contact_name    = str(row.get('contact_name')           or '')
            contact_title   = str(row.get('contact_title')          or '')
            contact_email   = str(row.get('contact_email')          or '')
            active_buyers   = int(row.get('active_buyer_count')     or 0)
            to_country      = bool(row.get('exports_to_country'))

            vol_str = f'~{int(buyer_vol):,} units/month' if buyer_vol > 0 else 'unknown volume'
            contact_str = (
                f'{contact_name} ({contact_title})' if contact_name else
                'find via ZoomInfo'
            )
            country_note = ' Already ships to buyer country.' if to_country else ''
            active_note  = f' {active_buyers} active buyers.' if active_buyers > 0 else ''

            recommended_action = (
                f"Buyer: {buyer_name} imports {hs_display} ({vol_str}) from {existing_sup}. "
                f"Relationship showing {stress_reason.replace('_', ' ')}. "
                f"Switch probability: {switch_prob:.0%}. "
                f"Contact: {contact_str}.{country_note}{active_note}"
            )

            lead_batch.append({
                'lead_id':                   stable_id(row['match_id'], 'switch_lead'),
                'opportunity_id':            row['opportunity_id'],
                'match_id':                  row['match_id'],
                'buyer_org_id':              str(row.get('buyer_org_id')                or ''),
                'buyer_name':                buyer_name,
                'candidate_supplier_org_id': str(row.get('candidate_supplier_org_id')   or ''),
                'candidate_supplier_name':   str(row.get('candidate_supplier_name')     or ''),
                'hs_code':                   hs_code,
                'stress_reason':             stress_reason,
                'switch_probability':        switch_prob,
                'match_score':               match_score,
                'final_lead_score':          final_score,
                'lead_priority':             lead_priority,
                'buyer_monthly_volume':      buyer_vol,
                'recommended_action':        recommended_action,
                'contact_name':              contact_name,
                'contact_title':             contact_title,
                'contact_email':             contact_email,
                'buyer_country':             str(row.get('buyer_country')   or ''),
                'buyer_industry':            str(row.get('buyer_industry')  or ''),
                'status':                    'new',
                'source':                    'switch_lead_engine_v1',
                'created_at':               now,
            })

        # ── Build unmatched (sourcing-needed) leads from dormant/churned opps ──
        for row in unmatched_rows:
            switch_prob  = float(row.get('switch_probability') or 0)
            buyer_vol    = float(row.get('buyer_monthly_volume') or 0)
            health_score = float(row.get('health_score') or 0)

            # Dormant/churned with no known alternative → high priority based on health.
            # Score includes health-collapse component; priority is floored at 'high'
            # because any dormant relationship with a switch probability over threshold
            # is already urgent regardless of composite score.
            final_score = (switch_prob * 40) + min(buyer_vol / 10000, 25) + max(0, (100 - health_score) * 0.35)
            final_score = round(min(100.0, final_score), 1)

            lead_priority = (
                'critical' if final_score >= 80 or health_score <= 10 else
                'high'     # dormant/churned relationships are always at least 'high'
            )

            buyer_name    = str(row.get('buyer_name')             or 'Unknown Buyer')
            hs_code       = str(row.get('hs_code')                or '')
            hs_display    = _clean_hs(hs_code)
            stress_reason = str(row.get('stress_reason')          or 'stress_detected')
            existing_sup  = str(row.get('existing_supplier_name') or 'current supplier')
            contact_name  = str(row.get('contact_name')           or '')
            contact_title = str(row.get('contact_title')          or '')
            contact_email = str(row.get('contact_email')          or '')
            health_status = str(row.get('health_status')          or 'dormant')
            vol_str       = f'~{int(buyer_vol):,} units/month' if buyer_vol > 0 else 'unknown volume'

            recommended_action = (
                f"URGENT - Relationship {health_status.upper()}: "
                f"{buyer_name} imports {hs_display} ({vol_str}) from {existing_sup}. "
                f"Relationship showing {stress_reason.replace('_', ' ')} (health score: {health_score:.0f}/100). "
                f"No alternative supplier matched yet — source alternative via market intelligence. "
                f"Switch probability: {switch_prob:.0%}."
            )

            lead_batch.append({
                'lead_id':                   stable_id(row['opportunity_id'], 'unmatched_lead'),
                'opportunity_id':            row['opportunity_id'],
                'match_id':                  '',
                'buyer_org_id':              str(row.get('buyer_org_id')             or ''),
                'buyer_name':                buyer_name,
                'candidate_supplier_org_id': '',
                'candidate_supplier_name':   '[Sourcing Needed]',
                'hs_code':                   hs_code,
                'stress_reason':             stress_reason,
                'switch_probability':        switch_prob,
                'match_score':               0.0,
                'final_lead_score':          final_score,
                'lead_priority':             lead_priority,
                'buyer_monthly_volume':      buyer_vol,
                'recommended_action':        recommended_action,
                'contact_name':              contact_name,
                'contact_title':             contact_title,
                'contact_email':             contact_email,
                'buyer_country':             str(row.get('buyer_country')   or ''),
                'buyer_industry':            str(row.get('buyer_industry')  or ''),
                'status':                    'sourcing_needed',
                'source':                    'switch_lead_engine_v1',
                'created_at':                now,
            })

        # ── Outbox + Kafka producer (lazy init — skipped if kafka.enabled=false) ─
        from app.kafka.outbox_writer import OutboxWriter
        from app.kafka.producer import KafkaLeadProducer
        outbox   = OutboxWriter(self.pg)
        producer = KafkaLeadProducer(self.settings)

        total_leads = 0
        for i in range(0, len(lead_batch), self.batch):
            chunk = lead_batch[i:i + self.batch]

            # Step A — write to Neo4j (source of truth)
            self.neo.run("""
                UNWIND $batch AS row
                MERGE (sl:SwitchLead {lead_id: row.lead_id})
                SET sl.opportunity_id            = row.opportunity_id,
                    sl.match_id                  = row.match_id,
                    sl.buyer_org_id              = row.buyer_org_id,
                    sl.buyer_name                = row.buyer_name,
                    sl.candidate_supplier_org_id = row.candidate_supplier_org_id,
                    sl.candidate_supplier_name   = row.candidate_supplier_name,
                    sl.hs_code                   = row.hs_code,
                    sl.stress_reason             = row.stress_reason,
                    sl.switch_probability        = row.switch_probability,
                    sl.match_score               = row.match_score,
                    sl.final_lead_score          = row.final_lead_score,
                    sl.lead_priority             = row.lead_priority,
                    sl.buyer_monthly_volume      = row.buyer_monthly_volume,
                    sl.recommended_action        = row.recommended_action,
                    sl.contact_name              = row.contact_name,
                    sl.contact_title             = row.contact_title,
                    sl.contact_email             = row.contact_email,
                    sl.buyer_country             = row.buyer_country,
                    sl.buyer_industry            = row.buyer_industry,
                    sl.status                    = row.status,
                    sl.source                    = row.source,
                    sl.created_at                = row.created_at
                WITH sl, row
                OPTIONAL MATCH (sm:SupplierMatch {match_id: row.match_id})
                FOREACH (_ IN CASE WHEN sm IS NOT NULL THEN [1] ELSE [] END |
                    MERGE (sm)-[:GENERATES]->(sl)
                )
                WITH sl, row
                OPTIONAL MATCH (buyer:Organization {orgId: row.buyer_org_id})
                FOREACH (_ IN CASE WHEN buyer IS NOT NULL THEN [1] ELSE [] END |
                    MERGE (sl)-[:TARGETS_BUYER]->(buyer)
                )
                RETURN count(sl) AS c
            """, {'batch': chunk})

            # Step B — write same events to outbox (Debezium → Kafka → MySQL crm2)
            # This runs immediately after the Neo4j write in the same Python call.
            # If Kafka is disabled, outbox writes are no-ops (table won't exist).
            try:
                outbox.write_switch_leads_batch(chunk)
            except Exception as e:
                warn(f'SwitchLeadEngine Stage 6: outbox write failed (non-fatal): {e}')

            # Step C — also publish directly to Kafka for low-latency consumers
            # The outbox is the reliable path; direct publish is best-effort.
            try:
                producer.publish_switch_leads_batch(chunk)
            except Exception as e:
                warn(f'SwitchLeadEngine Stage 6: Kafka publish failed (non-fatal): {e}')

            total_leads += len(chunk)
            info(f'SwitchLeadEngine: created {total_leads} SwitchLead nodes')

        producer.close()
        self._log('switch_leads_created', total_leads)
        ok(f'SwitchLeadEngine Stage 6 complete: {total_leads} leads created')
        return total_leads

    # ── Orchestrator ───────────────────────────────────────────────────────────


    def _publish_leads(self, lead_batch: list):
        """Write SwitchLeads to Neo4j + outbox + Kafka. Used by streaming path."""
        from app.kafka.outbox_writer import OutboxWriter
        from app.kafka.producer import KafkaLeadProducer
        outbox   = OutboxWriter(self.pg)
        producer = KafkaLeadProducer(self.settings)
        self.neo.run("""
            UNWIND $batch AS row
            MERGE (sl:SwitchLead {lead_id: row.lead_id})
            SET sl.opportunity_id            = row.opportunity_id,
                sl.match_id                  = row.match_id,
                sl.buyer_org_id              = row.buyer_org_id,
                sl.buyer_name                = row.buyer_name,
                sl.candidate_supplier_org_id = row.candidate_supplier_org_id,
                sl.candidate_supplier_name   = row.candidate_supplier_name,
                sl.hs_code                   = row.hs_code,
                sl.stress_reason             = row.stress_reason,
                sl.switch_probability        = row.switch_probability,
                sl.match_score               = row.match_score,
                sl.final_lead_score          = row.final_lead_score,
                sl.lead_priority             = row.lead_priority,
                sl.buyer_monthly_volume      = row.buyer_monthly_volume,
                sl.recommended_action        = row.recommended_action,
                sl.contact_name              = row.contact_name,
                sl.contact_title             = row.contact_title,
                sl.contact_email             = row.contact_email,
                sl.buyer_country             = row.buyer_country,
                sl.buyer_industry            = row.buyer_industry,
                sl.status                    = row.status,
                sl.source                    = row.source,
                sl.created_at               = row.created_at
            WITH sl, row
            OPTIONAL MATCH (sm:SupplierMatch {match_id: row.match_id})
            FOREACH (_ IN CASE WHEN sm IS NOT NULL THEN [1] ELSE [] END |
                MERGE (sm)-[:GENERATES]->(sl)
            )
            WITH sl, row
            OPTIONAL MATCH (buyer:Organization {orgId: row.buyer_org_id})
            FOREACH (_ IN CASE WHEN buyer IS NOT NULL THEN [1] ELSE [] END |
                MERGE (sl)-[:TARGETS_BUYER]->(buyer)
            )
            RETURN count(sl) AS c
        """, {"batch": lead_batch})
        try:
            outbox.write_switch_leads_batch(lead_batch)
        except Exception as e:
            warn(f"_publish_leads: outbox write failed (non-fatal): {e}")
        try:
            producer.publish_switch_leads_batch(lead_batch)
            producer.close()
        except Exception as e:
            warn(f"_publish_leads: Kafka publish failed (non-fatal): {e}")

    def detect_stress_for_rel(self, rel_id: str) -> bool:
        """
        Streaming switch-lead detection for a single TradeRelationship.
        Called by KafkaEventConsumer.handle_shipment() after process_single_shipment().

        Once models are trained, uses the full ML pipeline:
          - health_detector.pkl  → IsolationForest on snapshot DataFrame (1+ rows)
          - switch_classifier.pkl → predict_proba on extracted features
          - supplier_ranker.pkl   → rank alternative suppliers

        Falls back to rule-based scoring on each model that is not yet trained.
        Returns True if a SwitchLead was created or updated.
        """
        import pandas as pd
        from datetime import datetime, timezone

        # ── Fetch relationship + all snapshots ────────────────────────────────
        rows = self.neo.run("""
            MATCH (tr:TradeRelationship {rel_id: $rel_id})
            OPTIONAL MATCH (tr)-[:HAS_SNAPSHOT]->(snap:RelationshipSnapshot)
            RETURN tr.rel_id                   AS rel_id,
                   tr.last_shipment_date       AS last_shipment_date,
                   tr.baseline_avg_monthly_qty AS baseline_avg_qty,
                   tr.buyer_org_id             AS buyer_org_id,
                   tr.supplier_org_id          AS supplier_org_id,
                   tr.hs_code                  AS hs_code,
                   tr.buyer_name               AS buyer_name,
                   tr.supplier_name            AS supplier_name,
                   tr.buyer_monthly_volume     AS buyer_monthly_volume,
                   collect({
                       year_month:       snap.year_month,
                       shipment_count:   snap.shipment_count,
                       total_quantity:   snap.total_quantity,
                       total_value:      snap.total_value,
                       qty_vs_baseline:  snap.qty_vs_baseline_pct
                   }) AS snapshots
        """, {"rel_id": rel_id})

        if not rows:
            warn(f"detect_stress_for_rel: rel {rel_id} not found")
            return False

        row       = rows[0]
        now_dt    = datetime.now(timezone.utc)
        now_str   = utc_now()
        last_raw  = str(row.get("last_shipment_date") or "")
        baseline  = float(row.get("baseline_avg_qty") or 0)
        snapshots = [s for s in (row.get("snapshots") or []) if s.get("year_month")]
        buyer_id    = str(row.get("buyer_org_id")    or "")
        supplier_id = str(row.get("supplier_org_id") or "")
        hs_code     = str(row.get("hs_code")         or "")
        buyer_name  = str(row.get("buyer_name")      or "")
        sup_name    = str(row.get("supplier_name")   or "")
        buyer_vol   = float(row.get("buyer_monthly_volume") or baseline)

        # ── days since last shipment (used by both rule and ML paths) ─────────
        days_silent = 0
        if last_raw:
            try:
                for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                    try:
                        dt = datetime.strptime(last_raw[:len(fmt)], fmt); break
                    except ValueError:
                        continue
                else:
                    dt = datetime.fromisoformat(last_raw.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                days_silent = (now_dt - dt).days
            except Exception:
                pass

        # ══ STAGE A: health / stress detection ═══════════════════════════════
        #
        # Try ML model (health_detector.pkl) first.
        # Build the same DataFrame format used by detect_stress() batch method.
        # IsolationForest inference works on any number of rows — 1 is fine.
        #
        health_status   = None
        health_score    = None
        anomaly_score   = None
        trend_direction = None
        detection_method = "rule_based"

        if snapshots:
            try:
                detector = self.registry.load_health_detector()
                df_rows  = []
                for snap in snapshots:
                    df_rows.append({
                        "rel_id":             rel_id,
                        "buyer_org_id":       buyer_id,
                        "supplier_org_id":    supplier_id,
                        "hs_code":            hs_code,
                        "buyer_name":         buyer_name,
                        "supplier_name":      sup_name,
                        "baseline_avg_qty":   baseline,
                        "last_shipment_date": last_raw,
                        "year_month":         str(snap.get("year_month") or ""),
                        "total_quantity":     float(snap.get("total_quantity") or 0),
                        "total_value":        float(snap.get("total_value")    or 0),
                        "shipment_count":     int(snap.get("shipment_count")   or 0),
                        "qty_vs_baseline_pct": float(snap.get("qty_vs_baseline") or 0),
                    })
                df = pd.DataFrame(df_rows)
                df["total_quantity"] = pd.to_numeric(df["total_quantity"], errors="coerce").fillna(0)
                health_df = detector.predict(df)
                if not health_df.empty:
                    best = health_df.iloc[0]   # single rel → take first (only) result
                    health_status   = best["health_status"]
                    health_score    = float(best["health_score"])
                    anomaly_score   = float(best["anomaly_score"])
                    trend_direction = best["trend_direction"]
                    detection_method = "ml_based"
                    info(f"detect_stress_for_rel: ML health={health_status} score={health_score:.1f}")
            except Exception as e:
                warn(f"detect_stress_for_rel: ML health detection failed, using rules: {e}")

        # Rule-based fallback (or cross-check) ─────────────────────────────────
        has_history  = len(snapshots) > 0
        recent_snaps = sorted(snapshots, key=lambda s: s.get("year_month", ""), reverse=True)[:2]
        recent_qty   = sum(float(s.get("total_quantity") or 0) for s in recent_snaps)
        recent_count = sum(int(s.get("shipment_count")   or 0) for s in recent_snaps)

        vol_drop_pct = 0.0
        if baseline > 0 and recent_snaps:
            vol_drop_pct = max(0.0, (baseline - recent_qty / len(recent_snaps)) / baseline)

        all_counts    = [int(s.get("shipment_count") or 0) for s in snapshots]
        baseline_freq = (sum(all_counts) / len(all_counts)) if all_counts else 0
        recent_freq   = (recent_count / len(recent_snaps)) if recent_snaps else 0
        freq_halved   = baseline_freq > 0 and recent_freq < (baseline_freq * 0.5)

        if has_history and days_silent > 180:
            rule_status = "churned";   rule_score = 95; rule_reason = "no_shipment_6_months"
        elif days_silent > 90 or (has_history and vol_drop_pct >= 0.70):
            rule_status = "dormant";   rule_score = 80
            rule_reason = "no_shipment_3_months" if days_silent > 90 else "volume_collapsed_70pct"
        elif vol_drop_pct >= 0.30 or (45 <= days_silent <= 90):
            rule_status = "stressed";  rule_score = 60
            rule_reason = (f"volume_drop_{int(vol_drop_pct*100)}pct"
                           if vol_drop_pct >= 0.30 else "shipment_gap_45_90d")
        elif freq_halved:
            rule_status = "irregular"; rule_score = 35; rule_reason = "shipment_frequency_halved"
        else:
            rule_status = "healthy";   rule_score = 10; rule_reason = "no_stress_signals"

        # Use ML result if available, otherwise use rules
        if health_status is None:
            health_status = rule_status
            detection_method = "rule_based"

        # Derive stress_reason from ML trend or fall back to rule reason
        if detection_method == "ml_based":
            if health_status == "churned":
                stress_reason = "churned"
            elif days_silent > 180:
                stress_reason = "long_gap"
            elif trend_direction == "declining":
                stress_reason = "quantity_drop"
            elif trend_direction == "collapsed":
                stress_reason = "long_gap"
            else:
                stress_reason = rule_reason   # rule reason as tiebreak
            ml_stress_score = round(100 - health_score, 1)
        else:
            stress_reason   = rule_reason
            ml_stress_score = rule_score

        # Write health scores to TradeRelationship
        neo_set = {
            "rel_id": rel_id, "status": rule_status,
            "rule_score": rule_score, "reason": rule_reason,
            "days": days_silent, "vol_drop": round(vol_drop_pct * 100, 1),
            "now": now_str,
        }
        if detection_method == "ml_based":
            self.neo.run("""
                MATCH (tr:TradeRelationship {rel_id: $rel_id})
                SET tr.health_status    = $ml_status,
                    tr.health_score     = $ml_score,
                    tr.anomaly_score    = $anomaly,
                    tr.trend_direction  = $trend,
                    tr.rule_health_status = $status,
                    tr.rule_stress_score  = $rule_score,
                    tr.rule_stress_reason = $reason,
                    tr.rule_days_silent   = $days,
                    tr.rule_vol_drop_pct  = $vol_drop,
                    tr.rule_detected_at   = $now
            """, {**neo_set,
                  "ml_status": health_status,
                  "ml_score": health_score,
                  "anomaly": anomaly_score,
                  "trend": trend_direction})
        else:
            self.neo.run("""
                MATCH (tr:TradeRelationship {rel_id: $rel_id})
                SET tr.rule_health_status = $status,
                    tr.rule_stress_score  = $rule_score,
                    tr.rule_stress_reason = $reason,
                    tr.rule_days_silent   = $days,
                    tr.rule_vol_drop_pct  = $vol_drop,
                    tr.rule_detected_at   = $now
            """, neo_set)

        if health_status in ("healthy", "irregular"):
            info(f"detect_stress_for_rel: {rel_id} -> {health_status} ({detection_method}), no lead")
            return False

        # ══ STAGE B: switch probability scoring ══════════════════════════════
        opp_id      = stable_id(buyer_id, supplier_id, hs_code, "rule_opp")
        switch_prob = None

        try:
            classifier = self.registry.load_switch_classifier() if self.registry.models_exist() else None
            if classifier is not None:
                from app.features.feature_extractor import FeatureExtractor
                fe   = FeatureExtractor(self.neo, self.settings)
                feat = fe.extract_relationship_features()
                if not feat.empty:
                    feat_idx = feat.set_index(["buyer_org_id", "supplier_org_id", "hs_code"])
                    key      = (buyer_id, supplier_id, hs_code)
                    if key in feat_idx.index:
                        row_feat    = feat_idx.loc[[key]]
                        switch_prob = float(classifier.predict_proba(row_feat)[0])
                        info(f"detect_stress_for_rel: ML switch_prob={switch_prob:.3f}")
        except Exception as e:
            warn(f"detect_stress_for_rel: ML switch_prob failed, using rule fallback: {e}")

        if switch_prob is None:
            switch_prob = round(min(0.95, (ml_stress_score / 100) * 0.8), 4)

        # Upsert SupplierSwitchOpportunity
        self.neo.run("""
            MERGE (opp:SupplierSwitchOpportunity {opportunity_id: $opp_id})
            SET opp.buyer_org_id             = $buyer_id,
                opp.buyer_name               = $buyer_name,
                opp.hs_code                  = $hs_code,
                opp.existing_supplier_org_id = $supplier_id,
                opp.existing_supplier_name   = $sup_name,
                opp.stress_reason            = $stress_reason,
                opp.stress_score             = $stress_score,
                opp.switch_probability       = $switch_prob,
                opp.buyer_monthly_volume     = $buyer_vol,
                opp.detection_method         = $det_method,
                opp.status                   = 'open',
                opp.first_detected_at        = $now
            WITH opp
            MATCH (tr:TradeRelationship {rel_id: $rel_id})
            MERGE (tr)-[:TRIGGERED]->(opp)
        """, {
            "opp_id": opp_id, "rel_id": rel_id,
            "buyer_id": buyer_id, "buyer_name": buyer_name,
            "supplier_id": supplier_id, "sup_name": sup_name,
            "hs_code": hs_code, "stress_reason": stress_reason,
            "stress_score": ml_stress_score, "switch_prob": round(switch_prob, 4),
            "buyer_vol": buyer_vol, "det_method": detection_method, "now": now_str,
        })

        threshold = float(self.settings.models.switch_prob_threshold)
        if switch_prob < threshold:
            info(f"detect_stress_for_rel: {rel_id} prob {switch_prob:.3f} < threshold — opp saved, no lead yet")
            return False

        # ══ STAGE C: supplier matching ════════════════════════════════════════
        buyer_country = ""
        buyer_rows = self.neo.run(
            "MATCH (o:Organization {orgId: $id}) RETURN coalesce(o.zi_country, o.country, '') AS c",
            {"id": buyer_id}
        )
        if buyer_rows:
            buyer_country = str(buyer_rows[0].get("c") or "")

        hs_display = _clean_hs(hs_code)
        match_min  = float(self.settings.models.match_score_threshold)

        # Try ranker first, fall back to plain Neo4j
        candidates_raw = None
        match_score_default = 60.0
        try:
            ranker = self.registry.load_supplier_ranker()
            from app.features.feature_extractor import FeatureExtractor
            fe         = FeatureExtractor(self.neo, self.settings)
            cand_df    = fe.extract_supplier_candidates(hs_code, supplier_id, buyer_country or None)
            if not cand_df.empty:
                cand_df["buyer_volume_ratio"] = (
                    buyer_vol / cand_df["recent_export_volume"].replace(0, 1)
                ).clip(0, 10)
                raw_scores  = ranker.rank(cand_df)
                scores_100  = ranker.score_to_100(raw_scores)
                top_idx     = scores_100.argsort()[::-1][:5]
                candidates_raw = []
                for rank, idx in enumerate(top_idx, start=1):
                    r      = cand_df.iloc[idx]
                    mscore = float(scores_100[idx])
                    if mscore < match_min and rank > 1:
                        continue
                    candidates_raw.append({
                        "supplier_org_id":       str(r.get("supplier_org_id") or ""),
                        "supplier_name":         str(r.get("supplier_name")   or ""),
                        "match_score":           round(mscore, 1),
                        "rank":                  rank,
                        "active_buyer_count":    int(r.get("active_buyer_count") or 0),
                        "exports_to_buyer_country": bool(r.get("exports_to_buyer_country")),
                        "has_zi_contact":        bool(r.get("has_zi_contact")),
                    })
                if candidates_raw:
                    info(f"detect_stress_for_rel: ranker found {len(candidates_raw)} candidates")
        except Exception as e:
            warn(f"detect_stress_for_rel: ranker failed, using Neo4j fallback: {e}")

        if not candidates_raw:
            # Neo4j fallback: match by HS chapter
            hs_chapter = hs_code[:2]
            neo_cands = self.neo.run("""
                MATCH (org:Organization)
                WHERE org.orgId <> $supplier_id
                  AND (any(h IN coalesce(org.hs_codes_exported, []) WHERE toString(h) STARTS WITH $hs_chapter)
                       OR any(h IN coalesce(org.hs_codes_exported, []) WHERE toString(h) = $hs_code))
                RETURN org.orgId AS supplier_org_id,
                       coalesce(org.name, org.zi_name, org.orgId) AS supplier_name,
                       coalesce(org.active_buyer_count, 0)        AS active_buyer_count,
                       coalesce(org.zi_contact_count, 0) > 0      AS has_zi_contact
                ORDER BY org.active_buyer_count DESC
                LIMIT 5
            """, {"supplier_id": supplier_id, "hs_code": hs_code, "hs_chapter": hs_chapter})
            if neo_cands:
                candidates_raw = [{
                    "supplier_org_id":       str(c.get("supplier_org_id") or ""),
                    "supplier_name":         str(c.get("supplier_name")   or ""),
                    "match_score":           match_score_default,
                    "rank":                  i + 1,
                    "active_buyer_count":    int(c.get("active_buyer_count") or 0),
                    "exports_to_buyer_country": False,
                    "has_zi_contact":        bool(c.get("has_zi_contact")),
                } for i, c in enumerate(neo_cands)]

        if not candidates_raw:
            # No supplier found — sourcing-needed lead
            final_score   = round(min(100.0, (switch_prob * 40) + min(buyer_vol / 10000, 25) +
                                    max(0.0, (100 - ml_stress_score) * 0.35)), 1)
            lead_priority = "critical" if final_score >= 80 else "high"
            self._publish_leads([{
                "lead_id": stable_id(opp_id, "stream_unmatched"),
                "opportunity_id": opp_id, "match_id": "",
                "buyer_org_id": buyer_id, "buyer_name": buyer_name,
                "candidate_supplier_org_id": "", "candidate_supplier_name": "[Sourcing Needed]",
                "hs_code": hs_code, "stress_reason": stress_reason,
                "switch_probability": switch_prob, "match_score": 0.0,
                "final_lead_score": final_score, "lead_priority": lead_priority,
                "buyer_monthly_volume": buyer_vol,
                "recommended_action": (
                    f"URGENT - Relationship {health_status.upper()}: "
                    f"{buyer_name} imports {hs_display} from {sup_name}. "
                    f"Showing {stress_reason.replace('_', ' ')}. "
                    f"No alternative supplier matched - source via market intelligence. "
                    f"Switch probability: {switch_prob:.0%}."
                ),
                "contact_name": "", "contact_title": "", "contact_email": "",
                "buyer_country": buyer_country, "buyer_industry": "",
                "status": "sourcing_needed", "source": "switch_lead_engine_v1_streaming",
                "created_at": now_str,
            }])
            ok(f"detect_stress_for_rel: unmatched SwitchLead for {rel_id} ({health_status}, {detection_method})")
            return True

        # ── build leads for top candidates ────────────────────────────────────
        lead_batch = []
        for cand in candidates_raw[:1]:   # top match only for streaming
            sup_org_id   = cand["supplier_org_id"]
            sup_name_new = cand["supplier_name"]
            mscore       = cand["match_score"]
            match_id     = stable_id(opp_id, sup_org_id)
            final_score  = round(min(100.0, (switch_prob * 40) + (mscore * 0.35) +
                                     min(buyer_vol / 10000, 25)), 1)
            lead_priority = (
                "critical" if final_score >= 80 else
                "high"     if final_score >= 65 else
                "medium"
            )
            country_note = " Already ships to buyer country." if cand.get("exports_to_buyer_country") else ""
            active_note  = f" {cand['active_buyer_count']} active buyers." if cand.get("active_buyer_count") else ""

            self.neo.run("""
                MERGE (sm:SupplierMatch {match_id: $match_id})
                SET sm.opportunity_id            = $opp_id,
                    sm.candidate_supplier_org_id = $sup_org_id,
                    sm.candidate_supplier_name   = $sup_name,
                    sm.hs_code_match_type        = $match_type,
                    sm.match_score               = $mscore,
                    sm.rank                      = $rank,
                    sm.active_buyer_count        = $active_buyers,
                    sm.exports_to_buyer_country  = $to_country,
                    sm.has_zi_contact            = $has_zi,
                    sm.created_at                = $now
                WITH sm
                MATCH (opp:SupplierSwitchOpportunity {opportunity_id: $opp_id})
                MERGE (opp)-[:HAS_MATCH]->(sm)
            """, {
                "match_id": match_id, "opp_id": opp_id,
                "sup_org_id": sup_org_id, "sup_name": sup_name_new,
                "match_type": "ranker" if detection_method == "ml_based" else "streaming",
                "mscore": mscore, "rank": cand["rank"],
                "active_buyers": cand["active_buyer_count"],
                "to_country": cand.get("exports_to_buyer_country", False),
                "has_zi": cand.get("has_zi_contact", False),
                "now": now_str,
            })

            lead_batch.append({
                "lead_id": stable_id(match_id, "switch_lead"),
                "opportunity_id": opp_id, "match_id": match_id,
                "buyer_org_id": buyer_id, "buyer_name": buyer_name,
                "candidate_supplier_org_id": sup_org_id,
                "candidate_supplier_name": sup_name_new,
                "hs_code": hs_code, "stress_reason": stress_reason,
                "switch_probability": switch_prob, "match_score": mscore,
                "final_lead_score": final_score, "lead_priority": lead_priority,
                "buyer_monthly_volume": buyer_vol,
                "recommended_action": (
                    f"Buyer: {buyer_name} imports {hs_display} from {sup_name}. "
                    f"Relationship showing {stress_reason.replace('_', ' ')}. "
                    f"Switch probability: {switch_prob:.0%}. "
                    f"Recommended: {sup_name_new}.{country_note}{active_note}"
                ),
                "contact_name": "", "contact_title": "", "contact_email": "",
                "buyer_country": buyer_country, "buyer_industry": "",
                "status": "new", "source": "switch_lead_engine_v1_streaming",
                "created_at": now_str,
            })

        self._publish_leads(lead_batch)
        ok(f"detect_stress_for_rel: SwitchLead for {rel_id} "
           f"({health_status}, {detection_method}, prob={switch_prob:.2f})")
        return True


    def run_all(self) -> None:
        banner('SwitchLeadEngine: Running Full Switch Lead Pipeline')
        n_rels        = self.build_trade_relationships()
        n_orgs        = self.enrich_with_zoominfo()

        # Stage 2B — Trademo health + company profile enrichment
        from app.features.trademo_relationship_enricher import TrademoRelationshipEnricher
        from app.features.trademo_company_profile_enricher import TrademoCompanyProfileEnricher
        n_rh_enriched = TrademoRelationshipEnricher(self.neo, self.pg, self.settings).run().get('relationships_enriched', 0)
        n_cp_enriched = TrademoCompanyProfileEnricher(self.neo, self.pg, self.settings).run().get('organizations_enriched', 0)

        n_rule_opps   = self.detect_stress_rules()   # Stage 3A — rule-based (always runs)
        n_ml_opps     = self.detect_stress()          # Stage 3B — ML-based (runs if model exists)
        n_scored      = self.score_switch_probability()
        n_match       = self.match_suppliers()
        n_leads       = self.create_switch_leads()
        ok(
            f'SwitchLeadEngine complete — '
            f'relationships={n_rels}, orgs_enriched={n_orgs}, '
            f'rh_enriched={n_rh_enriched}, cp_enriched={n_cp_enriched}, '
            f'rule_opportunities={n_rule_opps}, ml_opportunities={n_ml_opps}, '
            f'scored={n_scored}, matches={n_match}, leads={n_leads}'
        )
