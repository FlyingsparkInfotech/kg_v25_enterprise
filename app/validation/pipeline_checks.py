from app.core.logger import info, ok, warn


class PipelineValidator:
    def __init__(self, neo, settings):
        self.neo      = neo
        self.settings = settings
        self.sc       = settings.scoring   # ScoringCfg — thresholds from config

    def run(self):
        checks = {
            'total_signals':
                'MATCH (s:Signal) RETURN count(s) AS c',
            'v25_signals_without_identity':
                "MATCH (s:Signal) WHERE s.source IN ['neo4j_derived','postgres'] "
                "AND NOT (s)-[:GENERATES]->(:IdentityHypothesis) RETURN count(s) AS c",
            'total_signals_with_identity':
                "MATCH (s:Signal)-[:GENERATES]->(:IdentityHypothesis) RETURN count(s) AS c",
            'evidence_missing_strength':
                'MATCH (e:Evidence) WHERE e.evidence_strength IS NULL RETURN count(e) AS c',
            'evidence_invalid_strength':
                'MATCH (e:Evidence) WHERE e.evidence_strength < 0 OR e.evidence_strength > 100 RETURN count(e) AS c',
            'v25_leads_without_classification':
                "MATCH (l:Lead) WHERE l.source='lead_factory_v25' "
                "AND NOT (l)<-[:CREATES]-(:LeadClassification) RETURN count(l) AS c",
            'weak_seller_visible_leads':
                "MATCH (l:Lead) WHERE l.source='lead_factory_v25' "
                f"AND coalesce(l.evidence_strength,0)<50 RETURN count(l) AS c",
            'total_leads':
                "MATCH (l:Lead) WHERE l.source='lead_factory_v25' RETURN count(l) AS c",
            'orphaned_identity_hypotheses':
                'MATCH (ih:IdentityHypothesis) WHERE NOT (ih)<-[:GENERATES]-(:Signal) RETURN count(ih) AS c',
            'evidence_without_identity':
                'MATCH (e:Evidence) WHERE NOT (e)-[:SUPPORTS]->(:IdentityHypothesis) RETURN count(e) AS c',
        }

        props = {}
        for k, q in checks.items():
            try:
                r = self.neo.run(q)
                props[k] = r[0]['c'] if r else 0
            except Exception as e:
                warn(f'Validation check failed [{k}]: {e}')
                props[k] = -1
            info(f'{k}: {props[k]}')

        # ── SLO threshold checks ──────────────────────────────────────────────
        failures = []
        sc = self.sc

        total_sigs = props.get('total_signals', 0) or 1  # avoid /0
        total_leads = props.get('total_leads', 0) or 1

        miss_pct = 100.0 * (props.get('v25_signals_without_identity', 0) / total_sigs)
        if miss_pct > sc.max_identity_miss_pct:
            msg = (f'FAIL: {miss_pct:.1f}% signals without identity > '
                   f'SLO {sc.max_identity_miss_pct}%')
            warn(msg); failures.append(msg)

        ev_null = props.get('evidence_missing_strength', 0)
        total_ev_q = self.neo.run('MATCH (e:Evidence) RETURN count(e) AS c')
        total_ev = (total_ev_q[0]['c'] if total_ev_q else 0) or 1
        ev_null_pct = 100.0 * ev_null / total_ev
        if ev_null_pct > sc.max_evidence_null_pct:
            msg = (f'FAIL: {ev_null_pct:.1f}% evidence nodes have null strength > '
                   f'SLO {sc.max_evidence_null_pct}%')
            warn(msg); failures.append(msg)

        weak_pct = 100.0 * (props.get('weak_seller_visible_leads', 0) / total_leads)
        if weak_pct > sc.max_weak_leads_pct:
            msg = (f'WARN: {weak_pct:.1f}% leads are weak (evidence<{sc.seller_visible_evidence_min}) > '
                   f'SLO {sc.max_weak_leads_pct}%')
            warn(msg); failures.append(msg)

        if props.get('evidence_invalid_strength', 0) > 0:
            msg = f"FAIL: {props['evidence_invalid_strength']} evidence nodes have invalid strength (<0 or >100)"
            warn(msg); failures.append(msg)

        if props.get('v25_leads_without_classification', 0) > 0:
            msg = f"WARN: {props['v25_leads_without_classification']} leads exist without a LeadClassification"
            warn(msg); failures.append(msg)

        # ── Persist report to Neo4j ───────────────────────────────────────────
        props['identity_miss_pct']    = round(miss_pct, 2)
        props['evidence_null_pct']    = round(ev_null_pct, 2)
        props['weak_leads_pct']       = round(weak_pct, 2)
        props['slo_failures']         = len(failures)
        props['slo_failure_messages'] = ' | '.join(failures) if failures else 'all_passed'
        props['passed']               = len(failures) == 0

        self.neo.run(
            "MERGE (r:ValidationReport {report_id:'validation:v25:latest'}) "
            "SET r += $props, r.generated_at=toString(datetime())",
            {'props': props},
        )

        if failures:
            warn(f'Validation complete with {len(failures)} SLO failure(s)')
        else:
            ok('Validation complete — all SLOs passed')
