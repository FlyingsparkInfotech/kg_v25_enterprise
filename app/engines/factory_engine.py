from __future__ import annotations
import time
import logging
from app.core.logger import info, ok, warn, banner

log = logging.getLogger(__name__)


class FactoryEngine:
    """Memory-safe modular factory.

    Each stage writes its outputs to Neo4j and marks source nodes done. This avoids
    one giant Cypher query and prevents WITH variable-scope errors.

    All scoring thresholds are read from settings.scoring so they can be tuned
    in config.yaml without touching this file.
    """

    def __init__(self, neo, settings, tracker=None):
        self.neo      = neo
        self.settings = settings
        self.tracker  = tracker
        self.model    = settings.runtime.scoring_model_version
        self.max_rows = int(settings.runtime.max_rows_per_step or 50000)
        self.batch    = max(100, int(settings.runtime.signal_batch or 2000))
        self.sc       = settings.scoring   # ScoringCfg — all thresholds from config

    # ── internal helpers ─────────────────────────────────────────────────────

    def _log_metric(self, name, value, step=None):
        if self.tracker:
            self.tracker.metric(name, value, step=step)

    def _loop(self, stage: str, query: str, params: dict | None = None,
              max_rows: int | None = None, retries: int = 3) -> int:
        """
        Run a batched Cypher write query until no rows remain.
        Retries on transient failures with exponential back-off.
        Returns total rows processed.
        """
        params      = dict(params or {})
        limit_total = self.max_rows if max_rows is None else int(max_rows or 0)
        total       = 0
        batch_no    = 0

        while True:
            if limit_total and total >= limit_total:
                info(f'{stage}: reached max_rows={limit_total}; rerun same command to continue')
                break
            current = self.batch if not limit_total else min(self.batch, limit_total - total)
            if current <= 0:
                break

            batch_no    += 1
            params['batch'] = current
            c = 0

            for attempt in range(retries):
                try:
                    rows = self.neo.run(query, params)
                    if rows is None:
                        raise ValueError(f'{stage}: neo4j returned None')
                    c = int(rows[0].get('c', 0)) if rows else 0
                    break
                except Exception as e:
                    if attempt < retries - 1:
                        wait = 2 ** attempt
                        warn(f'{stage}: error on attempt {attempt+1}/{retries} — retry in {wait}s: {e}')
                        time.sleep(wait)
                    else:
                        warn(f'{stage}: all retries exhausted, aborting batch loop: {e}')
                        return total

            if c <= 0:
                info(f'{stage}: no more rows')
                break
            total += c
            info(f'{stage}: +{c} batch={batch_no} total={total}')
            self._log_metric(stage.replace(' ', '_').lower(), total, step=batch_no)
            if c < current:
                break

        return total

    # ── pipeline stages ──────────────────────────────────────────────────────

    def derive_signals(self):
        banner('V25.2 DERIVE UI/CRM SIGNALS')
        sources = [
            ('PageView', 'pageViewId',  'page_view',          'WebsiteBehaviorSource',        self.max_rows),
            ('RFQ',      'rfqId',       'rfq_created',         'RFQInquirySource',             100000),
            ('Lead',     'leadId',      'lead_created',        'CRMSource',                    100000),
            ('Meeting',  'meetingId',   'meeting_scheduled',   'CRMSource',                    100000),
            ('Deal',     'dealId',      'deal_created',        'CommercialProgressionSource',  100000),
        ]
        q_tpl = '''
        MATCH (n:{label})
        WHERE n.{idf} IS NOT NULL AND coalesce(n.v25_signal_created,false)=false
        WITH n LIMIT $batch
        WITH n, toString(n.{idf}) AS id
        MERGE (raw:RawSource {{raw_source_uid:'raw:neo4j:{label}:'+id}})
        SET raw:SourceRecord,
            raw.source_record_uid=raw.raw_source_uid,
            raw.source_key='neo4j.{label}',
            raw.source_record_id=id,
            raw.source_table='{label}',
            raw.source_system='neo4j_derived',
            raw.occurred_at=coalesce(n.createdAt,n.created_at,n.eventTs,toString(datetime())),
            raw.confidence_score=0.75
        MERGE (sig:Signal {{signal_uid:'signal:neo4j:{label}:'+id}})
        SET sig.signalId=sig.signal_uid,
            sig.source_key='neo4j.{label}',
            sig.signal_type=$st,
            sig.signalType=$st,
            sig.source_family=$sf,
            sig.sourceFamily=$sf,
            sig.occurred_at=coalesce(n.createdAt,n.created_at,n.eventTs,toString(datetime())),
            sig.confidence_score=0.75,
            sig.intensity_score=0.7,
            sig.account_hint=coalesce(toString(n.accountId),toString(n.userId),toString(n.user_id),toString(n.buyer_user_id)),
            sig.person_hint=coalesce(toString(n.personId),toString(n.contactId),toString(n.contact_id)),
            sig.product_hint=coalesce(toString(n.productId),toString(n.product_id)),
            sig.category_hint=coalesce(toString(n.categoryId),toString(n.category_id)),
            sig.source='neo4j_derived',
            sig.sourceTable='{label}',
            sig.v25_stage=coalesce(sig.v25_stage,'signal_loaded')
        MERGE (reg:SourceRegistry {{source_key:'neo4j.{label}'}})
        SET reg.source_group=$sf, reg.source_name='{label}', reg.is_active=true
        MERGE (typ:SignalType {{name:$st}})
        MERGE (fam:SourceFamily {{name:$sf}})
        MERGE (raw)-[:RECORDED_IN_SOURCE]->(reg)
        MERGE (raw)-[:NORMALIZES_TO]->(sig)
        MERGE (sig)-[:FROM_SOURCE]->(reg)
        MERGE (sig)-[:HAS_TYPE]->(typ)
        MERGE (sig)-[:BELONGS_TO]->(fam)
        SET n.v25_signal_created=true
        RETURN count(n) AS c
        '''
        grand = 0
        for label, idf, st, sf, max_rows in sources:
            total = self._loop(
                f'{label}->Signal',
                q_tpl.format(label=label, idf=idf),
                {'st': st, 'sf': sf},
                max_rows=max_rows,
            )
            grand += total
        ok(f'Derive signals complete, created/updated={grand}')

    def identity(self):
        q = '''
        MATCH (s:Signal)
        WHERE coalesce(s.v25_identity_done,false)=false
        WITH s, coalesce(s.signal_type,s.signalType) AS st0, coalesce(s.source_family,s.sourceFamily) AS sf0
        WITH s, st0, sf0,
             CASE
               WHEN st0 IN ['rfq_submitted','rfq_created','rfq_started','inquiry_created'] THEN 100
               WHEN st0 IN ['deal_created','deal_stage_progressed','quote_sent','quote_opened','quote_accepted','po_received','contract_signed','deal_won'] THEN 95
               WHEN sf0='TradeShipmentSource' OR st0 CONTAINS 'shipment' OR st0 CONTAINS 'import' OR st0 CONTAINS 'export' THEN 90
               WHEN st0 IN ['meeting_scheduled','contact_enriched','third_party_intent_detected','business_event_detected','news_trigger_detected'] THEN 80
               WHEN st0 IN ['lead_created'] THEN 70
               ELSE 10 END AS factory_priority
        ORDER BY factory_priority DESC
        LIMIT $batch
        WITH s, coalesce(s.signal_uid,s.signalId) AS sid,
             coalesce(s.signal_type,s.signalType) AS st,
             coalesce(s.source_family,s.sourceFamily) AS sf
        WHERE sid IS NOT NULL
        MERGE (ih:IdentityHypothesis {hypothesis_id:'ih25:'+sid})
        SET ih.signal_type=st,
            ih.candidate_entity_type=CASE
                WHEN s.account_hint IS NOT NULL THEN 'Account'
                WHEN s.person_hint  IS NOT NULL THEN 'Person'
                WHEN s.domain_hint  IS NOT NULL THEN 'Organization'
                ELSE 'AnonymousSession' END,
            ih.candidate_entity_id=coalesce(
                toLower(trim(toString(s.account_hint))),
                toLower(trim(toString(s.person_hint))),
                toLower(trim(toString(s.domain_hint))),
                toString(s.session_id),
                sid),
            ih.resolution_method=CASE
                WHEN s.account_hint IS NOT NULL AND sf='TradeShipmentSource' THEN 'trademo_company_id_or_account_hint'
                WHEN s.account_hint IS NOT NULL THEN 'exact_account_id_match'
                WHEN s.person_hint  IS NOT NULL THEN 'exact_person_id_match'
                WHEN s.domain_hint  IS NOT NULL THEN 'email_domain_match'
                ELSE 'unresolved_signal' END,
            ih.confidence_score=CASE
                WHEN s.account_hint IS NOT NULL AND sf='TradeShipmentSource' THEN 0.95
                WHEN s.account_hint IS NOT NULL THEN 0.78
                WHEN s.person_hint  IS NOT NULL THEN 0.80
                WHEN s.domain_hint  IS NOT NULL THEN 0.70
                ELSE 0.40 END,
            ih.status=CASE
                WHEN s.account_hint IS NOT NULL OR s.person_hint IS NOT NULL OR s.domain_hint IS NOT NULL
                    THEN 'proposed'
                ELSE 'poor_identity' END,
            ih.scoring_model_version=$m,
            ih.created_at=coalesce(ih.created_at,toString(datetime()))
        MERGE (s)-[:GENERATES]->(ih)
        SET s.v25_identity_done=true, s.v25_stage='identity_done'
        RETURN count(s) AS c
        '''
        self._loop('IdentityHypothesis', q, {'m': self.model})

        # Lightweight resolution links — normalise both sides with toLower+trim
        # to handle mixed-case IDs from different source systems.
        self.neo.run(
            """
            MATCH (ih:IdentityHypothesis)
            WHERE ih.candidate_entity_type='Account'
              AND NOT (ih)-[:RESOLVES_TO]->(:Account)
            WITH ih LIMIT $batch
            MATCH (a:Account)
            WHERE toLower(trim(toString(coalesce(a.account_uid,a.accountId,a.userId,a.id,a.account_id))))
                = toLower(trim(toString(ih.candidate_entity_id)))
            MERGE (ih)-[:RESOLVES_TO {resolution_confidence:ih.confidence_score}]->(a)
            RETURN count(ih) AS c
            """,
            {'batch': self.batch},
        )
        self.neo.run(
            """
            MATCH (ih:IdentityHypothesis)
            WHERE ih.candidate_entity_type='Person'
              AND NOT (ih)-[:RESOLVES_TO]->(:Person)
            WITH ih LIMIT $batch
            MATCH (p:Person)
            WHERE toLower(trim(toString(coalesce(p.person_id,p.personId,p.id,p.userId,p.contactId))))
                = toLower(trim(toString(ih.candidate_entity_id)))
            MERGE (ih)-[:RESOLVES_TO {resolution_confidence:ih.confidence_score}]->(p)
            RETURN count(ih) AS c
            """,
            {'batch': self.batch},
        )

    def evidence(self):
        sc = self.sc
        # Gap 4: evidence decay e^(-age_days/30)
        # Gap 6: time window filtering per signal type
        q = '''
        MATCH (s:Signal)-[:GENERATES]->(ih:IdentityHypothesis)
        WHERE coalesce(s.v25_evidence_done,false)=false
        WITH s,ih,coalesce(s.signal_type,s.signalType) AS st0, coalesce(s.source_family,s.sourceFamily) AS sf0
        WITH s,ih,st0,sf0,
             CASE
               WHEN st0 IN ['rfq_submitted','rfq_created','rfq_started','inquiry_created'] THEN 100
               WHEN st0 IN ['deal_created','deal_stage_progressed','quote_sent','quote_opened','quote_accepted','po_received','contract_signed','deal_won'] THEN 95
               WHEN sf0='TradeShipmentSource' OR st0 CONTAINS 'shipment' OR st0 CONTAINS 'import' OR st0 CONTAINS 'export' THEN 90
               WHEN st0 IN ['meeting_scheduled','contact_enriched','third_party_intent_detected','business_event_detected','news_trigger_detected'] THEN 80
               WHEN st0 IN ['lead_created'] THEN 70
               ELSE 10 END AS factory_priority
        ORDER BY factory_priority DESC
        LIMIT $batch
        WITH s,ih,coalesce(s.signal_uid,s.signalId) AS sid,
             coalesce(s.signal_type,s.signalType) AS st,
             coalesce(s.source_family,s.sourceFamily) AS sf
        // Signal age in days — replace space with T to handle both ISO variants
        WITH s,ih,sid,st,sf,
             toInteger((datetime().epochMillis -
                        datetime(replace(coalesce(s.occurred_at,toString(datetime())),' ','T')).epochMillis)
                       / 86400000) AS age_raw
        WITH s,ih,sid,st,sf,
             CASE WHEN age_raw < 0 THEN 0 ELSE age_raw END AS signal_age_days
        // Time-window filter: skip signals outside their staleness window
        WITH s,ih,sid,st,sf,signal_age_days
        WHERE signal_age_days <= CASE
            WHEN st IN ['rfq_created','rfq_submitted','rfq_started','inquiry_created',
                        'specification_uploaded','drawing_uploaded','bom_uploaded','boq_uploaded'] THEN $tw_rfq
            WHEN sf='TradeShipmentSource' OR st CONTAINS 'shipment' OR st CONTAINS 'import'
                                          OR st CONTAINS 'export' THEN $tw_shipment
            WHEN st IN ['contact_enriched','firmographic_update_detected','company_discovered'] THEN $tw_enrichment
            ELSE $tw_behavior END
        // Evidence confidence and type classification
        WITH s,ih,sid,st,sf,signal_age_days,
             CASE WHEN sf='TradeShipmentSource'          THEN 0.90
                  WHEN sf='EnrichmentSource'             THEN 0.85
                  WHEN sf='ThirdPartyIntelligenceSource' THEN 0.70
                  WHEN sf='CRMSource'                    THEN 0.85
                  WHEN sf='RFQInquirySource'             THEN 0.95
                  ELSE coalesce(toFloat(s.confidence_score),0.70) END AS signal_confidence,
             CASE WHEN st IN ['rfq_created','rfq_submitted','rfq_started','inquiry_created',
                              'specification_uploaded','drawing_uploaded','bom_uploaded','boq_uploaded']
                       THEN 'rfq_intent_detected'
                  WHEN sf='TradeShipmentSource' OR st CONTAINS 'shipment' OR st CONTAINS 'import'
                       THEN 'active_importer_detected'
                  WHEN st IN ['contact_enriched','firmographic_update_detected','company_discovered']
                       THEN 'decision_maker_presence_detected'
                  WHEN st IN ['quote_sent','quote_opened','quote_accepted','po_received',
                              'contract_signed','deal_won']
                       THEN 'procurement_commitment_detected'
                  WHEN st IN ['lead_created','deal_created','deal_stage_progressed']
                       THEN 'active_opportunity_detected'
                  WHEN st IN ['meeting_scheduled','event_attended']
                       THEN 'event_engagement_detected'
                  ELSE 'product_interest_cluster' END AS ev_type
        WITH s,ih,sid,st,sf,signal_age_days,signal_confidence,ev_type,
             CASE WHEN ev_type='active_importer_detected'         THEN 'trade'
                  WHEN ev_type='rfq_intent_detected'              THEN 'intent'
                  WHEN ev_type='decision_maker_presence_detected' THEN 'enrichment'
                  WHEN ev_type='procurement_commitment_detected'  THEN 'commitment'
                  ELSE 'behavior' END AS ev_cat,
             CASE WHEN ev_type='rfq_intent_detected'             THEN $base_rfq
                  WHEN ev_type='active_importer_detected'        THEN $base_importer
                  WHEN ev_type='procurement_commitment_detected' THEN $base_commitment
                  WHEN ev_type='decision_maker_presence_detected' THEN $base_dm
                  ELSE $base_default END AS base_strength,
             CASE WHEN s.product_hint IS NOT NULL OR s.category_hint IS NOT NULL
                       OR s.hs_code IS NOT NULL THEN $bonus_product ELSE 0 END
             + CASE WHEN s.person_hint IS NOT NULL OR s.email_hint IS NOT NULL
                         OR s.phone_hint IS NOT NULL THEN $bonus_person ELSE 0 END AS context_bonus,
             CASE WHEN (sf='TradeShipmentSource' AND
                        (s.product_hint IS NOT NULL OR s.hs_code IS NOT NULL))
                       OR ev_type='rfq_intent_detected'
                  THEN $bonus_combo ELSE 0 END AS combination_bonus,
             exp(-toFloat(signal_age_days) / 30.0) AS decay_factor
        // Cap raw strength then apply decay
        WITH s,ih,sid,st,sf,signal_age_days,signal_confidence,ev_type,ev_cat,
             base_strength,context_bonus,combination_bonus,decay_factor,
             CASE WHEN base_strength+context_bonus+combination_bonus>100
                  THEN 100
                  ELSE base_strength+context_bonus+combination_bonus END AS raw_strength
        WITH s,ih,sid,st,sf,signal_age_days,signal_confidence,ev_type,ev_cat,
             raw_strength,context_bonus,combination_bonus,decay_factor,
             CASE WHEN toInteger(round(toFloat(raw_strength)*decay_factor)) < 1 THEN 1
                  ELSE toInteger(round(toFloat(raw_strength)*decay_factor)) END AS evidence_strength
        MERGE (e:Evidence {evidence_uid:'ev25:'+sid})
        SET e.evidence_type=ev_type,
            e.evidence_category=ev_cat,
            e.base_signal_strength=raw_strength,
            e.context_bonus=context_bonus,
            e.combination_bonus=combination_bonus,
            e.evidence_strength=evidence_strength,
            e.evidence_strength_raw=raw_strength,
            e.decay_factor=round(decay_factor*1000)/1000.0,
            e.signal_age_days=signal_age_days,
            e.evidence_strength_formula='(base+context+combo) * exp(-age_days/30)',
            e.confidence_score=signal_confidence,
            e.signal_confidence=signal_confidence,
            e.product_match=CASE WHEN s.product_hint IS NOT NULL OR s.category_hint IS NOT NULL
                                      OR s.hs_code IS NOT NULL THEN 1.0 ELSE 0.3 END,
            e.contact_exists=CASE WHEN s.person_hint IS NOT NULL OR s.email_hint IS NOT NULL
                                       OR s.phone_hint IS NOT NULL THEN true ELSE false END,
            e.rfq_exists=CASE WHEN ev_type='rfq_intent_detected' THEN true ELSE false END,
            e.trade_exists=CASE WHEN ev_cat='trade' THEN true ELSE false END,
            e.number_of_shipments=coalesce(toFloat(s.number_of_shipments),0),
            e.scoring_model_version=$m,
            e.created_at=coalesce(e.created_at,toString(datetime()))
        MERGE (s)-[:CREATES]->(e)
        MERGE (e)-[:DERIVED_FROM]->(s)
        MERGE (e)-[:SUPPORTS]->(ih)
        MERGE (cat:EvidenceCategory {name:ev_cat})
        MERGE (e)-[:BELONGS_TO_CATEGORY]->(cat)
        SET s.v25_evidence_done=true, s.v25_stage='evidence_done'
        RETURN count(s) AS c
        '''
        self._loop('Evidence', q, {
            'm':              self.model,
            'base_rfq':       sc.evidence_base_rfq,
            'base_importer':  sc.evidence_base_importer,
            'base_commitment':sc.evidence_base_commitment,
            'base_dm':        sc.evidence_base_decision_maker,
            'base_default':   sc.evidence_base_default,
            'bonus_product':  sc.bonus_product_hint,
            'bonus_person':   sc.bonus_person_hint,
            'bonus_combo':    sc.bonus_combination,
            'tw_rfq':         sc.time_window_rfq_days,
            'tw_shipment':    sc.time_window_shipment_days,
            'tw_enrichment':  sc.time_window_enrichment_days,
            'tw_behavior':    sc.time_window_behavior_days,
        })

    def opportunity(self):
        sc = self.sc
        q = '''
        MATCH (e:Evidence)
        WHERE e.scoring_model_version=$m AND NOT (e)-[:MAPS_TO]->(:OpportunityHypothesis)
        WITH e LIMIT $batch
        WITH e, CASE
            WHEN e.evidence_type='active_importer_detected'         THEN 'new_sourcing_cycle'
            WHEN e.evidence_type='rfq_intent_detected'              THEN 'procurement_project_active'
            WHEN e.evidence_type='decision_maker_presence_detected' THEN 'buying_committee_active'
            WHEN e.evidence_type='procurement_commitment_detected'  THEN 'strategic_consolidation'
            WHEN e.evidence_type='active_opportunity_detected'      THEN 'champion_reengagement'
            ELSE 'high_intent_abandonment' END AS htype
        MERGE (oh:OpportunityHypothesis {hypothesis_id:'oh25:'+e.evidence_uid})
        SET oh.hypothesis_type=htype,
            oh.rule='v25_from_'+e.evidence_type,
            oh.threshold=$opp_threshold,
            oh.confidence=$w_evidence*(toFloat(e.evidence_strength)/100.0)
                         +$w_product*coalesce(e.product_match,0.3)
                         +$w_contact*(CASE WHEN e.contact_exists THEN 1.0 ELSE 0.0 END),
            oh.scoring_model_version=$m,
            oh.created_at=coalesce(oh.created_at,toString(datetime()))
        MERGE (e)-[:MAPS_TO]->(oh)
        MERGE (oh)-[:TRIGGERED_BY]->(e)
        RETURN count(e) AS c
        '''
        self._loop('OpportunityHypothesis', q, {
            'm':             self.model,
            'opp_threshold': sc.opp_threshold,
            'w_evidence':    sc.opp_weight_evidence,
            'w_product':     sc.opp_weight_product,
            'w_contact':     sc.opp_weight_contact,
        })

    def fit(self):
        sc = self.sc
        # Gap 2: 6-component multi-dimensional scoring
        # Behavior(15%) + Intent(25%) + Trade(25%) + Fit(15%) + Recency(10%) + Reachability(10%)
        q = '''
        MATCH (e:Evidence)-[:SUPPORTS]->(ih:IdentityHypothesis)
        WHERE e.scoring_model_version=$m AND NOT (e)-[:EVALUATED_INTO]->(:FitSuppression)
        WITH e,ih LIMIT $batch
        // 6 score components — each normalised 0-100
        WITH e,ih,
             CASE WHEN e.evidence_category='behavior' THEN toFloat(e.evidence_strength) ELSE 0.0 END AS behavior_score,
             CASE WHEN e.evidence_category='intent' THEN toFloat(e.evidence_strength) ELSE 0.0 END AS intent_score,
             CASE WHEN e.evidence_category IN ['trade','commitment'] THEN toFloat(e.evidence_strength) ELSE 0.0 END AS trade_score,
             coalesce(e.product_match,0.3)*100.0 AS fit_score,
             coalesce(e.decay_factor,1.0)*100.0   AS recency_score,
             CASE WHEN e.contact_exists=true THEN 100.0 ELSE 25.0 END AS reachability_score
        WITH e,ih,behavior_score,intent_score,trade_score,fit_score,recency_score,reachability_score,
             round($w_behavior*behavior_score + $w_intent*intent_score + $w_trade*trade_score
                   + $w_fit*fit_score + $w_recency*recency_score
                   + $w_reachability*reachability_score) AS six_component_score
        // Suppression rules (applied to six_component_score)
        WITH e,ih,behavior_score,intent_score,trade_score,fit_score,recency_score,reachability_score,
             six_component_score,
             CASE WHEN ih.confidence_score < $suppress_weak_identity                THEN 'weak_identity'
                  WHEN coalesce(e.signal_confidence,e.confidence_score,0)
                           < $suppress_low_confidence                               THEN 'low_confidence'
                  WHEN coalesce(e.evidence_strength,0) < $suppress_low_intent       THEN 'low_intent'
                  WHEN six_component_score < $suppress_no_fit                       THEN 'no_fit'
                  ELSE null END AS suppression
        MERGE (fs:FitSuppression {fit_suppression_uid:'fs25:'+e.evidence_uid})
        SET fs.behavior_score=behavior_score,
            fs.intent_score=intent_score,
            fs.trade_score=trade_score,
            fs.fit_score=fit_score,
            fs.recency_score=recency_score,
            fs.reachability_score=reachability_score,
            fs.six_component_score=six_component_score,
            fs.fit_band=CASE WHEN six_component_score>=85 THEN 'very_high_fit'
                             WHEN six_component_score>=70 THEN 'high_fit'
                             WHEN six_component_score>=50 THEN 'medium_fit'
                             ELSE 'low_fit' END,
            fs.suppression_type=suppression,
            fs.suppression_reason=coalesce(suppression,'none'),
            fs.final_decision=CASE WHEN suppression='weak_identity' THEN 'block'
                                   WHEN suppression IS NOT NULL     THEN 'review'
                                   WHEN six_component_score>=70     THEN 'allow'
                                   WHEN six_component_score>=50     THEN 'review'
                                   ELSE 'block' END,
            fs.final_score=six_component_score,
            fs.scoring_model_version=$m,
            fs.evaluated_at=coalesce(fs.evaluated_at,toString(datetime()))
        MERGE (e)-[:EVALUATED_INTO]->(fs)
        RETURN count(e) AS c
        '''
        self._loop('FitSuppression', q, {
            'm':                       self.model,
            'w_behavior':              sc.score_weight_behavior,
            'w_intent':                sc.score_weight_intent,
            'w_trade':                 sc.score_weight_trade,
            'w_fit':                   sc.score_weight_fit,
            'w_recency':               sc.score_weight_recency,
            'w_reachability':          sc.score_weight_reachability,
            'suppress_weak_identity':  sc.suppress_weak_identity_below,
            'suppress_low_confidence': sc.suppress_low_confidence_below,
            'suppress_low_intent':     sc.suppress_low_intent_below,
            'suppress_no_fit':         sc.suppress_no_fit_below,
        })

    def classify(self):
        sc = self.sc
        # Gap 5: 19-type lead taxonomy
        # Gap 3: hard score constraints per lead type
        # Gap 9: type-specific score thresholds
        q = '''
        MATCH (e:Evidence)-[:EVALUATED_INTO]->(fs:FitSuppression)
        WHERE fs.scoring_model_version=$m AND NOT (fs)-[:QUALIFIES_FOR]->(:LeadClassification)
        WITH e,fs LIMIT $batch
        WITH e,fs,fs.six_component_score AS scs,
             CASE
                 WHEN fs.final_decision='block' THEN 'blocked'
                 // RFQ types — split by hard score floor
                 WHEN e.evidence_type='rfq_intent_detected' AND scs >= $min_rfq THEN 'rfq_submitted'
                 WHEN e.evidence_type='rfq_intent_detected' THEN 'rfq_draft'
                 // Commitment types
                 WHEN e.evidence_type='procurement_commitment_detected'
                      AND coalesce(e.number_of_shipments,0) >= 5 THEN 'strategic_consolidation'
                 WHEN e.evidence_type='procurement_commitment_detected' THEN 'deal_ready'
                 // Active opportunity
                 WHEN e.evidence_type='active_opportunity_detected'
                      AND scs >= $priority_high THEN 'active_deal'
                 WHEN e.evidence_type='active_opportunity_detected' THEN 'procurement_project_active'
                 // Trade types — tiered by shipment count and score
                 WHEN e.evidence_category='trade'
                      AND coalesce(e.number_of_shipments,0) >= 5
                      AND scs >= $priority_high THEN 'active_importer'
                 WHEN e.evidence_category='trade'
                      AND scs >= $priority_high THEN 'trade_buyer_candidate'
                 WHEN e.evidence_category='trade'
                      AND scs >= $min_trade THEN 'market_opportunity'
                 WHEN e.evidence_category='trade' THEN 'cold_market'
                 // Decision-maker / enrichment types
                 WHEN e.evidence_type='decision_maker_presence_detected'
                      AND scs >= $min_dm THEN 'buying_committee_active'
                 WHEN e.evidence_type='decision_maker_presence_detected' THEN 'strategic_account'
                 // Event engagement
                 WHEN e.evidence_type='event_engagement_detected'
                      AND scs >= $priority_critical THEN 'quote_ready'
                 WHEN e.evidence_type='event_engagement_detected'
                      AND scs >= $priority_high THEN 'hot_in_market'
                 // Score-tier fallbacks
                 WHEN scs >= $priority_critical THEN 'procurement_project_active'
                 WHEN scs >= $priority_high     THEN 'hot_in_market'
                 WHEN scs >= 50                 THEN 'warm_account'
                 WHEN scs >= 30                 THEN 'intent_only'
                 ELSE 'visit_only' END AS lt
        MERGE (lc:LeadClassification {classification_uid:'lc25:'+fs.fit_suppression_uid})
        SET lc.lead_type=lt,
            lc.lead_grain=CASE WHEN lt='blocked' THEN 'none'
                               WHEN e.rfq_exists=true OR e.trade_exists=true
                                    OR coalesce(e.product_match,0)>=0.60 THEN 'opportunity_lead'
                               WHEN e.contact_exists=true                THEN 'person_lead'
                               ELSE 'account_lead' END,
            lc.lead_stage=CASE WHEN lt IN ['deal_ready','quote_ready','rfq_submitted',
                                           'strategic_consolidation'] THEN 'late'
                               WHEN lt='blocked'                        THEN 'invalid'
                               WHEN lt IN ['visit_only','cold_market']  THEN 'early'
                               ELSE 'mid' END,
            lc.priority=CASE
                WHEN lt IN ['deal_ready','quote_ready','rfq_submitted','strategic_consolidation']
                     OR scs >= $priority_critical THEN 'critical'
                WHEN lt IN ['rfq_draft','hot_in_market','market_opportunity','active_deal',
                            'active_importer','trade_buyer_candidate','buying_committee_active']
                     OR scs >= $priority_high THEN 'high'
                WHEN lt IN ['warm_account','procurement_project_active','strategic_account'] THEN 'medium'
                WHEN lt IN ['visit_only','cold_market','intent_only'] THEN 'low'
                WHEN lt='blocked' THEN 'none'
                ELSE 'medium' END,
            lc.visibility=CASE
                WHEN lt='blocked'                          THEN 'internal_only'
                WHEN lt IN ['rfq_submitted','deal_ready']  THEN 'instant_alert'
                WHEN lt IN ['hot_in_market','quote_ready',
                            'buying_committee_active']     THEN 'push_notify'
                WHEN lt IN ['market_opportunity','strategic_account','active_deal','active_importer',
                            'trade_buyer_candidate','strategic_consolidation'] THEN 'priority'
                ELSE 'feed' END,
            lc.scoring_model_version=$m,
            lc.created_at=coalesce(lc.created_at,toString(datetime()))
        MERGE (fs)-[:QUALIFIES_FOR]->(lc)
        RETURN count(fs) AS c
        '''
        self._loop('LeadClassification', q, {
            'm':               self.model,
            'priority_critical': sc.priority_critical_score,
            'priority_high':     sc.priority_high_score,
            'min_rfq':           sc.min_score_rfq,
            'min_dm':            sc.min_score_decision_maker,
            'min_trade':         sc.min_score_trade,
        })

    def create_leads(self):
        sc = self.sc
        # Gap 10: add account_state, playbook_tags, opportunity_specificity
        # Gap 7: store buyer_account_key for KG-level dedup (applied by dedup_leads)
        q = '''
        MATCH (lc:LeadClassification)<-[:QUALIFIES_FOR]-(fs:FitSuppression)<-[:EVALUATED_INTO]-(e:Evidence)
        WHERE lc.scoring_model_version=$m AND lc.lead_type<>'blocked' AND NOT (lc)-[:CREATES]->(:Lead)
        OPTIONAL MATCH (e)-[:MAPS_TO]->(oh:OpportunityHypothesis)
        OPTIONAL MATCH (e)-[:SUPPORTS]->(ih:IdentityHypothesis)
        WITH lc,fs,e,oh,ih LIMIT $batch
        WITH lc,fs,e,oh,ih,
             lc.lead_type AS lt,
             fs.six_component_score AS scs,
             coalesce(ih.candidate_entity_id, 'no_entity:'+e.evidence_uid) AS buyer_account_key
        MERGE (l:Lead {lead_uid:'lead25:'+lc.classification_uid})
        SET l.leadId=l.lead_uid,
            l.lead_grain=lc.lead_grain,
            l.lead_type=lt,
            l.lead_stage=lc.lead_stage,
            l.priority=lc.priority,
            l.visibility=CASE WHEN coalesce(e.evidence_strength,0) < $ev_min
                                   OR fs.final_decision <> 'allow'
                              THEN 'internal_only' ELSE lc.visibility END,
            l.seller_visible=CASE WHEN coalesce(e.evidence_strength,0) >= $ev_min
                                       AND fs.final_decision='allow' THEN true ELSE false END,
            l.routing_eligible=CASE WHEN coalesce(e.evidence_strength,0) >= $ev_min
                                         AND fs.final_decision='allow' THEN true ELSE false END,
            l.final_decision=fs.final_decision,
            l.final_score=scs,
            l.six_component_score=scs,
            l.fit_score=fs.fit_score,
            l.behavior_score=fs.behavior_score,
            l.intent_score=fs.intent_score,
            l.trade_score=fs.trade_score,
            l.recency_score=fs.recency_score,
            l.reachability_score=fs.reachability_score,
            l.evidence_strength=e.evidence_strength,
            l.evidence_strength_raw=e.evidence_strength_raw,
            l.decay_factor=e.decay_factor,
            l.signal_age_days=e.signal_age_days,
            l.identity_confidence=coalesce(ih.confidence_score,0),
            l.buyer_account_key=buyer_account_key,
            // Gap 10 — account state from identity resolution
            l.account_state=CASE
                WHEN ih.resolution_method CONTAINS 'trademo' THEN 'known_trade_account'
                WHEN ih.resolution_method CONTAINS 'crm'
                     OR ih.resolution_method CONTAINS 'account' THEN 'known_crm_account'
                WHEN ih.status='poor_identity' THEN 'unresolved_account'
                ELSE 'candidate_account' END,
            // Gap 10 — playbook tags by lead type
            l.playbook_tags=CASE
                WHEN lt IN ['rfq_submitted','rfq_draft'] THEN 'inbound_rfq,respond_immediately'
                WHEN lt IN ['active_importer','trade_buyer_candidate'] THEN 'trade_data,proactive_outreach'
                WHEN lt IN ['buying_committee_active','strategic_account'] THEN 'contact_enrichment,multi_stakeholder'
                WHEN lt='strategic_consolidation' THEN 'consolidation,executive_outreach'
                WHEN lt IN ['deal_ready','active_deal'] THEN 'late_stage,close_now'
                ELSE 'standard_outreach' END,
            // Gap 10 — specificity of the opportunity signal
            l.opportunity_specificity=CASE
                WHEN e.rfq_exists=true THEN 'specific_product_rfq'
                WHEN e.trade_exists=true AND coalesce(e.number_of_shipments,0)>0 THEN 'repeat_importer'
                WHEN e.trade_exists=true THEN 'trade_signal'
                WHEN e.contact_exists=true THEN 'known_contact'
                ELSE 'broad_interest' END,
            l.status=coalesce(l.status, CASE WHEN coalesce(e.evidence_strength,0) < $ev_min
                                                  OR fs.final_decision <> 'allow'
                                             THEN 'internal_candidate' ELSE 'new' END),
            l.source='lead_factory_v25',
            l.scoring_model_version=$m,
            l.created_at=coalesce(l.created_at,toString(datetime()))
        MERGE (lc)-[:CREATES]->(l)
        MERGE (lt_node:LeadType {type_name:lt})
        MERGE (lg:LeadGrain {grain_type:lc.lead_grain})
        MERGE (l)-[:HAS_TYPE]->(lt_node)
        MERGE (l)-[:HAS_GRAIN]->(lg)
        FOREACH (_ IN CASE WHEN oh IS NULL THEN [] ELSE [1] END |
            MERGE (l)-[:HAS_HYPOTHESIS]->(oh))
        RETURN count(l) AS c
        '''
        self._loop('LeadCreation', q, {
            'm':      self.model,
            'ev_min': sc.seller_visible_evidence_min,
        })

    def dedup_leads(self):
        """Gap 7: KG-level deduplication — per buyer_account_key, keep only the
        highest-scoring lead seller-visible; suppress the rest to internal_only."""
        q = '''
        MATCH (l:Lead)
        WHERE l.source='lead_factory_v25'
          AND l.seller_visible=true
          AND l.buyer_account_key IS NOT NULL
        WITH l.buyer_account_key AS buyer_key, l
        ORDER BY l.final_score DESC
        WITH buyer_key, collect(l) AS leads
        WITH buyer_key, head(leads) AS best_lead, tail(leads) AS lower_leads
        UNWIND lower_leads AS l
        SET l.seller_visible=false,
            l.visibility='internal_only',
            l.routing_eligible=false,
            l.dedup_suppressed=true
        RETURN count(l) AS c
        '''
        suppressed = self.neo.run(q) or [{'c': 0}]
        info(f'dedup_leads: suppressed {suppressed[0].get("c", 0)} duplicate buyer leads')

    def route(self):
        # Gap 8: proper threshold-based routing by lead type
        q = '''
        MATCH (l:Lead)
        WHERE l.source='lead_factory_v25'
          AND coalesce(l.v25_post_processed,false)=false
          AND coalesce(l.routing_eligible,false)=true
        WITH l LIMIT $batch
        WITH l,
             CASE WHEN l.lead_type IN ['rfq_submitted','deal_ready','strategic_consolidation']
                       THEN 'immediate'
                  WHEN l.lead_type IN ['rfq_draft','hot_in_market','active_importer',
                                       'buying_committee_active','quote_ready']
                       THEN 'priority_queue'
                  WHEN l.lead_type IN ['trade_buyer_candidate','market_opportunity',
                                       'procurement_project_active','active_deal',
                                       'strategic_account']
                       THEN 'standard_queue'
                  ELSE 'research_queue' END AS routing_queue
        MERGE (seller:Seller {seller_id:'seller:default_unassigned'})
        SET seller.name='Default Unassigned Seller'
        MERGE (l)-[:ROUTES_TO]->(seller)
        // Route to named queue node based on lead type
        FOREACH (_ IN CASE WHEN routing_queue='immediate' THEN [1] ELSE [] END |
            MERGE (q:SellerQueue {queue_id:'queue:immediate'})
            SET q.priority=1, q.name='Immediate Response Queue'
            MERGE (l)-[:IN_QUEUE]->(q))
        FOREACH (_ IN CASE WHEN routing_queue='priority_queue' THEN [1] ELSE [] END |
            MERGE (q:SellerQueue {queue_id:'queue:priority'})
            SET q.priority=2, q.name='Priority Queue'
            MERGE (l)-[:IN_QUEUE]->(q))
        FOREACH (_ IN CASE WHEN routing_queue='standard_queue' THEN [1] ELSE [] END |
            MERGE (q:SellerQueue {queue_id:'queue:standard'})
            SET q.priority=3, q.name='Standard Queue'
            MERGE (l)-[:IN_QUEUE]->(q))
        FOREACH (_ IN CASE WHEN routing_queue='research_queue' THEN [1] ELSE [] END |
            MERGE (q:SellerQueue {queue_id:'queue:research'})
            SET q.priority=4, q.name='Research Queue'
            MERGE (l)-[:IN_QUEUE]->(q))
        MERGE (n:Notification {notification_id:'notify:v25:'+l.lead_uid})
        SET n.channel=CASE WHEN routing_queue='immediate' THEN 'push'
                           WHEN l.visibility IN ['instant_alert','push_notify']
                                OR l.priority IN ['critical','high'] THEN 'push'
                           ELSE 'in_app' END,
            n.template=CASE WHEN l.lead_type IN ['rfq_submitted','rfq_draft'] THEN 'rfq_alert_v25'
                            WHEN l.lead_type IN ['active_importer','trade_buyer_candidate',
                                                 'market_opportunity'] THEN 'trade_opportunity_v25'
                            WHEN l.lead_type IN ['deal_ready','strategic_consolidation']
                                 THEN 'deal_ready_v25'
                            ELSE 'lead_alert_v25' END,
            n.urgency=l.priority,
            n.routing_queue=routing_queue,
            n.created_at=coalesce(n.created_at,toString(datetime()))
        MERGE (l)-[:TRIGGERS]->(n)
        MERGE (h:LeadStatusHistory {history_id:'status:v25:'+l.lead_uid})
        SET h.status_sequence='new',
            h.transition_reasons='created_by_v25_2_factory_6component',
            h.routing_queue=routing_queue
        MERGE (l)-[:HAS_STATUS_HISTORY]->(h)
        MERGE (o:Outcome {outcome_id:'outcome:pending:v25:'+l.lead_uid})
        SET o.outcome_type='pending', o.value=0
        MERGE (l)-[:HAS_OUTCOME]->(o)
        SET l.v25_post_processed=true,
            l.post_processed_v25_at=toString(datetime()),
            l.routing_queue=routing_queue
        RETURN count(l) AS c
        '''
        self._loop('RoutingLifecycleFeedback', q)

    def factory_only(self):
        banner('V25.2 FACTORY ONLY')
        self.identity()
        self.evidence()
        self.opportunity()
        self.fit()
        self.classify()
        self.create_leads()
        self.dedup_leads()
        self.route()
        ok('Factory-only complete')
