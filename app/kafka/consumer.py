"""
KafkaEventConsumer — consumes raw CRM / web / trade events from Kafka
and triggers the appropriate KG pipeline stage for each event type.

Raw topics consumed (produced by Debezium CDC on source DBs):
  crm.rfq_submitted     — new RFQ from GoGlo platform  → LeadClassifier
  crm.shipments         — new shipment record           → TradeAggregator
  crm.buyer_sessions    — user session ended            → BuyerBehaviorAggregator
  crm.buyer_clicks      — user click event              → BuyerBehaviorAggregator
  crm.lead_updates      — lead status changed in CRM    → LeadClassifier (reclassify)

Run via:
  python3 main.py kafka-consume --config config.yaml

The consumer runs in an infinite loop (blocking).  Stop with CTRL+C.
Each event is processed individually — no micro-batching — to keep
Neo4j writes near real-time (< 2 seconds end-to-end latency).
"""

import json
import logging
import signal
import sys
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Map Kafka topic → handler method name on this class
TOPIC_HANDLERS = {
    'crm.rfq_submitted':  'handle_rfq',
    'crm.shipments':      'handle_shipment',
    'crm.buyer_sessions': 'handle_session',
    'crm.buyer_clicks':   'handle_click',
    'crm.lead_updates':   'handle_lead_update',
}


class KafkaEventConsumer:

    def __init__(self, neo, pg, settings):
        self.neo      = neo
        self.pg       = pg
        self.settings = settings
        self._running = True

        # Graceful shutdown on SIGTERM / SIGINT
        signal.signal(signal.SIGINT,  self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, *_):
        logger.info('KafkaEventConsumer: shutting down gracefully...')
        self._running = False

    def run(self):
        cfg = self.settings.kafka
        try:
            from kafka import KafkaConsumer as _KC
        except ImportError:
            logger.error('kafka-python not installed. Run: pip install kafka-python')
            sys.exit(1)

        consumer = _KC(
            *list(TOPIC_HANDLERS.keys()),
            bootstrap_servers=cfg.bootstrap_servers,
            group_id=cfg.consumer_group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda b: json.loads(b.decode('utf-8')),
            consumer_timeout_ms=1000,       # unblock poll every 1s to check _running
        )

        logger.info(f'KafkaEventConsumer: listening on topics {list(TOPIC_HANDLERS.keys())}')

        try:
            while self._running:
                for message in consumer:
                    if not self._running:
                        break
                    topic   = message.topic
                    payload = message.value or {}
                    handler = getattr(self, TOPIC_HANDLERS.get(topic, '_noop'), self._noop)
                    try:
                        handler(payload)
                    except Exception as e:
                        logger.error(f'KafkaEventConsumer: error handling {topic}: {e}')
                        # continue — don't crash the consumer on one bad message
        finally:
            consumer.close()
            logger.info('KafkaEventConsumer: stopped.')

    # ── handlers ───────────────────────────────────────────────────────────────

    def handle_rfq(self, payload: dict):
        """New RFQ → classify lead immediately."""
        logger.info(f'handle_rfq: rfq_id={payload.get("rfq_id")}')
        from app.features.lead_classifier import run_single_rfq
        run_single_rfq(self.neo, payload)

    def handle_shipment(self, payload: dict):
        """New shipment → update TradeRelationship + re-run stress detection."""
        logger.info(f'handle_shipment: buyer={payload.get("buyer_id")} hs={payload.get("hs_code")}')
        from app.features.trade_aggregator import TradeAggregator
        from app.engines.switch_lead_engine import SwitchLeadEngine

        # Update just the affected TradeRelationship
        TradeAggregator(self.neo, self.pg, self.settings).process_single_shipment(payload)

        # Re-run stress detection on this relationship only
        rel_id = payload.get('rel_id') or payload.get('relationship_id')
        if rel_id:
            SwitchLeadEngine(self.neo, self.pg, self.settings).detect_stress_for_rel(rel_id)

    def handle_session(self, payload: dict):
        """Browser session ended → update BuyerProfile.
        Routes page_event_trackings and page_visits with commercial button_identity
        as click signals (correction per Saif: commercial actions live in
        click_tracking + page_event_trackings, not page_visits.page_name).
        """
        source_table = payload.get("_source_table", "")
        user_id = payload.get("user_id") or payload.get("userId") or ""

        # page_event_trackings with a button_identity → treat as commercial click
        if source_table == "crm.page_event_trackings" and payload.get("button_identity"):
            logger.info(f'handle_session(page_event): user={user_id} btn={payload.get("button_identity")}')
            from app.features.buyer_behavior_aggregator import record_click_signal
            record_click_signal(self.neo, payload)
            return

        # page_visits with a commercial button_identity (element_name) → click signal
        if source_table == "crm.page_visits" and payload.get("element_name"):
            logger.info(f'handle_session(page_visit click): user={user_id} elem={payload.get("element_name")}')
            from app.features.buyer_behavior_aggregator import record_click_signal
            # map element_name → button_identity for weight lookup
            payload.setdefault("button_identity", payload.get("element_name", ""))
            record_click_signal(self.neo, payload)
            # still fall through to update session count below

        logger.info(f'handle_session: user={user_id} table={source_table}')
        from app.features.buyer_behavior_aggregator import update_single_profile
        update_single_profile(self.neo, self.pg, payload)

    def handle_click(self, payload: dict):
        """Click event → increment intent signal on BuyerProfile."""
        from app.features.buyer_behavior_aggregator import record_click_signal
        record_click_signal(self.neo, payload)

    def handle_lead_update(self, payload: dict):
        """Lead status changed in CRM → reclassify. Routes by source table."""
        source_table = payload.get("_source_table", "crm.crm_leads")
        logger.info(f'handle_lead_update: table={source_table} id={payload.get("id")}')

        if source_table == "crm.deals":
            self._handle_deal(payload)
        elif source_table == "crm.account_risk_flags":
            self._handle_risk_flag(payload)
        elif source_table in ("crm.crm_emails", "crm.auto_quote_email_events"):
            self._handle_email_event(payload)
        elif source_table == "crm.account_identity":
            self._handle_identity_resolved(payload)
        elif source_table == "crm.lead_master":
            self._handle_lead_master(payload)
        else:
            from app.features.lead_classifier import reclassify_lead
            reclassify_lead(self.neo, payload)

    def _handle_deal(self, payload: dict):
        """New/updated deal → create or update engaged_account or reactivation_candidate lead."""
        deal_id = str(payload.get("id") or "")
        if not deal_id:
            return
        amount = float(payload.get("amount") or 0)
        deal_name = (payload.get("deal_name") or "")[:200]
        src = f"deal:{deal_id}"
        # If the deal is being updated and is old → reactivation_candidate
        # If it's new → engaged_account
        lead_type = "engaged_account"
        score = 70

        self.neo.run("""
            MERGE (l:Lead {source_ref: $src})
            ON CREATE SET
                l.lead_uid         = 'classified:' + $lt + ':' + $src,
                l.lead_type        = $lt,
                l.score_final      = $score,
                l.visibility_level = 'priority',
                l.source           = 'goglo_crm',
                l.source_ref       = $src,
                l.synced_from_sql  = true,
                l.deal_name        = $deal_name,
                l.deal_amount      = $amount,
                l.playbook_tags    = ['account_warming', 'contact_qualification'],
                l.classified_at    = $now,
                l.created_at       = $now
            ON MATCH SET
                l.lead_type    = $lt,
                l.score_final  = $score,
                l.classified_at = $now
        """, {"lt": lead_type, "score": score, "src": src,
              "deal_name": deal_name, "amount": amount,
              "now": datetime.now(timezone.utc).isoformat()})
        logger.info(f'_handle_deal: upserted Lead {src} as {lead_type}')


    def _handle_identity_resolved(self, payload: dict):
        """Identity resolved in CRM → annotate matching Lead/BuyerProfile nodes."""
        account_uid = str(payload.get("account_uid") or payload.get("id") or "")
        confidence  = float(payload.get("confidence_score") or 0)
        org_name    = str(payload.get("organization_name") or "")
        if not account_uid:
            return
        self.neo.run("""
            MATCH (l:Lead)
            WHERE l.account_uid = $uid OR l.source_ref CONTAINS $uid
            SET l.identity_confidence = $conf,
                l.org_name_resolved   = $org
        """, {"uid": account_uid, "conf": confidence, "org": org_name})
        logger.info(f'_handle_identity_resolved: account={account_uid} confidence={confidence}')

    def _handle_lead_master(self, payload: dict):
        """CRM lead_master updated → sync status/score to matching KG Lead node."""
        lead_uid  = str(payload.get("lead_uid") or "")
        status    = str(payload.get("status") or "")
        score     = float(payload.get("score_final") or 0)
        suppressed = bool(payload.get("is_suppressed") or False)
        if not lead_uid:
            return
        self.neo.run("""
            MATCH (l:Lead {lead_uid: $uid})
            SET l.crm_status   = $status,
                l.crm_score    = $score,
                l.is_suppressed = $supp
        """, {"uid": lead_uid, "status": status, "score": score, "supp": suppressed})
        logger.info(f'_handle_lead_master: lead={lead_uid} status={status} score={score}')

    def _handle_risk_flag(self, payload: dict):
        """Account risk flag → suppress any matching leads from routing."""
        account_uid = str(payload.get("account_uid") or "")
        risk_type   = str(payload.get("risk_type") or "unknown")
        flag_id     = str(payload.get("id") or "")
        if not account_uid and not flag_id:
            return

        src = f"risk_flag:{flag_id}"
        # Create a suppressed_noise lead for this risk flag
        self.neo.run("""
            MERGE (l:Lead {source_ref: $src})
            ON CREATE SET
                l.lead_uid          = 'classified:suppressed_noise:' + $src,
                l.lead_type         = 'suppressed_noise',
                l.score_final       = 10,
                l.visibility_level  = 'count_only',
                l.source            = 'crm_risk',
                l.source_ref        = $src,
                l.synced_from_sql   = true,
                l.account_uid       = $account_uid,
                l.suppressed        = true,
                l.suppression_reason = $risk_type,
                l.distribution_status = 'suppressed',
                l.seller_visible    = false,
                l.classified_at     = $now,
                l.created_at        = $now
        """, {"src": src, "account_uid": account_uid,
              "risk_type": risk_type,
              "now": datetime.now(timezone.utc).isoformat()})

        # Also suppress any existing leads for this account
        if account_uid:
            self.neo.run("""
                MATCH (l:Lead)
                WHERE l.account_uid = $account_uid
                  AND l.lead_type <> 'suppressed_noise'
                  AND coalesce(l.suppressed, false) = false
                SET l.suppressed        = true,
                    l.suppression_reason = $risk_type,
                    l.distribution_status = 'suppressed',
                    l.seller_visible    = false
            """, {"account_uid": account_uid, "risk_type": risk_type})
        logger.info(f'_handle_risk_flag: suppressed account {account_uid} ({risk_type})')

    def _handle_email_event(self, payload: dict):
        """Email open/click → update engagement score on related Lead."""
        source_table = payload.get("_source_table", "")
        event_type   = str(payload.get("event_type") or payload.get("status") or "")
        account_id   = str(payload.get("account_id") or payload.get("auto_quote_email_id") or "")

        if not account_id:
            return

        # Boost score for opens/clicks, penalise bounces
        if event_type in ("open", "opened", "click", "clicked"):
            score_delta = 5
        elif event_type in ("bounce", "bounced", "spam", "unsubscribed"):
            score_delta = -10
        else:
            return

        self.neo.run("""
            MATCH (l:Lead)
            WHERE l.account_id = $account_id
              OR l.contact_id = $account_id
            SET l.score_final = CASE
                WHEN l.score_final + $delta < 10  THEN 10
                WHEN l.score_final + $delta > 100 THEN 100
                ELSE l.score_final + $delta
            END,
            l.last_email_event = $event_type,
            l.classified_at    = $now
        """, {"account_id": account_id, "delta": score_delta,
              "event_type": event_type,
              "now": datetime.now(timezone.utc).isoformat()})
        logger.info(f'_handle_email_event: score {score_delta:+d} for account {account_id} ({event_type})')

    def _noop(self, payload: dict):
        logger.warning(f'KafkaEventConsumer: no handler for payload: {payload}')
