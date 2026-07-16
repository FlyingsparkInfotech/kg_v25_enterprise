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
        """Browser session ended → update BuyerProfile."""
        logger.info(f'handle_session: user={payload.get("user_id")} org={payload.get("org_id")}')
        from app.features.buyer_behavior_aggregator import update_single_profile
        update_single_profile(self.neo, self.pg, payload)

    def handle_click(self, payload: dict):
        """Click event → increment intent signal on BuyerProfile."""
        from app.features.buyer_behavior_aggregator import record_click_signal
        record_click_signal(self.neo, payload)

    def handle_lead_update(self, payload: dict):
        """Lead status changed in CRM → reclassify."""
        logger.info(f'handle_lead_update: lead_id={payload.get("lead_id")}')
        from app.features.lead_classifier import reclassify_lead
        reclassify_lead(self.neo, payload)

    def _noop(self, payload: dict):
        logger.warning(f'KafkaEventConsumer: no handler for payload: {payload}')
