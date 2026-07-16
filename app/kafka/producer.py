"""
KafkaLeadProducer — publishes KG output events to Kafka topics.

Called from SwitchLeadEngine Stage 6 (create_switch_leads) after every
Neo4j write.  The Kafka JDBC Sink Connector then reads these topics and
writes to MySQL crm2 kg_output tables for the admin UI.

Topics published:
  kg.switch_leads          — one message per SwitchLead created/updated
  kg.trade_relationships   — one message per TradeRelationship health update
  kg.buyer_profiles        — one message per BuyerProfile score update
  kg.pipeline_runs         — one message per pipeline run summary

Serialisation: JSON (UTF-8).  Each message key = entity primary ID.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class KafkaLeadProducer:

    TOPICS = {
        'switch_leads':        'kg.switch_leads',
        'trade_relationships': 'kg.trade_relationships',
        'buyer_profiles':      'kg.buyer_profiles',
        'pipeline_runs':       'kg.pipeline_runs',
    }

    def __init__(self, settings):
        self._settings = settings
        self._producer  = None
        self._enabled   = getattr(settings, 'kafka', None) is not None and \
                          getattr(settings.kafka, 'enabled', False)

    def _get_producer(self):
        if self._producer is not None:
            return self._producer
        if not self._enabled:
            return None
        try:
            from kafka import KafkaProducer
            cfg = self._settings.kafka
            self._producer = KafkaProducer(
                bootstrap_servers=cfg.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',                 # wait for all replicas to confirm
                retries=3,
                max_block_ms=10_000,        # fail fast if Kafka is unreachable
            )
            logger.info(f'KafkaLeadProducer connected to {cfg.bootstrap_servers}')
        except Exception as e:
            logger.warning(f'KafkaLeadProducer: could not connect — {e}. Running without Kafka.')
            self._producer = None
        return self._producer

    # ── public publish methods ─────────────────────────────────────────────────

    def publish_switch_lead(self, lead: dict) -> bool:
        """Publish a single SwitchLead event. Returns True if sent."""
        payload = {
            'event_type':    'switch_lead.upserted',
            'event_time':    _now_iso(),
            'lead_id':                   lead.get('lead_id', ''),
            'buyer_name':                lead.get('buyer_name', ''),
            'buyer_org_id':              lead.get('buyer_org_id', ''),
            'buyer_country':             lead.get('buyer_country', ''),
            'buyer_industry':            lead.get('buyer_industry', ''),
            'current_supplier_name':     lead.get('existing_supplier_name', ''),
            'recommended_supplier_name': lead.get('candidate_supplier_name', ''),
            'recommended_supplier_id':   lead.get('candidate_supplier_org_id', ''),
            'hs_code':                   lead.get('hs_code', ''),
            'final_lead_score':          lead.get('final_lead_score', 0),
            'switch_probability':        lead.get('switch_probability', 0),
            'lead_priority':             lead.get('lead_priority', 'low'),
            'stress_reason':             lead.get('stress_reason', ''),
            'match_score':               lead.get('match_score', 0),
            'buyer_monthly_volume':      lead.get('buyer_monthly_volume', 0),
            'contact_name':              lead.get('contact_name', ''),
            'contact_title':             lead.get('contact_title', ''),
            'contact_email':             lead.get('contact_email', ''),
            'recommended_action':        lead.get('recommended_action', ''),
            'status':                    lead.get('status', 'new'),
            'created_at':                lead.get('created_at', _now_iso()),
        }
        return self._send(self.TOPICS['switch_leads'], lead.get('lead_id'), payload)

    def publish_switch_leads_batch(self, leads: list[dict]) -> int:
        """Publish a batch of SwitchLead events. Returns count sent."""
        sent = 0
        for lead in leads:
            if self.publish_switch_lead(lead):
                sent += 1
        producer = self._get_producer()
        if producer:
            producer.flush()
        return sent

    def publish_trade_relationship(self, rel: dict) -> bool:
        payload = {
            'event_type':   'trade_relationship.health_updated',
            'event_time':   _now_iso(),
            'rel_id':                   rel.get('rel_id', ''),
            'buyer_org_id':             rel.get('buyer_org_id', ''),
            'buyer_name':               rel.get('buyer_name', ''),
            'supplier_org_id':          rel.get('supplier_org_id', ''),
            'supplier_name':            rel.get('supplier_name', ''),
            'hs_code':                  rel.get('hs_code', ''),
            'health_score':             rel.get('health_score'),
            'health_status':            rel.get('health_status', ''),
            'baseline_monthly_qty':     rel.get('baseline_avg_qty', 0),
            'total_shipments':          rel.get('total_shipments', 0),
            'relationship_age_months':  rel.get('relationship_age_months', 0),
            'last_shipment_date':       rel.get('last_shipment_date', ''),
            'updated_at':               rel.get('updated_at', _now_iso()),
        }
        return self._send(self.TOPICS['trade_relationships'], rel.get('rel_id'), payload)

    def publish_buyer_profile(self, profile: dict) -> bool:
        payload = {
            'event_type':          'buyer_profile.score_updated',
            'event_time':          _now_iso(),
            'user_id':             profile.get('user_id', ''),
            'org_id':              profile.get('org_id', ''),
            'behavioral_score':    profile.get('behavioral_score', 0),
            'session_count':       profile.get('session_count', 0),
            'click_intent_score':  profile.get('click_intent_score', 0),
            'scroll_depth_avg':    profile.get('scroll_depth_avg', 0),
            'search_count':        profile.get('search_count', 0),
            'last_active_date':    profile.get('last_active_date', ''),
            'updated_at':          _now_iso(),
        }
        return self._send(self.TOPICS['buyer_profiles'], profile.get('user_id'), payload)

    def publish_pipeline_run(self, run_summary: dict) -> bool:
        payload = {
            'event_type':           'pipeline.run_completed',
            'event_time':           _now_iso(),
            **run_summary,
        }
        return self._send(self.TOPICS['pipeline_runs'], run_summary.get('run_id'), payload)

    # ── internal ───────────────────────────────────────────────────────────────

    def _send(self, topic: str, key: str, payload: dict) -> bool:
        producer = self._get_producer()
        if producer is None:
            return False
        try:
            producer.send(topic, key=key, value=payload)
            return True
        except Exception as e:
            logger.warning(f'KafkaLeadProducer._send({topic}): {e}')
            return False

    def close(self):
        if self._producer:
            try:
                self._producer.flush(timeout=5)
                self._producer.close()
            except Exception:
                pass
            self._producer = None
