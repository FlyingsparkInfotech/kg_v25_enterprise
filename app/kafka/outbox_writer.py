"""
OutboxWriter — writes KG output events to the kg_outbox table in Postgres.

This implements the Transactional Outbox Pattern:
  1. Pipeline writes lead to Neo4j  (in create_switch_leads)
  2. Pipeline ALSO writes same data to kg_outbox in Postgres (here)
  3. Debezium monitors kg_outbox via CDC
  4. Debezium publishes to Kafka topic: kg.switch_leads
  5. JDBC Sink Connector reads Kafka → writes to MySQL crm2 kg_output tables
  6. Admin UI reads from crm2

Why outbox instead of writing to Kafka directly?
  If the Kafka producer fails after writing to Neo4j but before sending to Kafka,
  the event is lost. The outbox is written in the same Python transaction as the
  Neo4j write — so either both succeed or neither does. Debezium then reliably
  picks it up via CDC, guaranteeing at-least-once delivery.

DDL to create the table (run once):
  CREATE TABLE kg_outbox (
      event_id      VARCHAR(36)  PRIMARY KEY,
      event_type    VARCHAR(60)  NOT NULL,
      aggregate_id  VARCHAR(64)  NOT NULL,
      payload       JSON         NOT NULL,
      created_at    TIMESTAMP    NOT NULL DEFAULT NOW(),
      processed     BOOLEAN      DEFAULT FALSE,
      INDEX idx_processed (processed),
      INDEX idx_created   (created_at)
  );
"""

import json
import logging
import uuid
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')


class OutboxWriter:

    def __init__(self, pg):
        """
        pg — PostgresClient instance (already connected to the 'develop' Postgres DB).
        The kg_outbox table lives in the 'etl' schema: etl.kg_outbox
        """
        self.pg = pg
        self._table_ensured = False

    def ensure_table(self):
        """Create kg_outbox table if it doesn't exist. Safe to call multiple times."""
        if self._table_ensured:
            return
        try:
            self.pg.execute("""
                CREATE TABLE IF NOT EXISTS etl.kg_outbox (
                    event_id     VARCHAR(36)  NOT NULL PRIMARY KEY,
                    event_type   VARCHAR(60)  NOT NULL,
                    aggregate_id VARCHAR(64)  NOT NULL,
                    payload      TEXT         NOT NULL,
                    created_at   TIMESTAMP    NOT NULL DEFAULT NOW(),
                    processed    BOOLEAN      NOT NULL DEFAULT FALSE
                )
            """)
            self.pg.execute("""
                CREATE INDEX IF NOT EXISTS idx_kg_outbox_processed
                ON etl.kg_outbox (processed)
            """)
            self.pg.execute("""
                CREATE INDEX IF NOT EXISTS idx_kg_outbox_created
                ON etl.kg_outbox (created_at)
            """)
            self._table_ensured = True
            logger.info('OutboxWriter: etl.kg_outbox table ready')
        except Exception as e:
            logger.warning(f'OutboxWriter.ensure_table: {e}')

    # ── write methods ──────────────────────────────────────────────────────────

    def write_switch_lead(self, lead: dict):
        """Write a SwitchLead event to the outbox."""
        self._insert(
            event_type='switch_lead.upserted',
            aggregate_id=lead.get('lead_id', ''),
            payload={
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
                'health_score':              lead.get('health_score'),
                'health_status':             lead.get('health_status', ''),
                'stress_reason':             lead.get('stress_reason', ''),
                'match_score':               lead.get('match_score', 0),
                'buyer_monthly_volume':      lead.get('buyer_monthly_volume', 0),
                'contact_name':              lead.get('contact_name', ''),
                'contact_title':             lead.get('contact_title', ''),
                'contact_email':             lead.get('contact_email', ''),
                'recommended_action':        lead.get('recommended_action', ''),
                'status':                    lead.get('status', 'new'),
                'created_at':               lead.get('created_at', _now()),
                'synced_at':                _now(),
            }
        )

    def write_switch_leads_batch(self, leads: list[dict]) -> int:
        """Write multiple SwitchLead events. Returns count written."""
        written = 0
        for lead in leads:
            try:
                self.write_switch_lead(lead)
                written += 1
            except Exception as e:
                logger.error(f'OutboxWriter.write_switch_leads_batch: lead_id={lead.get("lead_id")} — {e}')
        return written

    def write_trade_relationship(self, rel: dict):
        self._insert(
            event_type='trade_relationship.health_updated',
            aggregate_id=rel.get('rel_id', ''),
            payload={
                'rel_id':                   rel.get('rel_id', ''),
                'buyer_org_id':             rel.get('buyer_id', ''),
                'buyer_name':               rel.get('buyer_name', ''),
                'supplier_org_id':          rel.get('supplier_id', ''),
                'supplier_name':            rel.get('supplier_name', ''),
                'hs_code':                  rel.get('hs_code', ''),
                'health_score':             rel.get('health_score'),
                'health_status':            rel.get('health_status', ''),
                'baseline_monthly_qty':     rel.get('baseline_avg_qty', 0),
                'total_shipments':          rel.get('total_shipments', 0),
                'relationship_age_months':  rel.get('relationship_age_months', 0),
                'last_shipment_date':       rel.get('last_shipment_date', ''),
                'synced_at':               _now(),
            }
        )

    def write_pipeline_run(self, run_summary: dict):
        self._insert(
            event_type='pipeline.run_completed',
            aggregate_id=run_summary.get('run_id', str(uuid.uuid4())),
            payload={**run_summary, 'synced_at': _now()},
        )

    # ── internal ───────────────────────────────────────────────────────────────

    def _insert(self, event_type: str, aggregate_id: str, payload: dict):
        self.ensure_table()
        event_id = str(uuid.uuid4())
        try:
            self.pg.execute("""
                INSERT INTO etl.kg_outbox
                    (event_id, event_type, aggregate_id, payload, created_at, processed)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (aggregate_id) DO UPDATE
                    SET payload    = EXCLUDED.payload,
                        event_type = EXCLUDED.event_type,
                        created_at = EXCLUDED.created_at,
                        processed  = FALSE
            """, [event_id, event_type, aggregate_id,
                  json.dumps(payload, default=str), _now(), False])
        except Exception as e:
            logger.error(f'OutboxWriter._insert({event_type}, {aggregate_id}): {e}')
            raise
