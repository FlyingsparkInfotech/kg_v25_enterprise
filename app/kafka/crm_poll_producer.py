"""
CRMPollProducer — replaces Debezium CDC when REPLICATION CLIENT is unavailable.

Polls 19 CRM / goglo_staging tables every 30 seconds via plain SELECT.
Uses auto-increment id watermarks stored in a JSON file to detect new rows.
Publishes raw row JSON to the appropriate Kafka topics.

READ-ONLY: only SELECT queries against the live CRM MySQL tunnel (localhost:3307).

Topics published:
  crm.rfq_submitted   ← crm.rfqs, crm.quotation, goglo_staging.enquiries
  crm.lead_updates    ← crm.crm_leads, crm.lead_master, crm.crm_emails,
                         crm.auto_quote_email_events, crm.account_risk_flags,
                         crm.deals, crm.account_identity
  crm.buyer_sessions  ← crm.page_visits, crm.scroll_depths, crm.session_engagements,
                         crm.trade_relationship, crm.page_event_trackings,
                         goglo_staging.tracking_sessions, goglo_staging.tracking_page_views
  crm.buyer_clicks    ← crm.click_tracking, goglo_staging.tracking_click_events

Run via:
  python3 main.py kafka-poll --config config.yaml
"""

import json
import logging
import os
import signal
import time
from datetime import datetime, timezone

import pymysql
import pymysql.cursors

logger = logging.getLogger(__name__)

WATERMARK_FILE = "/opt/.debug/kg_v25_enterprise/crm_poll_watermarks.json"
POLL_INTERVAL  = 30    # seconds between full poll cycles
BATCH_SIZE     = 500   # max rows per table per poll

# (database, table, watermark_col, wm_type, kafka_topic)
# wm_type: "int"  → watermark is an integer (auto-increment id), starts at 0
#          "time" → watermark is an ISO timestamp string, starts at "1970-01-01 00:00:00"
TABLE_MAP = [
    # ── RFQ / Quotation events ────────────────────────────────────────────────
    ("crm",           "rfqs",                    "id",         "int",  "crm.rfq_submitted"),
    ("crm",           "quotation",               "id",         "int",  "crm.rfq_submitted"),
    ("goglo_staging", "enquiries",               "id",         "int",  "crm.rfq_submitted"),

    # ── Lead / account updates ────────────────────────────────────────────────
    ("crm",           "crm_leads",               "id",         "int",  "crm.lead_updates"),
    ("crm",           "lead_master",             "created_at", "time", "crm.lead_updates"),
    ("crm",           "crm_emails",              "id",         "int",  "crm.lead_updates"),
    ("crm",           "auto_quote_email_events", "id",         "int",  "crm.lead_updates"),
    ("crm",           "account_risk_flags",      "id",         "int",  "crm.lead_updates"),
    ("crm",           "deals",                   "id",         "int",  "crm.lead_updates"),
    ("crm",           "account_identity",        "created_at", "time", "crm.lead_updates"),

    # ── Buyer session / page events ───────────────────────────────────────────
    ("crm",           "trade_relationship",      "id",         "int",  "crm.buyer_sessions"),
    ("crm",           "page_visits",             "id",         "int",  "crm.buyer_sessions"),
    ("crm",           "scroll_depths",           "id",         "int",  "crm.buyer_sessions"),
    ("crm",           "session_engagements",     "id",         "int",  "crm.buyer_sessions"),
    ("crm",           "page_event_trackings",    "id",         "int",  "crm.buyer_sessions"),
    ("goglo_staging", "tracking_sessions",       "id",         "int",  "crm.buyer_sessions"),
    ("goglo_staging", "tracking_page_views",     "id",         "int",  "crm.buyer_sessions"),

    # ── Click / intent signals ────────────────────────────────────────────────
    ("crm",           "click_tracking",          "id",         "int",  "crm.buyer_clicks"),
    ("goglo_staging", "tracking_click_events",   "id",         "int",  "crm.buyer_clicks"),
]

# Postgres (goglo_etl) tables polled for trade signal streaming.
# schema, table, watermark_col, wm_type, kafka_topic
PG_TABLE_MAP = [
    ('raw', 'trademo_shipment_bl', 'bl_key', 'int', 'crm.shipments'),
]
PG_POLL_INTERVAL = 60   # seconds between Postgres poll cycles



def _to_json_safe(v):
    """Convert MySQL types that are not JSON-serialisable."""
    if v is None:
        return None
    if hasattr(v, "isoformat"):      # datetime / date / time
        return v.isoformat()
    try:
        from decimal import Decimal
        if isinstance(v, Decimal):
            return float(v)
    except ImportError:
        pass
    return v


class CRMPollProducer:

    def __init__(self, settings):
        self.settings  = settings
        self._running  = True
        self._producer = None
        self._conn     = None
        self._wm       = self._load_watermarks()

        signal.signal(signal.SIGINT,  self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, *_):
        logger.info("CRMPollProducer: shutdown signal received")
        self._running = False

    # ── Watermarks ────────────────────────────────────────────────────────────

    def _load_watermarks(self) -> dict:
        try:
            with open(WATERMARK_FILE) as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _save_watermarks(self):
        wm_dir = os.path.dirname(WATERMARK_FILE)
        if wm_dir:
            os.makedirs(wm_dir, exist_ok=True)
        with open(WATERMARK_FILE, "w") as f:
            json.dump(self._wm, f, indent=2, default=str)

    def _get_wm(self, db: str, table: str, wm_type: str):
        default = 0 if wm_type == "int" else "1970-01-01 00:00:00"
        return self._wm.get(f"{db}.{table}", default)

    def _set_wm(self, db: str, table: str, value):
        self._wm[f"{db}.{table}"] = value

    # ── MySQL connection ──────────────────────────────────────────────────────

    def _get_conn(self):
        if self._conn:
            try:
                self._conn.ping(reconnect=True)
                return self._conn
            except Exception:
                self._conn = None

        cfg = self.settings.mysql_crm
        self._conn = pymysql.connect(
            host=cfg.host,
            port=cfg.port,
            user=cfg.user,
            password=cfg.password,
            charset="utf8mb4",
            connect_timeout=10,
            read_timeout=30,
            cursorclass=pymysql.cursors.DictCursor,
        )
        logger.info(f"CRMPollProducer: connected to MySQL {cfg.host}:{cfg.port}")
        return self._conn

    def _close_conn(self):
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    # ── Kafka producer ────────────────────────────────────────────────────────

    def _get_producer(self):
        if self._producer:
            return self._producer
        from kafka import KafkaProducer
        cfg = self.settings.kafka
        self._producer = KafkaProducer(
            bootstrap_servers=cfg.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
            max_block_ms=10_000,
        )
        logger.info(f"CRMPollProducer: Kafka producer connected to {cfg.bootstrap_servers}")
        return self._producer

    # ── Poll one table ────────────────────────────────────────────────────────

    def _poll_table(self, db: str, table: str, wm_col: str, wm_type: str, topic: str) -> int:
        last_wm  = self._get_wm(db, table, wm_type)
        conn     = self._get_conn()
        new_wm   = last_wm

        with conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM `{db}`.`{table}` "
                f"WHERE `{wm_col}` > %s "
                f"ORDER BY `{wm_col}` ASC "
                f"LIMIT %s",
                (last_wm, BATCH_SIZE),
            )
            rows = cur.fetchall()

        if not rows:
            return 0

        producer  = self._get_producer()
        published = 0
        now_iso   = datetime.now(timezone.utc).isoformat()

        for row in rows:
            payload = {k: _to_json_safe(v) for k, v in row.items()}
            payload["_source_table"] = f"{db}.{table}"
            payload["_polled_at"]    = now_iso

            # Compute new watermark value
            wm_val = row.get(wm_col)
            if wm_type == "int":
                try:
                    wm_val = int(wm_val or 0)
                except (TypeError, ValueError):
                    wm_val = 0
                if wm_val > new_wm:
                    new_wm = wm_val
                key = f"{db}.{table}:{wm_val}"
            else:
                # datetime / timestamp
                wm_str = wm_val.isoformat() if hasattr(wm_val, "isoformat") else str(wm_val or "")
                if wm_str > str(new_wm):
                    new_wm = wm_str
                key = f"{db}.{table}:{wm_str}"

            try:
                producer.send(topic, key=key, value=payload)
                published += 1
            except Exception as e:
                logger.warning(f"CRMPollProducer: send to {topic} failed: {e}")

        try:
            producer.flush(timeout=10)
        except Exception as e:
            logger.warning(f"CRMPollProducer: flush failed: {e}")

        self._set_wm(db, table, new_wm)
        self._save_watermarks()

        logger.info(
            f"CRMPollProducer: {db}.{table} → {topic}: "
            f"{published}/{len(rows)} rows  (watermark={new_wm})"
        )
        return published

    # ── Main loop ─────────────────────────────────────────────────────────────


    # ── Postgres (Trademo) polling ────────────────────────────────────────────

    def _get_pg_conn(self):
        """Lazy-init psycopg2 connection to goglo_etl Postgres."""
        if getattr(self, '_pg_conn', None):
            try:
                self._pg_conn.cursor().execute('SELECT 1')
                return self._pg_conn
            except Exception:
                pass
        import psycopg2, psycopg2.extras
        pg_cfg = getattr(self.settings, 'postgres', None)
        host     = getattr(pg_cfg, 'host',     'localhost')  if pg_cfg else 'localhost'
        port     = getattr(pg_cfg, 'port',     5432)         if pg_cfg else 5432
        dbname   = getattr(pg_cfg, 'database', 'goglo_etl')  if pg_cfg else 'goglo_etl'
        user     = getattr(pg_cfg, 'user',     'etl_user')   if pg_cfg else 'etl_user'
        password = getattr(pg_cfg, 'password', 'EtlCozmo@2026!') if pg_cfg else 'EtlCozmo@2026!'
        self._pg_conn = psycopg2.connect(
            host=host, port=port, dbname=dbname, user=user, password=password,
            connect_timeout=10,
        )
        self._pg_conn.autocommit = True
        logger.info(f'CRMPollProducer: connected to Postgres {host}:{port}/{dbname}')
        return self._pg_conn

    def _poll_pg_table(self, schema: str, table: str,
                       wm_col: str, wm_type: str, topic: str) -> int:
        """Poll one Postgres table, publish new rows to Kafka. Returns row count."""
        last_wm  = self._get_wm(schema, table, wm_type)
        now_iso  = datetime.utcnow().isoformat()
        producer = self._get_producer()

        try:
            conn = self._get_pg_conn()
            cur  = conn.cursor(cursor_factory=__import__('psycopg2.extras', fromlist=['RealDictCursor']).RealDictCursor)
            if wm_type == 'int':
                cur.execute(
                    f'SELECT * FROM "{schema}"."{table}" WHERE "{wm_col}" > %s ORDER BY "{wm_col}" LIMIT %s',
                    (int(last_wm), self.batch_size),
                )
            else:
                cur.execute(
                    f'SELECT * FROM "{schema}"."{table}" WHERE "{wm_col}" > %s ORDER BY "{wm_col}" LIMIT %s',
                    (str(last_wm), self.batch_size),
                )
            rows = cur.fetchall()
        except Exception as e:
            logger.error(f'CRMPollProducer: Postgres poll {schema}.{table} failed: {e}')
            self._pg_conn = None   # force reconnect next time
            return 0

        if not rows:
            return 0

        new_wm   = last_wm
        sent     = 0
        for row in rows:
            payload = dict(row)
            wm_val  = payload.get(wm_col)

            # Serialize any non-JSON-native types
            for k, v in list(payload.items()):
                if hasattr(v, 'isoformat'):          # date/datetime
                    payload[k] = v.isoformat()
                elif isinstance(v, (list, set)):     # postgres arrays
                    payload[k] = list(v)

            payload['_source_table'] = f'{schema}.{table}'
            payload['_polled_at']    = now_iso

            key = f'{schema}.{table}:{wm_val}'
            try:
                producer.send(topic, key=key.encode(), value=payload)
                sent += 1
                if wm_type == 'int' and wm_val is not None:
                    new_wm = max(int(new_wm or 0), int(wm_val))
                elif wm_val is not None:
                    new_wm = max(str(new_wm), str(wm_val))
            except Exception as e:
                logger.warning(f'CRMPollProducer: Postgres send to {topic} failed: {e}')

        try:
            producer.flush(timeout=10)
        except Exception:
            pass

        self._set_wm(schema, table, new_wm)
        self._save_wm()
        logger.info(f'CRMPollProducer: {schema}.{table} → {topic}: {sent} new rows (wm={new_wm})')
        return sent

    def _run_pg_poll_cycle(self):
        """Run one poll cycle over all Postgres tables."""
        total = 0
        for (schema, table, wm_col, wm_type, topic) in PG_TABLE_MAP:
            try:
                n = self._poll_pg_table(schema, table, wm_col, wm_type, topic)
                total += n
            except Exception as e:
                logger.error(f'CRMPollProducer: error in Postgres poll {schema}.{table}: {e}')
        return total

    def run(self):
        logger.info("CRMPollProducer: starting — poll interval=%ds, tables=%d", POLL_INTERVAL, len(TABLE_MAP))

        while self._running:
            cycle_total = 0
            for (db, table, wm_col, wm_type, topic) in TABLE_MAP:
                if not self._running:
                    break
                try:
                    n = self._poll_table(db, table, wm_col, wm_type, topic)
                    cycle_total += n
                except Exception as e:
                    logger.error(f"CRMPollProducer: error polling {db}.{table}: {e}")
                    self._close_conn()   # force reconnect on next cycle

            if cycle_total:
                logger.info(f"CRMPollProducer: cycle complete — {cycle_total} rows published")


            # Postgres / Trademo poll (runs every PG_POLL_INTERVAL seconds)
            import time as _time
            _now_ts = _time.time()
            if _now_ts - getattr(self, "_last_pg_poll", 0) >= PG_POLL_INTERVAL:
                try:
                    self._run_pg_poll_cycle()
                except Exception as _e:
                    logger.error(f"CRMPollProducer: Postgres poll cycle error: {_e}")
                self._last_pg_poll = _time.time()

            # Interruptible sleep
            for _ in range(POLL_INTERVAL):
                if not self._running:
                    break
                time.sleep(1)

        # Graceful cleanup
        if self._producer:
            try:
                self._producer.flush(timeout=5)
                self._producer.close()
            except Exception:
                pass
        self._close_conn()
        logger.info("CRMPollProducer: stopped.")
