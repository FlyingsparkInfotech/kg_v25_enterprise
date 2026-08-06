# Database helper — psycopg2 connection + watermark tracking
import psycopg2
import psycopg2.extras
from datetime import datetime
from config import PG


def get_conn():
    return psycopg2.connect(**PG)


def upsert_watermark(conn, feed_name: str, status: str, records: int = 0, error: str = None):
    now = datetime.utcnow()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO public.etl_watermarks (feed_name, last_run_at, last_success_at, records_loaded, status, error_msg)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (feed_name) DO UPDATE SET
                last_run_at     = EXCLUDED.last_run_at,
                last_success_at = CASE WHEN EXCLUDED.status = 'success' THEN EXCLUDED.last_run_at
                                       ELSE etl_watermarks.last_success_at END,
                records_loaded  = EXCLUDED.records_loaded,
                status          = EXCLUDED.status,
                error_msg       = EXCLUDED.error_msg
        """, (feed_name, now, now if status == 'success' else None, records, status, error))
    conn.commit()


def get_watermark(conn, feed_name: str):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM public.etl_watermarks WHERE feed_name = %s", (feed_name,))
        return cur.fetchone()
