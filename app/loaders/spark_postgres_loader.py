"""
SparkPostgresLoader: PySpark replacement for PostgresLoader.

Reads every mapped Postgres table in parallel via JDBC partitions,
then writes signals to Neo4j using foreachPartition so multiple
Spark tasks write concurrently.

Drop-in replacement: same Neo4j graph schema as PostgresLoader.
"""

import json
from app.core.ids import stable_id, utc_now
from app.core.logger import info, ok, warn
from app.mappings.postgres_signal_map import signal_mapping
from app.spark.session import get_spark, jdbc_url, jdbc_props


# ── helpers ────────────────────────────────────────────────────────────────────

def _first(row: dict, keys: list):
    lower = {k.lower(): k for k in row}
    for k in keys:
        if k in row and row[k] not in (None, ''):
            return row[k]
        lk = k.lower()
        if lk in lower and row[lower[lk]] not in (None, ''):
            return row[lower[lk]]
    return None


def _to_float(v):
    if v in (None, ''):
        return None
    try:
        return float(str(v).replace(',', '').strip())
    except Exception:
        return None


def _transform_row(row: dict, source_key: str, st: str, sf: str, now: str) -> dict:
    rid = str(
        _first(row, ['id', 'source_record_id', 'uuid', 'uid', 'company_id', 'companyid',
                     'contact_id', 'shipment_id', 'identifier', 'bl_id', 'bsl_key'])
        or stable_id(source_key, json.dumps(row, default=str, sort_keys=True)[:1000])
    )
    raw_uid = stable_id('source_record', source_key, rid)
    sig_uid = stable_id('signal', source_key, rid, st)
    account  = _first(row, ['account_id', 'company_id', 'companyid', 'organization_id', 'org_id',
                             'importer_id', 'exporter_id', 'company_name', 'companyname',
                             'consignee_name', 'shipper_name', 'domain'])
    person   = _first(row, ['person_id', 'contact_id', 'user_id', 'email', 'phone', 'mobilephone'])
    product  = _first(row, ['product_id', 'sku', 'product_name', 'hs_code', 'hscode', 'productdescription'])
    category = _first(row, ['category_id', 'category', 'hs_code', 'hscode', 'industry'])
    return {
        'source_key': source_key,
        'raw_uid':    raw_uid,
        'rid':        rid,
        'payload':    json.dumps(row, default=str, ensure_ascii=False)[:12000],
        'sig_uid':    sig_uid,
        'st':         st,
        'sf':         sf,
        'occurred_at': str(_first(row, ['occurred_at', 'created_at', 'updated_at', 'timestamp',
                                        'event_time', 'shipment_date', 'date', 'received_at',
                                        'modified_on', 'created_on']) or now),
        'conf':       0.90 if sf == 'TradeShipmentSource' else (0.85 if sf == 'EnrichmentSource' else 0.70),
        'account_hint':  str(account)  if account  is not None else None,
        'person_hint':   str(person)   if person   is not None else None,
        'product_hint':  str(product)  if product  is not None else None,
        'category_hint': str(category) if category is not None else None,
        'hs_code':       str(_first(row, ['hs_code', 'hscode', 'matched_hs_code']) or '') or None,
        'shipment_value': _to_float(_first(row, ['shipment_value', 'shipmentvalue', 'total_value',
                                                  'trade_value', 'amount'])),
        'number_of_shipments': _to_float(_first(row, ['number_of_shipments', 'totalshipments',
                                                        'shipment_count', 'shipments'])),
    }


_SIGNAL_CYPHER = """
UNWIND $rows AS row
MERGE (raw:RawSource {raw_source_uid: row.raw_uid})
  SET raw:SourceRecord,
      raw.source_record_uid = row.raw_uid,
      raw.source_key        = row.source_key,
      raw.source_record_id  = row.rid,
      raw.payload_json      = row.payload,
      raw.payload_ref       = row.payload,
      raw.source_table      = row.source_key,
      raw.source_system     = 'postgres',
      raw.occurred_at       = row.occurred_at,
      raw.confidence_score  = row.conf
MERGE (sig:Signal {signal_uid: row.sig_uid})
  SET sig.signalId          = row.sig_uid,
      sig.source_key        = row.source_key,
      sig.signal_type       = row.st,
      sig.signalType        = row.st,
      sig.source_family     = row.sf,
      sig.sourceFamily      = row.sf,
      sig.occurred_at       = row.occurred_at,
      sig.confidence_score  = row.conf,
      sig.intensity_score   = CASE WHEN row.st CONTAINS 'shipment' OR row.st CONTAINS 'rfq'
                                   THEN 1.0 ELSE 0.6 END,
      sig.account_hint      = row.account_hint,
      sig.person_hint       = row.person_hint,
      sig.product_hint      = row.product_hint,
      sig.category_hint     = row.category_hint,
      sig.hs_code           = row.hs_code,
      sig.shipment_value    = row.shipment_value,
      sig.number_of_shipments = row.number_of_shipments,
      sig.source            = 'postgres',
      sig.sourceTable       = row.source_key,
      sig.v25_stage         = coalesce(sig.v25_stage, 'signal_loaded')
MERGE (reg:SourceRegistry  {source_key: row.source_key})
MERGE (sf:SourceFamily     {name: row.sf})
MERGE (typ:SignalType      {name: row.st})
MERGE (raw)-[:RECORDED_IN_SOURCE]->(reg)
MERGE (raw)-[:NORMALIZES_TO]->(sig)
MERGE (sig)-[:FROM_SOURCE]->(reg)
MERGE (sig)-[:BELONGS_TO]->(sf)
MERGE (sig)-[:HAS_TYPE]->(typ)
"""


def _write_partition(rows_iter, neo4j_uri: str, neo4j_user: str,
                     neo4j_password: str, source_key: str,
                     st: str, sf: str, batch_size: int):
    """Called once per Spark partition — opens its own Neo4j connection."""
    from neo4j import GraphDatabase
    from app.core.ids import utc_now

    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    now = utc_now()
    batch = []
    written = 0

    with driver.session() as session:
        for spark_row in rows_iter:
            row = spark_row.asDict(recursive=True)
            batch.append(_transform_row(row, source_key, st, sf, now))
            if len(batch) >= batch_size:
                session.run(_SIGNAL_CYPHER, {'rows': batch})
                written += len(batch)
                batch = []
        if batch:
            session.run(_SIGNAL_CYPHER, {'rows': batch})
            written += len(batch)

    driver.close()


# ── main class ─────────────────────────────────────────────────────────────────

class SparkPostgresLoader:

    def __init__(self, neo, settings):
        self.neo      = neo
        self.settings = settings
        self.spark    = get_spark('KG-SparkPostgresLoader')
        pg = settings.postgres
        self.url   = jdbc_url(pg.host, pg.port, pg.database)
        self.props = jdbc_props(pg.user, pg.password)

    def run(self):
        pg          = self.settings.postgres
        neo_uri     = self.settings.neo4j.uri
        neo_user    = self.settings.neo4j.user
        neo_pass    = self.settings.neo4j.password
        batch_size  = int(self.settings.runtime.batch_size)
        table_limit = int(pg.table_limit or 0)
        total       = 0

        schemas_sql = ','.join(f"'{s}'" for s in pg.schemas)
        meta_df = self.spark.read.jdbc(
            url=self.url,
            table=f"""(
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema IN ({schemas_sql})
                  AND table_type = 'BASE TABLE'
                ORDER BY table_schema, table_name
            ) AS _meta""",
            properties=self.props,
        )
        tables = [(r['table_schema'], r['table_name']) for r in meta_df.collect()]

        for schema, table in tables:
            mapped = signal_mapping(schema, table)
            if not mapped:
                continue
            st, sf = mapped
            key = f'{schema}.{table}'.lower()

            limit_clause = f' LIMIT {table_limit}' if table_limit > 0 else ''

            try:
                # Count rows for partition planning
                cnt_row = self.spark.read.jdbc(
                    url=self.url,
                    table=f'(SELECT COUNT(*) AS cnt FROM "{schema}"."{table}") AS _cnt',
                    properties=self.props,
                ).collect()[0]
                cnt = int(cnt_row['cnt'])
                if cnt == 0:
                    info(f'skip {key}: empty table')
                    continue

                # Determine number of partitions (1 per ~20k rows, cap at 50)
                num_parts = max(1, min(cnt // 20000 + 1, 50))

                # Check for a numeric column to partition on
                cols_df = self.spark.read.jdbc(
                    url=self.url,
                    table=f'(SELECT * FROM "{schema}"."{table}" LIMIT 1) AS _s',
                    properties=self.props,
                )
                cols = cols_df.columns[:100]
                part_col = next((c for c in ['id', 'bl_id', 'shipment_id', 'company_id',
                                              'contact_id', 'record_id'] if c in cols), None)

                read_sql = f'(SELECT {", ".join(chr(34)+c+chr(34) for c in cols)} FROM "{schema}"."{table}"{limit_clause}) AS _data'

                if part_col and num_parts > 1:
                    bounds = self.spark.read.jdbc(
                        url=self.url,
                        table=f'(SELECT MIN("{part_col}") AS lo, MAX("{part_col}") AS hi FROM "{schema}"."{table}") AS _b',
                        properties=self.props,
                    ).collect()[0]
                    lo, hi = int(bounds['lo'] or 0), int(bounds['hi'] or 0)
                    df = self.spark.read.jdbc(
                        url=self.url, table=read_sql,
                        column=part_col, lowerBound=lo, upperBound=hi + 1,
                        numPartitions=num_parts, properties=self.props,
                    )
                else:
                    df = self.spark.read.jdbc(url=self.url, table=read_sql, properties=self.props)

            except Exception as e:
                warn(f'SparkPostgresLoader: skip {key}: {e}')
                continue

            # Register source in Neo4j (single write, not per partition)
            self.neo.run(
                "MERGE (sr:SourceRegistry {source_key:$k}) SET sr.source_group=$g, sr.source_name=$n, sr.is_active=true",
                {'k': key, 'g': sf, 'n': table}
            )

            # Parallel write: each Spark partition opens its own Neo4j connection
            df.foreachPartition(
                lambda rows: _write_partition(rows, neo_uri, neo_user, neo_pass, key, st, sf, batch_size)
            )

            row_count = cnt if not table_limit else min(cnt, table_limit)
            total += row_count
            info(f'{key}: {st} — {row_count} rows written via {num_parts} partitions')

        ok(f'SparkPostgresLoader complete — ~{total} signals across {len(tables)} tables')
        return total
