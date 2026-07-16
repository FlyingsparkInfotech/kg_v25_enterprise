"""
SparkTradeAggregator: PySpark replacement for TradeAggregator.

Reads raw.trademo_shipment_bl from Postgres in parallel via JDBC,
runs GROUP BY aggregation + baseline window functions in Spark,
then writes TradeRelationship + RelationshipSnapshot nodes to Neo4j
using foreachPartition (parallel writes).

Same Neo4j schema as TradeAggregator — fully compatible downstream.
"""

from app.core.ids import stable_id, utc_now
from app.core.logger import info, ok, warn, banner
from app.spark.session import get_spark, jdbc_url, jdbc_props


# ── Cypher templates ──────────────────────────────────────────────────────────

_REL_CYPHER = """
UNWIND $batch AS row
MERGE (tr:TradeRelationship {rel_id: row.rel_id})
SET tr.buyer_org_id               = row.buyer_id,
    tr.buyer_name                 = row.buyer_name,
    tr.supplier_org_id            = row.supplier_id,
    tr.supplier_name              = row.supplier_name,
    tr.hs_code                    = row.hs_code,
    tr.hs_chapter                 = row.hs_chapter,
    tr.total_shipments            = row.total_shipments,
    tr.relationship_age_months    = row.relationship_age_months,
    tr.baseline_avg_monthly_qty   = row.baseline_avg_qty,
    tr.baseline_avg_monthly_value = row.baseline_avg_value,
    tr.last_shipment_date         = row.last_shipment_date,
    tr.health_status              = 'pending',
    tr.health_score               = null,
    tr.anomaly_score              = null,
    tr.switch_probability         = null,
    tr.updated_at                 = row.updated_at
WITH tr, row
OPTIONAL MATCH (buyer:Organization {orgId: row.buyer_id})
FOREACH (_ IN CASE WHEN buyer IS NOT NULL THEN [1] ELSE [] END |
    MERGE (buyer)-[:BUYER_IN]->(tr)
)
WITH tr, row
OPTIONAL MATCH (supplier:Organization {orgId: row.supplier_id})
FOREACH (_ IN CASE WHEN supplier IS NOT NULL THEN [1] ELSE [] END |
    MERGE (supplier)-[:SUPPLIER_IN]->(tr)
)
RETURN count(tr) AS c
"""

_SNAP_CYPHER = """
UNWIND $batch AS row
MERGE (snap:RelationshipSnapshot {snapshot_id: row.snapshot_id})
SET snap.rel_id              = row.rel_id,
    snap.year_month          = row.year_month,
    snap.shipment_count      = row.shipment_count,
    snap.total_quantity      = row.total_quantity,
    snap.total_value         = row.total_value,
    snap.qty_vs_baseline_pct = row.qty_vs_baseline_pct,
    snap.created_at          = row.created_at
WITH snap, row
MATCH (tr:TradeRelationship {rel_id: row.rel_id})
MERGE (tr)-[:HAS_SNAPSHOT]->(snap)
RETURN count(snap) AS c
"""


# ── partition writers ─────────────────────────────────────────────────────────

def _write_rel_partition(rows_iter, neo4j_uri: str, neo4j_user: str,
                         neo4j_password: str, batch_size: int):
    from neo4j import GraphDatabase
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    batch = []
    with driver.session() as session:
        for spark_row in rows_iter:
            batch.append(spark_row.asDict(recursive=True))
            if len(batch) >= batch_size:
                session.run(_REL_CYPHER, {'batch': batch})
                batch = []
        if batch:
            session.run(_REL_CYPHER, {'batch': batch})
    driver.close()


def _write_snap_partition(rows_iter, neo4j_uri: str, neo4j_user: str,
                          neo4j_password: str, batch_size: int):
    from neo4j import GraphDatabase
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    batch = []
    with driver.session() as session:
        for spark_row in rows_iter:
            batch.append(spark_row.asDict(recursive=True))
            if len(batch) >= batch_size:
                session.run(_SNAP_CYPHER, {'batch': batch})
                batch = []
        if batch:
            session.run(_SNAP_CYPHER, {'batch': batch})
    driver.close()


# ── main class ────────────────────────────────────────────────────────────────

class SparkTradeAggregator:

    def __init__(self, neo, settings):
        self.neo      = neo
        self.settings = settings
        self.spark    = get_spark('KG-SparkTradeAggregator')
        pg = settings.postgres
        self.url      = jdbc_url(pg.host, pg.port, pg.database)
        self.props    = jdbc_props(pg.user, pg.password)

    def run(self) -> dict:
        banner('SparkTradeAggregator: Building TradeRelationship + RelationshipSnapshot nodes')

        col_map = self._discover_columns('raw', 'trademo_shipment_bl')
        if not col_map.get('buyer_id') or not col_map.get('supplier_id'):
            warn('SparkTradeAggregator: buyer_id or supplier_id not found — aborting')
            return {'relationships': 0, 'snapshots': 0}

        monthly_df = self._build_monthly_df(col_map)
        if monthly_df is None:
            return {'relationships': 0, 'snapshots': 0}

        rel_df, snap_df = self._compute_rel_and_snap(monthly_df)
        n_rels  = self._write_relationships(rel_df)
        n_snaps = self._write_snapshots(snap_df)

        ok(f'SparkTradeAggregator complete — relationships={n_rels}, snapshots={n_snaps}')
        return {'relationships': n_rels, 'snapshots': n_snaps}

    # ── column discovery ──────────────────────────────────────────────────────

    def _discover_columns(self, schema: str, table: str) -> dict:
        try:
            sample = self.spark.read.jdbc(
                url=self.url,
                table=f'(SELECT * FROM "{schema}"."{table}" LIMIT 1) AS _s',
                properties=self.props,
            )
            actual_cols = sample.columns
        except Exception as e:
            warn(f'SparkTradeAggregator: column discovery failed: {e}')
            return {}

        lower_map = {c.lower(): c for c in actual_cols}
        candidates = {
            'buyer_id':      ['importer_id', 'buyer_id', 'consignee_id', 'companyid'],
            'buyer_name':    ['importer_name', 'buyer_name', 'consignee_name', 'company_name'],
            'supplier_id':   ['exporter_id', 'supplier_id', 'shipper_id'],
            'supplier_name': ['exporter_name', 'supplier_name', 'shipper_name'],
            'hs_code':       ['hs_code', 'hscode', 'matched_hs_code', 'commodity_code'],
            'quantity':      ['quantity', 'net_weight', 'gross_weight', 'total_weight', 'weight_kg'],
            'value':         ['shipment_value', 'total_value', 'trade_value', 'fob_value', 'cif_value', 'amount'],
            'date':          ['shipment_date', 'bl_date', 'date', 'arrival_date', 'departure_date', 'created_at'],
        }
        col_map = {}
        for canonical, options in candidates.items():
            found = next((lower_map[opt] for opt in options if opt in lower_map), None)
            col_map[canonical] = found
            if found:
                info(f'SparkTradeAggregator: {canonical} -> "{found}"')
            else:
                warn(f'SparkTradeAggregator: no column for "{canonical}"')
        return col_map

    # ── read + aggregate with Spark ───────────────────────────────────────────

    def _build_monthly_df(self, col_map: dict):
        from pyspark.sql import functions as F

        required = ['buyer_id', 'supplier_id', 'hs_code', 'date']
        for r in required:
            if not col_map.get(r):
                warn(f'SparkTradeAggregator: required column "{r}" missing')
                return None

        b   = col_map['buyer_id']
        bn  = col_map.get('buyer_name')
        s   = col_map['supplier_id']
        sn  = col_map.get('supplier_name')
        h   = col_map['hs_code']
        d   = col_map['date']
        qty = col_map.get('quantity')
        val = col_map.get('value')

        # Read last 36 months from trademo_shipment_bl in parallel partitions
        read_sql = f"""(
            SELECT *
            FROM raw.trademo_shipment_bl
            WHERE "{b}" IS NOT NULL
              AND "{s}" IS NOT NULL
              AND "{h}" IS NOT NULL
              AND "{d}" IS NOT NULL
              AND "{d}"::timestamp >= NOW() - INTERVAL '36 months'
        ) AS _raw"""

        try:
            # Partition by shipment date month for parallelism
            df = self.spark.read.jdbc(
                url=self.url, table=read_sql,
                numPartitions=40,
                properties=self.props,
            )
        except Exception as e:
            warn(f'SparkTradeAggregator: JDBC read failed: {e}')
            return None

        info(f'SparkTradeAggregator: raw rows loaded = {df.count():,}')

        # Cast and clean
        df = (df
              .withColumn('_buyer_id',   F.col(f'`{b}`').cast('string'))
              .withColumn('_supplier_id', F.col(f'`{s}`').cast('string'))
              .withColumn('_hs_code',    F.col(f'`{h}`').cast('string'))
              .withColumn('_date',       F.col(f'`{d}`').cast('timestamp'))
              .withColumn('_buyer_name',    F.col(f'`{bn}`').cast('string') if bn else F.lit(None).cast('string'))
              .withColumn('_supplier_name', F.col(f'`{sn}`').cast('string') if sn else F.lit(None).cast('string'))
              .withColumn('_qty',  F.col(f'`{qty}`').cast('double') if qty else F.lit(0.0))
              .withColumn('_val',  F.col(f'`{val}`').cast('double') if val else F.lit(0.0))
              .filter(
                  F.col('_buyer_id').isNotNull() &
                  F.col('_supplier_id').isNotNull() &
                  F.col('_hs_code').isNotNull() &
                  F.col('_date').isNotNull()
              )
              .withColumn('_year_month', F.date_trunc('month', F.col('_date')).cast('string'))
              )

        # Monthly GROUP BY
        monthly = (df
                   .groupBy('_buyer_id', '_supplier_id', '_hs_code', '_year_month')
                   .agg(
                       F.first('_buyer_name').alias('buyer_name'),
                       F.first('_supplier_name').alias('supplier_name'),
                       F.count('*').alias('shipment_count'),
                       F.sum(F.coalesce(F.col('_qty'), F.lit(0.0))).alias('total_quantity'),
                       F.sum(F.coalesce(F.col('_val'), F.lit(0.0))).alias('total_value'),
                   )
                   .withColumnRenamed('_buyer_id',    'buyer_id')
                   .withColumnRenamed('_supplier_id', 'supplier_id')
                   .withColumnRenamed('_hs_code',     'hs_code')
                   .withColumnRenamed('_year_month',  'year_month')
                   )

        return monthly

    # ── compute rel + snap DataFrames ─────────────────────────────────────────

    def _compute_rel_and_snap(self, monthly_df):
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        now = utc_now()

        # Window for row-number ordering within each relationship
        w_order = Window.partitionBy('buyer_id', 'supplier_id', 'hs_code').orderBy('year_month')

        monthly_with_rn = monthly_df.withColumn('rn', F.row_number().over(w_order))

        # Baseline = avg of first 6 months
        baseline_df = (monthly_with_rn
                       .filter(F.col('rn') <= 6)
                       .groupBy('buyer_id', 'supplier_id', 'hs_code')
                       .agg(
                           F.avg('total_quantity').alias('baseline_avg_qty'),
                           F.avg('total_value').alias('baseline_avg_value'),
                       ))

        monthly_bl = monthly_with_rn.join(baseline_df, ['buyer_id', 'supplier_id', 'hs_code'], 'left')

        # ── RelationshipSnapshot rows ─────────────────────────────────────────
        snap_df = (monthly_bl
                   .withColumn('rel_id', F.udf(lambda b, s, h: stable_id(b, s, h))('buyer_id', 'supplier_id', 'hs_code'))
                   .withColumn('snapshot_id', F.udf(lambda r, y: stable_id(r, y))('rel_id', 'year_month'))
                   .withColumn('qty_vs_baseline_pct',
                               F.when(F.col('baseline_avg_qty') > 0,
                                      F.round((F.col('total_quantity') / F.col('baseline_avg_qty') - 1.0) * 100, 2))
                               .otherwise(F.lit(0.0)))
                   .withColumn('created_at', F.lit(now))
                   .select('snapshot_id', 'rel_id', 'year_month',
                           'shipment_count', 'total_quantity', 'total_value',
                           'qty_vs_baseline_pct', 'created_at')
                   )

        # ── TradeRelationship rows ────────────────────────────────────────────
        rel_agg = (monthly_bl
                   .groupBy('buyer_id', 'buyer_name', 'supplier_id', 'supplier_name',
                             'hs_code', 'baseline_avg_qty', 'baseline_avg_value')
                   .agg(
                       F.sum('shipment_count').alias('total_shipments'),
                       F.countDistinct('year_month').alias('relationship_age_months'),
                       F.max('year_month').alias('last_shipment_date'),
                   )
                   .withColumn('rel_id', F.udf(lambda b, s, h: stable_id(b, s, h))('buyer_id', 'supplier_id', 'hs_code'))
                   .withColumn('hs_chapter', F.substring(F.col('hs_code'), 1, 2))
                   .withColumn('updated_at', F.lit(now))
                   .select('rel_id', 'buyer_id', 'buyer_name', 'supplier_id', 'supplier_name',
                           'hs_code', 'hs_chapter', 'total_shipments', 'relationship_age_months',
                           'baseline_avg_qty', 'baseline_avg_value', 'last_shipment_date', 'updated_at')
                   )

        return rel_agg, snap_df

    # ── write to Neo4j ────────────────────────────────────────────────────────

    def _write_relationships(self, rel_df) -> int:
        neo_uri  = self.settings.neo4j.uri
        neo_user = self.settings.neo4j.user
        neo_pass = self.settings.neo4j.password
        bs       = int(self.settings.runtime.batch_size)

        n = rel_df.count()
        rel_df.foreachPartition(
            lambda rows: _write_rel_partition(rows, neo_uri, neo_user, neo_pass, bs)
        )
        info(f'SparkTradeAggregator: {n} TradeRelationship nodes written')
        return n

    def _write_snapshots(self, snap_df) -> int:
        neo_uri  = self.settings.neo4j.uri
        neo_user = self.settings.neo4j.user
        neo_pass = self.settings.neo4j.password
        bs       = int(self.settings.runtime.batch_size)

        n = snap_df.count()
        snap_df.foreachPartition(
            lambda rows: _write_snap_partition(rows, neo_uri, neo_user, neo_pass, bs)
        )
        info(f'SparkTradeAggregator: {n} RelationshipSnapshot nodes written')
        return n
