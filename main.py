import typer
from app.core.config import load_settings
from app.core.logger import banner, ok
from app.db.neo4j_client import Neo4jClient
from app.db.postgres_client import PostgresClient
from app.loaders.legacy_base_loader import LegacyBaseLoader
from app.loaders.postgres_loader import PostgresLoader
from app.engines.init_engine import InitEngine
from app.engines.factory_engine import FactoryEngine
from app.engines.distribution_engine import DistributionEngine
from app.engines.feedback_engine import FeedbackEngine
from app.engines.conversion_engine import ConversionEngine
from app.engines.monetization_engine import MonetizationEngine
from app.validation.pipeline_checks import PipelineValidator
from app.observability.mlflow_logger import MlflowTracker

app = typer.Typer(help='KG V25.2 Enterprise Modular Framework with progress + optional MLflow')

def neo(settings):
    return Neo4jClient(settings.neo4j.uri, settings.neo4j.user, settings.neo4j.password)

def run_params(settings):
    return {
        'neo4j_uri': settings.neo4j.uri,
        'postgres_enabled': settings.postgres.enabled,
        'postgres_db': settings.postgres.database,
        'postgres_table_limit': settings.postgres.table_limit,
        'signal_batch': settings.runtime.signal_batch,
        'max_rows_per_step': settings.runtime.max_rows_per_step,
        'scoring_model_version': settings.runtime.scoring_model_version,
    }

@app.command('init')
def init(config: str = 'config.yaml'):
    s = load_settings(config); tracker = MlflowTracker(s); banner('KG V25.2 INIT')
    with tracker.run('init', run_params(s)):
        n = neo(s)
        try: InitEngine(n, s).run()
        finally: n.close()

@app.command('load-base')
def load_base(config: str = 'config.yaml', skip_ui: bool = False, skip_crm: bool = False, skip_governance: bool = False):
    s = load_settings(config); tracker = MlflowTracker(s); banner('BASE GOVERNANCE + UI + CRM + G-I')
    with tracker.run('load-base', run_params(s)):
        LegacyBaseLoader(s).run(skip_governance, skip_ui, skip_crm)

@app.command('load-postgres')
def load_postgres(config: str = 'config.yaml'):
    s = load_settings(config); tracker = MlflowTracker(s); banner('POSTGRES TRADEMO + ZOOMINFO + SKG')
    with tracker.run('load-postgres', run_params(s)):
        n = neo(s); pg = PostgresClient(s.postgres.host, s.postgres.port, s.postgres.user, s.postgres.password, s.postgres.database)
        try: PostgresLoader(n, pg, s).run()
        finally: pg.close(); n.close()

@app.command('derive-signals')
def derive_signals(config: str = 'config.yaml'):
    s = load_settings(config); tracker = MlflowTracker(s); banner('DERIVE UI/CRM SIGNALS')
    with tracker.run('derive-signals', run_params(s)):
        n = neo(s)
        try: FactoryEngine(n, s, tracker).derive_signals()
        finally: n.close()

@app.command('factory-only')
def factory_only(config: str = 'config.yaml'):
    s = load_settings(config); tracker = MlflowTracker(s); banner('KG V25.2 FACTORY ONLY')
    with tracker.run('factory-only', run_params(s)):
        n = neo(s)
        try: FactoryEngine(n, s, tracker).factory_only()
        finally: n.close()

@app.command('validate')
def validate(config: str = 'config.yaml'):
    s = load_settings(config); tracker = MlflowTracker(s); banner('KG V25.2 VALIDATE')
    with tracker.run('validate', run_params(s)):
        n = neo(s)
        try: PipelineValidator(n, s).run()
        finally: n.close()

@app.command('spark-load-postgres')
def spark_load_postgres(config: str = 'config.yaml'):
    s = load_settings(config); tracker = MlflowTracker(s); banner('SPARK POSTGRES LOADER (parallel JDBC)')
    with tracker.run('spark-load-postgres', run_params(s)):
        n = neo(s)
        try:
            from app.loaders.spark_postgres_loader import SparkPostgresLoader
            SparkPostgresLoader(n, s).run()
        finally: n.close()

@app.command('spark-build-trade-graph')
def spark_build_trade_graph(config: str = 'config.yaml'):
    s = load_settings(config); tracker = MlflowTracker(s); banner('SPARK TRADE GRAPH (parallel JDBC + Spark aggregation)')
    with tracker.run('spark-build-trade-graph', run_params(s)):
        n = neo(s)
        try:
            from app.features.spark_trade_aggregator import SparkTradeAggregator
            from app.features.zoominfo_enricher import ZoomInfoEnricher
            from app.db.postgres_client import PostgresClient
            pg = PostgresClient(s.postgres.host, s.postgres.port, s.postgres.user, s.postgres.password, s.postgres.database)
            try:
                SparkTradeAggregator(n, s).run()
                ZoomInfoEnricher(n, pg, s).run()
            finally: pg.close()
        finally: n.close()

@app.command('build-trade-graph')
def build_trade_graph(config: str = 'config.yaml'):
    s = load_settings(config); tracker = MlflowTracker(s); banner('BUILD TRADE RELATIONSHIP GRAPH')
    with tracker.run('build-trade-graph', run_params(s)):
        n = neo(s); pg = PostgresClient(s.postgres.host, s.postgres.port, s.postgres.user, s.postgres.password, s.postgres.database)
        try:
            from app.engines.switch_lead_engine import SwitchLeadEngine
            e = SwitchLeadEngine(n, pg, s, tracker); e.build_trade_relationships(); e.enrich_with_zoominfo()
        finally: pg.close(); n.close()

@app.command('detect-stress-rules')
def detect_stress_rules(config: str = 'config.yaml'):
    """Run rule-based stress detection only (no ML model needed). Works with any data volume."""
    s = load_settings(config); tracker = MlflowTracker(s); banner('RULE-BASED STRESS DETECTION')
    with tracker.run('detect-stress-rules', run_params(s)):
        n = neo(s); pg = PostgresClient(s.postgres.host, s.postgres.port, s.postgres.user, s.postgres.password, s.postgres.database)
        try:
            from app.engines.switch_lead_engine import SwitchLeadEngine
            SwitchLeadEngine(n, pg, s, tracker).detect_stress_rules()
        finally: pg.close(); n.close()


@app.command('detect-switch-leads')
def detect_switch_leads(config: str = 'config.yaml'):
    s = load_settings(config); tracker = MlflowTracker(s); banner('DETECT SUPPLIER SWITCH LEADS')
    with tracker.run('detect-switch-leads', run_params(s)):
        n = neo(s); pg = PostgresClient(s.postgres.host, s.postgres.port, s.postgres.user, s.postgres.password, s.postgres.database)
        try:
            from app.engines.switch_lead_engine import SwitchLeadEngine
            e = SwitchLeadEngine(n, pg, s, tracker)
            e.detect_stress(); e.score_switch_probability(); e.match_suppliers(); e.create_switch_leads()
        finally: pg.close(); n.close()

@app.command('train-models')
def train_models(config: str = 'config.yaml'):
    s = load_settings(config); tracker = MlflowTracker(s); banner('TRAIN ML MODELS')
    with tracker.run('train-models', run_params(s)):
        from app.training.train_all import run as train_run
        train_run(config)

@app.command('classify-platform-leads')
def classify_platform_leads(config: str = 'config.yaml'):
    s = load_settings(config); banner('CLASSIFY PLATFORM LEADS')
    from app.features.lead_classifier import run as clf_run
    clf_run(config)

@app.command('build-buyer-profiles')
def build_buyer_profiles(config: str = 'config.yaml'):
    """Build BuyerProfile nodes from GoGlo platform behavioral data (clicks, scrolls, sessions, searches)."""
    s = load_settings(config); banner('BUILD BUYER PROFILES')
    from app.features.buyer_behavior_aggregator import run as bba_run
    bba_run(config)

@app.command('run-switch-pipeline')
def run_switch_pipeline(config: str = 'config.yaml'):
    s = load_settings(config); tracker = MlflowTracker(s); banner('RUN FULL SWITCH LEAD PIPELINE')
    with tracker.run('run-switch-pipeline', run_params(s)):
        n = neo(s); pg = PostgresClient(s.postgres.host, s.postgres.port, s.postgres.user, s.postgres.password, s.postgres.database)
        try:
            from app.engines.switch_lead_engine import SwitchLeadEngine
            SwitchLeadEngine(n, pg, s, tracker).run_all()
        finally: pg.close(); n.close()

@app.command('run-all')
def run_all(config: str = 'config.yaml', skip_base: bool = False, skip_postgres: bool = False):
    s = load_settings(config); tracker = MlflowTracker(s); banner('KG V25.2 RUN ALL')
    with tracker.run('run-all', run_params(s)):
        if not skip_base:
            banner('STAGE 1/6 BASE GOVERNANCE + UI + CRM + G-I')
            LegacyBaseLoader(s).run()
        banner('STAGE 2/6 INIT CONSTRAINTS + INDEXES')
        n = neo(s)
        try: InitEngine(n, s).run()
        finally: n.close()
        if s.postgres.enabled and not skip_postgres:
            banner('STAGE 3/6 POSTGRES INTELLIGENCE')
            load_postgres(config)
        banner('STAGE 4/6 DERIVE UI/CRM SIGNALS')
        derive_signals(config)
        banner('STAGE 5/6 MODULAR FACTORY')
        factory_only(config)
        banner('STAGE 6/6 VALIDATION')
        validate(config)
        ok('KG V25.2 run-all complete')

@app.command('distribute')
def distribute(config: str = 'config.yaml'):
    """Lead Distribution Centre — assign leads to sellers based on HS code / geography / tier."""
    s = load_settings(config); tracker = MlflowTracker(s); banner('LEAD DISTRIBUTION CENTRE')
    with tracker.run('distribute', run_params(s)):
        n = neo(s)
        try: DistributionEngine(n, s).run()
        finally: n.close()


@app.command('process-feedback')
def process_feedback(config: str = 'config.yaml'):
    """Run seller feedback threshold auto-tuning."""
    s = load_settings(config); tracker = MlflowTracker(s); banner('FEEDBACK THRESHOLD TUNING')
    with tracker.run('process-feedback', run_params(s)):
        n = neo(s)
        try: FeedbackEngine(n, s, config).run_tuning()
        finally: n.close()


@app.command('track-conversions')
def track_conversions(config: str = 'config.yaml'):
    """Record ConversionFact nodes for all seller-converted leads."""
    s = load_settings(config); tracker = MlflowTracker(s); banner('CONVERSION TRACKING')
    with tracker.run('track-conversions', run_params(s)):
        n = neo(s)
        try: ConversionEngine(n, s).run()
        finally: n.close()


@app.command('billing-reset')
def billing_reset(config: str = 'config.yaml'):
    """Reset monthly credit allocations for seller accounts due for renewal."""
    s = load_settings(config); tracker = MlflowTracker(s); banner('MONTHLY BILLING RESET')
    with tracker.run('billing-reset', run_params(s)):
        n = neo(s)
        try: MonetizationEngine(n, s).run()
        finally: n.close()


@app.command('init-seller-schema')
def init_seller_schema(config: str = 'config.yaml'):
    """Create constraints and indexes for the seller/distribution layer."""
    s = load_settings(config); banner('SELLER SCHEMA INIT')
    n = neo(s)
    try: n.run_file('app/cypher/seller_schema.cypher')
    finally: n.close()
    ok('Seller schema initialised')


@app.command('kafka-poll')
def kafka_poll(config: str = 'config.yaml'):
    """
    Start the CRM Polling Producer — polls 11 CRM/goglo_staging tables every 30 s
    and publishes new rows to Kafka topics.  Replaces Debezium when the MySQL user
    lacks REPLICATION CLIENT privilege.

    Watermarks stored at /opt/kg_data/crm_poll_watermarks.json — delete this file
    to re-publish all historical rows (full initial load).

    Runs indefinitely. Stop with CTRL+C or SIGTERM.
    Requires kafka.enabled=true in config.yaml.
    """
    s = load_settings(config)
    if not getattr(s.kafka, 'enabled', False):
        banner('Kafka is disabled — set kafka.enabled=true in config.yaml to activate')
        return
    banner('KG V25.2 CRM POLL PRODUCER')
    from app.kafka.crm_poll_producer import CRMPollProducer
    CRMPollProducer(s).run()


@app.command('kafka-consume')
def kafka_consume(config: str = 'config.yaml'):
    """
    Start the Kafka event consumer — listens to raw CRM/web topics and
    triggers the appropriate KG pipeline stage for each event.

    Topics consumed:
      crm.rfq_submitted   → LeadClassifier
      crm.shipments       → TradeAggregator + StressDetector
      crm.buyer_sessions  → BuyerBehaviorAggregator
      crm.buyer_clicks    → BuyerBehaviorAggregator (intent signal)
      crm.lead_updates    → LeadClassifier (reclassify)

    Runs indefinitely. Stop with CTRL+C or SIGTERM.
    Requires kafka.enabled=true in config.yaml.
    """
    s = load_settings(config)
    if not getattr(s.kafka, 'enabled', False):
        banner('Kafka is disabled — set kafka.enabled=true in config.yaml to activate')
        return
    banner('KG V25.2 KAFKA EVENT CONSUMER')
    n  = neo(s)
    pg = PostgresClient(s.postgres.host, s.postgres.port, s.postgres.user,
                        s.postgres.password, s.postgres.database)
    try:
        from app.kafka.consumer import KafkaEventConsumer
        KafkaEventConsumer(n, pg, s).run()
    finally:
        pg.close()
        n.close()


@app.command('serve')
def serve(config: str = 'config.yaml', host: str = '0.0.0.0', port: int = 8000):
    banner('KG V25.2 SWITCH LEAD API')
    import uvicorn, os
    os.environ['KG_CONFIG'] = config
    uvicorn.run('app.api.server:app', host=host, port=port, reload=False)

if __name__ == '__main__':
    app()
