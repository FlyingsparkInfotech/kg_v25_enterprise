import yaml, os
from typing import List
from pydantic import BaseModel, Field


class Neo4jCfg(BaseModel):
    uri: str; user: str; password: str


class MySQLCfg(BaseModel):
    host: str = '127.0.0.1'; port: int = 3306
    user: str = 'root'; password: str; database: str


class PostgresCfg(BaseModel):
    enabled: bool = True
    host: str = ''; port: int = 5432
    user: str = ''; password: str = ''; database: str = ''
    schemas: List[str] = Field(default_factory=lambda: ['raw', 'zoominfo', 'skg', 'public', 'staging', 'etl'])
    table_limit: int = 1000


class XlsxCfg(BaseModel):
    taxonomy: str = ''; entity: str = ''; relationships: str = ''; conditional: str = ''


class RuntimeCfg(BaseModel):
    batch_size: int = 5000
    signal_batch: int = 2000
    max_rows_per_step: int = 50000
    legacy_base_file: str = 'ui_data_injector_v16_full_governance_fixed_GI_integrated_v4_production.py'
    scoring_model_version: str = 'v25_1_enterprise_modular_observed'
    progress_every_batches: int = 1


class MlflowCfg(BaseModel):
    enabled: bool = False
    tracking_uri: str = 'file:./mlruns'
    experiment_name: str = 'kg_v25_enterprise'


class ModelsCfg(BaseModel):
    health_detector: str = 'models/health_detector.pkl'
    switch_classifier: str = 'models/switch_classifier.pkl'
    supplier_ranker: str = 'models/supplier_ranker.pkl'
    retrain_after_days: int = 30
    switch_lead_min_score: float = 60.0
    switch_prob_threshold: float = 0.40
    match_score_threshold: float = 60.0


class ScoringCfg(BaseModel):
    """
    All numeric thresholds used in the factory pipeline.
    Centralised here so they can be tuned via config.yaml without
    touching any Python or Cypher source files.
    """
    # ── Evidence base strength by type ────────────────────────────────────────
    evidence_base_rfq: int = 70
    evidence_base_importer: int = 70
    evidence_base_commitment: int = 90
    evidence_base_decision_maker: int = 45
    evidence_base_default: int = 25

    # ── Context / combination bonuses ─────────────────────────────────────────
    bonus_product_hint: int = 10
    bonus_person_hint: int = 10
    bonus_combination: int = 10

    # ── Opportunity confidence formula weights ────────────────────────────────
    opp_weight_evidence: float = 0.40    # weight of (evidence_strength/100)
    opp_weight_product: float = 0.30     # weight of product_match
    opp_weight_contact: float = 0.30     # weight of contact_exists flag
    opp_threshold: float = 0.65          # min confidence to create OpportunityHypothesis

    # ── FitSuppression final-score formula weights ────────────────────────────
    final_weight_evidence: float = 0.35
    final_weight_fit: float = 0.25
    final_weight_confidence: float = 0.20
    final_weight_base: float = 0.20
    final_base_value: int = 50           # constant term in the formula

    # ── Suppression thresholds ────────────────────────────────────────────────
    suppress_weak_identity_below: float = 0.55
    suppress_low_confidence_below: float = 0.50
    suppress_low_intent_below: int = 50
    suppress_no_fit_below: int = 40

    # ── Lead priority score cutoffs ───────────────────────────────────────────
    priority_critical_score: int = 85
    priority_high_score: int = 70

    # ── Seller visibility minimum evidence strength ───────────────────────────
    seller_visible_evidence_min: int = 50

    # ── Time windows for signal freshness filtering ───────────────────────────
    time_window_rfq_days: int = 7        # RFQ signals older than 7d are stale
    time_window_shipment_days: int = 90  # Trade/shipment signals: 90-day window
    time_window_enrichment_days: int = 365  # Contact/enrichment: 1-year window
    time_window_behavior_days: int = 30  # Web/CRM behaviour: 30-day window

    # ── 6-component scoring weights (should sum to ~1.0) ─────────────────────
    score_weight_behavior: float = 0.15
    score_weight_intent: float = 0.25
    score_weight_trade: float = 0.25
    score_weight_fit: float = 0.15
    score_weight_recency: float = 0.10
    score_weight_reachability: float = 0.10

    # ── Hard minimum scores per lead type ─────────────────────────────────────
    min_score_rfq: int = 75              # RFQ lead must score ≥ 75 to be 'rfq_submitted'
    min_score_commitment: int = 70       # Commitment lead must score ≥ 70
    min_score_decision_maker: int = 55   # DM-presence lead must score ≥ 55
    min_score_trade: int = 40            # Trade lead must score ≥ 40 to be 'market_opportunity'

    # ── Validation SLOs (used by PipelineValidator) ───────────────────────────
    max_identity_miss_pct: float = 20.0  # % signals allowed without identity
    max_evidence_null_pct: float = 5.0   # % evidence nodes allowed with null strength
    max_weak_leads_pct: float = 30.0     # % leads allowed with evidence < seller_visible_evidence_min


class DistributionCfg(BaseModel):
    """Lead Distribution Centre configuration."""
    enabled: bool = True
    max_leads_per_seller_per_day: int = 50
    exclusivity: bool = False       # True = one seller per lead
    tier_priority: bool = True      # enterprise sellers get first pick


class MonetizationCfg(BaseModel):
    """Credit-based subscription tier configuration."""
    enabled: bool = True
    default_tier: str = 'basic'
    credit_cost_critical: int = 5
    credit_cost_high: int = 3
    credit_cost_medium: int = 1
    credit_cost_low: int = 0
    monthly_credits_basic: int = 100
    monthly_credits_professional: int = 500
    monthly_credits_enterprise: int = -1   # -1 = unlimited


class KafkaCfg(BaseModel):
    """Kafka broker + topic configuration."""
    enabled:           bool      = False
    bootstrap_servers: list[str] = Field(default_factory=lambda: ['localhost:9092'])
    consumer_group_id: str       = 'kg-pipeline-consumers'
    # Topics — override in config.yaml if your Kafka uses different names
    topic_switch_leads:        str = 'kg.switch_leads'
    topic_trade_relationships: str = 'kg.trade_relationships'
    topic_buyer_profiles:      str = 'kg.buyer_profiles'
    topic_pipeline_runs:       str = 'kg.pipeline_runs'
    topic_crm_rfq:             str = 'crm.rfq_submitted'
    topic_crm_shipments:       str = 'crm.shipments'
    topic_crm_sessions:        str = 'crm.buyer_sessions'
    topic_crm_clicks:          str = 'crm.buyer_clicks'


class DemandbaseCfg(BaseModel):
    """Demandbase IP-to-company resolution configuration."""
    enabled: bool = False
    api_key: str = ''
    confidence: float = 0.65
    batch_size: int = 200


class FeedbackCfg(BaseModel):
    """Seller feedback and threshold auto-tuning configuration."""
    enabled: bool = True
    min_feedback_samples: int = 50
    retune_every_days: int = 7
    config_path: str = 'config.yaml'


class Settings(BaseModel):
    neo4j: Neo4jCfg
    mysql_ui: MySQLCfg
    mysql_crm: MySQLCfg
    postgres: PostgresCfg = Field(default_factory=PostgresCfg)
    xlsx: XlsxCfg = Field(default_factory=XlsxCfg)
    runtime: RuntimeCfg = Field(default_factory=RuntimeCfg)
    mlflow: MlflowCfg = Field(default_factory=MlflowCfg)
    models: ModelsCfg = Field(default_factory=ModelsCfg)
    scoring: ScoringCfg = Field(default_factory=ScoringCfg)
    distribution: DistributionCfg = Field(default_factory=DistributionCfg)
    monetization: MonetizationCfg = Field(default_factory=MonetizationCfg)
    demandbase: DemandbaseCfg = Field(default_factory=DemandbaseCfg)
    feedback: FeedbackCfg = Field(default_factory=FeedbackCfg)
    kafka: KafkaCfg = Field(default_factory=KafkaCfg)


def _expand(v):
    if isinstance(v, str):   return os.path.expandvars(v)
    if isinstance(v, dict):  return {k: _expand(x) for k, x in v.items()}
    if isinstance(v, list):  return [_expand(x) for x in v]
    return v


def load_settings(path):
    with open(path, 'r', encoding='utf-8') as f:
        return Settings.model_validate(_expand(yaml.safe_load(f) or {}))
