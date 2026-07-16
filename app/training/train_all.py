"""
Training orchestrator for all 3 ML models.

Usage:
    python main.py train-models --config config.yaml
"""

from app.core.config import load_settings
from app.core.logger import info, ok, warn, banner
from app.db.neo4j_client import Neo4jClient
from app.models.model_registry import ModelRegistry
from app.observability.mlflow_logger import MlflowTracker


def train_health_detector(neo, settings, registry: ModelRegistry, tracker=None) -> dict:
    banner('Training: RelationshipHealthDetector')
    import pandas as pd
    from app.models.health_detector import RelationshipHealthDetector

    rows = neo.run("""
        MATCH (tr:TradeRelationship)-[:HAS_SNAPSHOT]->(snap:RelationshipSnapshot)
        RETURN tr.rel_id          AS rel_id,
               snap.year_month    AS year_month,
               snap.total_quantity AS total_quantity,
               snap.total_value   AS total_value,
               snap.shipment_count AS shipment_count,
               snap.qty_vs_baseline_pct AS qty_vs_baseline_pct
    """)

    if not rows:
        warn('train_health_detector: no snapshot data found — skipping')
        return {'skipped': True, 'reason': 'no data'}

    df = pd.DataFrame(rows)
    df['total_quantity'] = pd.to_numeric(df['total_quantity'], errors='coerce').fillna(0)

    model = RelationshipHealthDetector(settings)
    model.fit(df)
    registry.save(model, 'health_detector',
                  metrics={'n_relationships': df['rel_id'].nunique(), 'model_type': 'IsolationForest'},
                  tracker=tracker)

    return {'n_relationships': int(df['rel_id'].nunique()), 'model_type': 'IsolationForest'}


def train_switch_classifier(neo, settings, registry: ModelRegistry, tracker=None) -> dict:
    banner('Training: SwitchProbabilityClassifier')
    from app.training.build_dataset import build_training_dataset
    from app.models.switch_classifier import SwitchProbabilityClassifier

    X, y, meta = build_training_dataset(neo, settings)

    if X.empty:
        warn('train_switch_classifier: no training data — skipping')
        return {'skipped': True, 'reason': 'no data'}

    pos_count = int(y.sum())
    if pos_count < 50:
        warn(f'train_switch_classifier: only {pos_count} positive samples (need >=50) — skipping')
        return {'skipped': True, 'reason': f'insufficient positives: {pos_count}'}

    from sklearn.model_selection import train_test_split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

    model = SwitchProbabilityClassifier(settings)
    model.fit(X_train, y_train)

    # Evaluate
    from app.training.evaluate import evaluate_classifier
    metrics = evaluate_classifier(model, X_test, y_test)
    metrics['n_samples']    = len(X)
    metrics['positive_rate'] = round(float(y.mean()), 4)

    registry.save(model, 'switch_classifier', metrics=metrics, tracker=tracker)
    ok(f'train_switch_classifier: AUC={metrics.get("roc_auc", "n/a")}, F1={metrics.get("f1", "n/a")}')
    return metrics


def train_supplier_ranker(neo, settings, registry: ModelRegistry, tracker=None) -> dict:
    banner('Training: SupplierRanker')
    from app.training.build_dataset import build_supplier_ranking_dataset
    from app.models.supplier_ranker import SupplierRanker

    X, y, groups = build_supplier_ranking_dataset(neo, settings)

    if X.empty:
        warn('train_supplier_ranker: no ranking data — skipping')
        return {'skipped': True, 'reason': 'no labeled pairs'}

    model = SupplierRanker(settings)
    model.fit(X, y, groups)
    n_queries = int(groups.nunique()) if hasattr(groups, 'nunique') else 0
    registry.save(model, 'supplier_ranker',
                  metrics={'n_pairs': len(X), 'n_queries': n_queries},
                  tracker=tracker)

    return {'n_pairs': len(X), 'n_queries': n_queries}


def run(config_path: str = 'config.yaml') -> None:
    banner('KG V25.2 TRAIN ML MODELS')
    settings = load_settings(config_path)
    tracker  = MlflowTracker(settings)
    registry = ModelRegistry(settings)

    n = neo = Neo4jClient(settings.neo4j.uri, settings.neo4j.user, settings.neo4j.password)
    try:
        r1 = train_health_detector(n, settings, registry, tracker)
        r2 = train_switch_classifier(n, settings, registry, tracker)
        r3 = train_supplier_ranker(n, settings, registry, tracker)

        ok('Training complete:')
        for name, result in [('health_detector', r1), ('switch_classifier', r2), ('supplier_ranker', r3)]:
            if result.get('skipped'):
                warn(f'  {name}: SKIPPED — {result.get("reason")}')
            else:
                info(f'  {name}: {result}')
    finally:
        n.close()


if __name__ == '__main__':
    import sys
    cfg = sys.argv[1] if len(sys.argv) > 1 else 'config.yaml'
    run(cfg)
