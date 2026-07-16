"""
Builds ML training dataset from Neo4j graph data.

Labels are derived from historical relationship health:
  Positive (switch=1): relationships with health_status in ['dormant', 'churned']
  Negative (switch=0): relationships with health_status in ['healthy', 'stressed', 'pending']

Run after the switch lead pipeline has processed at least 3 months of data.
"""

from app.core.logger import info, ok, warn


def build_training_dataset(neo, settings) -> tuple:
    """
    Returns (X: pd.DataFrame, y: pd.Series, meta: pd.DataFrame).
    """
    import pandas as pd
    from app.features.feature_extractor import FeatureExtractor

    info('build_dataset: extracting relationship features...')
    fe = FeatureExtractor(neo, settings)
    X  = fe.extract_relationship_features()

    if X.empty:
        warn('build_dataset: no feature data available')
        return pd.DataFrame(), pd.Series(dtype=int), pd.DataFrame()

    # Query labels from Neo4j
    label_rows = neo.run("""
        MATCH (tr:TradeRelationship)
        RETURN tr.rel_id        AS rel_id,
               tr.health_status AS health_status
    """)

    label_map = {r['rel_id']: r['health_status'] for r in label_rows}

    POSITIVE_STATUSES = {'dormant', 'churned'}
    labels = []
    for rid in X['rel_id']:
        status = label_map.get(rid, 'unknown')
        labels.append(1 if status in POSITIVE_STATUSES else 0)

    y    = pd.Series(labels, name='switch_label')
    meta = pd.DataFrame({'rel_id': X['rel_id'], 'health_status': [label_map.get(r, 'unknown') for r in X['rel_id']]})

    pos_rate = y.mean()
    info(f'build_dataset: {len(X)} samples, positive_rate={pos_rate:.2%}')

    if y.sum() < 10:
        warn('build_dataset: fewer than 10 positive samples — model accuracy will be low')

    return X, y, meta


def build_supplier_ranking_dataset(neo, settings) -> tuple:
    """
    Returns (X: pd.DataFrame, y: pd.Series, groups: pd.Series) for XGBRanker.
    """
    import pandas as pd
    from app.features.feature_extractor import FeatureExtractor

    # Query completed switch leads with outcomes
    lead_rows = neo.run("""
        MATCH (opp:SupplierSwitchOpportunity)-[:HAS_MATCH]->(sm:SupplierMatch)-[:GENERATES]->(sl:SwitchLead)
        WHERE sl.status IN ['won', 'lost', 'contacted']
        RETURN opp.opportunity_id          AS opportunity_id,
               sm.match_id                 AS match_id,
               sm.candidate_supplier_org_id AS supplier_id,
               sm.hs_code_match_type       AS hs_match_type,
               sm.match_score              AS match_score,
               sm.active_buyer_count       AS active_buyer_count,
               sm.exports_to_buyer_country AS exports_to_country,
               sm.has_zi_contact           AS has_zi_contact,
               opp.buyer_monthly_volume    AS buyer_monthly_volume,
               CASE WHEN sl.status = 'won' THEN 1 ELSE 0 END AS won
    """)

    if len(lead_rows) < 20:
        warn(f'build_supplier_ranking_dataset: only {len(lead_rows)} labeled pairs — not enough for training')
        return pd.DataFrame(), pd.Series(dtype=int), pd.Series(dtype=int)

    df = pd.DataFrame(lead_rows)
    df['hs_exact_match']    = (df['hs_match_type'] == 'exact').astype(float)
    df['hs_chapter_match']  = 1.0
    df['recent_export_volume'] = 0.0
    df['supplier_zi_employee_count'] = 0
    df['export_recency_days'] = 30
    df['buyer_volume_ratio'] = df['buyer_monthly_volume'].fillna(0) / 1000

    y      = df['won']
    groups = df.groupby('opportunity_id').cumcount().map(lambda _: 1)
    group_sizes = df.groupby('opportunity_id').size()
    groups = df['opportunity_id'].map(lambda oid: group_sizes[oid])

    from app.models.supplier_ranker import RANK_FEATURE_COLS
    X = df[[c for c in RANK_FEATURE_COLS if c in df.columns]].fillna(0)

    ok(f'build_supplier_ranking_dataset: {len(X)} pairs across {df["opportunity_id"].nunique()} queries')
    return X, y, groups


def save_dataset(X: 'pd.DataFrame', y: 'pd.Series', path: str) -> None:
    import pandas as pd
    X.to_csv(path + '_X.csv', index=False)
    y.to_csv(path + '_y.csv', index=False)
    ok(f'save_dataset: saved to {path}_X.csv and {path}_y.csv')
