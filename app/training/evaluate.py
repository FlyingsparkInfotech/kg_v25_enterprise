"""
Evaluation metrics for ML models.
"""

from app.core.logger import info, warn


def evaluate_classifier(model, X_test: 'pd.DataFrame', y_test: 'pd.Series') -> dict:
    try:
        from sklearn.metrics import (
            accuracy_score, precision_score, recall_score,
            f1_score, roc_auc_score, confusion_matrix,
        )
        import numpy as np
    except ImportError:
        warn('evaluate_classifier: sklearn not available')
        return {}

    probas = model.predict_proba(X_test)
    preds  = (probas >= 0.5).astype(int)

    try:
        auc = float(roc_auc_score(y_test, probas))
    except Exception:
        auc = 0.0

    cm = confusion_matrix(y_test, preds).tolist()

    return {
        'accuracy':         round(float(accuracy_score(y_test, preds)),  4),
        'precision':        round(float(precision_score(y_test, preds, zero_division=0)), 4),
        'recall':           round(float(recall_score(y_test, preds, zero_division=0)),    4),
        'f1':               round(float(f1_score(y_test, preds, zero_division=0)),        4),
        'roc_auc':          round(auc, 4),
        'confusion_matrix': cm,
        'n_test':           len(y_test),
    }


def evaluate_ranker(model, X_test: 'pd.DataFrame', groups_test: 'pd.Series', y_test: 'pd.Series') -> dict:
    try:
        import numpy as np
    except ImportError:
        return {}

    raw_scores = model.rank(X_test)
    scores_100 = model.score_to_100(raw_scores)

    # Mean Reciprocal Rank per query
    mrr_vals = []
    ndcg_vals = []
    query_ids = groups_test.unique() if hasattr(groups_test, 'unique') else []

    for qid in query_ids:
        mask   = groups_test == qid
        q_y    = y_test[mask].values
        q_s    = scores_100[mask]
        order  = np.argsort(q_s)[::-1]
        q_y_sorted = q_y[order]
        # MRR
        for rank, label in enumerate(q_y_sorted, start=1):
            if label == 1:
                mrr_vals.append(1.0 / rank)
                break
        # NDCG@5
        top5    = q_y_sorted[:5]
        dcg     = sum(label / np.log2(i + 2) for i, label in enumerate(top5))
        ideal   = sorted(q_y, reverse=True)[:5]
        idcg    = sum(label / np.log2(i + 2) for i, label in enumerate(ideal))
        ndcg_vals.append(dcg / idcg if idcg > 0 else 0.0)

    return {
        'mean_reciprocal_rank': round(float(np.mean(mrr_vals))  if mrr_vals  else 0.0, 4),
        'ndcg_at_5':            round(float(np.mean(ndcg_vals)) if ndcg_vals else 0.0, 4),
        'n_queries':            len(query_ids),
    }


def print_report(metrics: dict, title: str = 'Evaluation Report') -> None:
    try:
        from rich.table import Table
        from rich.console import Console
        console = Console()
        table = Table(title=title, show_header=True, header_style='bold cyan')
        table.add_column('Metric', style='bold')
        table.add_column('Value')
        for k, v in metrics.items():
            if k == 'confusion_matrix':
                continue
            table.add_row(k, str(v))
        console.print(table)
    except ImportError:
        info(f'--- {title} ---')
        for k, v in metrics.items():
            info(f'  {k}: {v}')
