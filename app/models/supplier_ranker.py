"""
SupplierRanker: Ranks candidate suppliers for a stressed buyer-HS_code pair.

Model:  XGBoost rank:pairwise (Learning to Rank)
Score:  match_score 0-100 (higher = better match)
"""

import numpy as np
from app.core.logger import info, ok, warn

RANK_FEATURE_COLS = [
    'hs_exact_match', 'hs_chapter_match',
    'active_buyer_count', 'recent_export_volume',
    'exports_to_buyer_country', 'supplier_zi_employee_count',
    'has_zi_contact', 'export_recency_days',
    'buyer_volume_ratio',
]


class SupplierRanker:

    def __init__(self, settings=None):
        self.settings = settings
        self.model    = None

    def _prepare(self, X: 'pd.DataFrame') -> 'pd.DataFrame':
        cols    = [c for c in RANK_FEATURE_COLS if c in X.columns]
        missing = [c for c in RANK_FEATURE_COLS if c not in X.columns]
        Xp = X[cols].copy()
        for c in missing:
            Xp[c] = 0.0
        return Xp[RANK_FEATURE_COLS].fillna(0)

    def fit(self, X: 'pd.DataFrame', y: 'pd.Series', groups: 'pd.Series') -> None:
        try:
            import xgboost as xgb
        except ImportError:
            warn('SupplierRanker: xgboost not installed — ranker unavailable, using heuristic fallback')
            return

        Xp = self._prepare(X)
        self.model = xgb.XGBRanker(
            objective='rank:pairwise', n_estimators=100, max_depth=4,
            learning_rate=0.1, random_state=42, verbosity=0,
        )
        group_sizes = groups.value_counts().sort_index().tolist()
        self.model.fit(Xp, y, group=group_sizes)
        ok(f'SupplierRanker: fitted on {len(Xp)} candidates across {len(group_sizes)} queries')

    def rank(self, X: 'pd.DataFrame') -> np.ndarray:
        if self.model is None:
            return self._heuristic_scores(X)
        Xp = self._prepare(X)
        try:
            return self.model.predict(Xp)
        except Exception as e:
            warn(f'SupplierRanker.rank: {e} — using heuristic fallback')
            return self._heuristic_scores(X)

    def _heuristic_scores(self, X: 'pd.DataFrame') -> np.ndarray:
        scores = np.zeros(len(X))
        for i, (_, row) in enumerate(X.iterrows()):
            s  = float(row.get('hs_exact_match', 0))            * 40
            s += float(row.get('hs_chapter_match', 0))          * 20
            s += min(float(row.get('active_buyer_count', 0)), 10) * 3
            s += float(row.get('exports_to_buyer_country', 0))  * 15
            s += float(row.get('has_zi_contact', 0))            * 10
            recency = float(row.get('export_recency_days', 999))
            s += max(0, 7 - recency / 30) * 2
            scores[i] = s
        return scores

    def score_to_100(self, raw_scores: np.ndarray) -> np.ndarray:
        s_min, s_max = raw_scores.min(), raw_scores.max()
        span = s_max - s_min
        if span < 1e-9:
            return np.full(len(raw_scores), 50.0)
        return ((raw_scores - s_min) / span) * 100

    def save(self, path: str) -> None:
        try:
            import joblib, os
            os.makedirs(os.path.dirname(path), exist_ok=True)
            joblib.dump({'model': self.model}, path)
            ok(f'SupplierRanker: saved to {path}')
        except Exception as e:
            warn(f'SupplierRanker.save: {e}')

    def load(self, path: str) -> None:
        try:
            import joblib
            data = joblib.load(path)
            self.model = data.get('model')
            info(f'SupplierRanker: loaded from {path}')
        except Exception as e:
            warn(f'SupplierRanker.load: {e}')
