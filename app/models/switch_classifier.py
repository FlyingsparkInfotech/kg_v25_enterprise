"""
SwitchProbabilityClassifier: Predicts P(buyer switches supplier within 90 days).

Model:  XGBoost binary classifier
Label:  1 = relationship became dormant/churned within 90 days
        0 = relationship continued normally
"""

from app.core.logger import info, ok, warn

FEATURE_COLS = [
    'relationship_age_months', 'total_shipments', 'baseline_avg_monthly_qty',
    'last_shipment_date_days_ago', 'qty_trend_3m', 'qty_trend_6m',
    'shipment_regularity', 'months_with_zero_shipments',
    'buyer_total_supplier_count', 'buyer_zi_employee_count',
    'buyer_zi_industry_encoded', 'buyer_zi_intent_signal_count',
    'buyer_zi_has_leadership_change', 'buyer_zi_has_funding_event',
    'buyer_zi_has_ma_event', 'supplier_active_buyer_count',
    'supplier_recent_export_volume', 'hs_code_market_supplier_count',
    'anomaly_score',
]


class SwitchProbabilityClassifier:

    def __init__(self, settings=None):
        self.settings = settings
        self.model = None
        self._importances: dict = {}

    def _prepare(self, X: 'pd.DataFrame') -> 'pd.DataFrame':
        import pandas as pd
        cols = [c for c in FEATURE_COLS if c in X.columns]
        missing = [c for c in FEATURE_COLS if c not in X.columns]
        if missing:
            warn(f'SwitchProbabilityClassifier: {len(missing)} features missing — filling with 0')
        Xp = X[cols].copy()
        for c in missing:
            Xp[c] = 0.0
        return Xp[FEATURE_COLS].fillna(0)

    def fit(self, X: 'pd.DataFrame', y: 'pd.Series') -> None:
        try:
            import xgboost as xgb
        except ImportError:
            warn('SwitchProbabilityClassifier: xgboost not installed — using RandomForest fallback')
            self._fit_fallback(X, y)
            return

        Xp = self._prepare(X)
        pos = int(y.sum())
        neg = len(y) - pos
        scale = max(1, neg // pos) if pos > 0 else 3

        self.model = xgb.XGBClassifier(
            n_estimators=200, max_depth=6, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8,
            scale_pos_weight=scale,
            eval_metric='logloss', random_state=42, verbosity=0,
        )
        self.model.fit(Xp, y)

        importances = self.model.feature_importances_
        self._importances = dict(sorted(
            zip(FEATURE_COLS, importances),
            key=lambda x: x[1], reverse=True
        ))
        top5 = list(self._importances.items())[:5]
        info('SwitchProbabilityClassifier: top-5 features: ' +
             ', '.join(f'{k}={v:.3f}' for k, v in top5))
        ok(f'SwitchProbabilityClassifier: fitted on {len(Xp)} samples (pos_rate={pos/len(y):.2%})')

    def _fit_fallback(self, X: 'pd.DataFrame', y: 'pd.Series') -> None:
        from sklearn.ensemble import RandomForestClassifier
        Xp = self._prepare(X)
        self.model = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
        self.model.fit(Xp, y)
        ok('SwitchProbabilityClassifier: fitted with RandomForest fallback')

    def predict_proba(self, X: 'pd.DataFrame') -> 'np.ndarray':
        import numpy as np
        if self.model is None:
            warn('SwitchProbabilityClassifier: model not fitted — returning zeros')
            return np.zeros(len(X))
        Xp = self._prepare(X)
        try:
            proba = self.model.predict_proba(Xp)
            return proba[:, 1]
        except Exception as e:
            warn(f'SwitchProbabilityClassifier.predict_proba: {e}')
            return np.zeros(len(X))

    def get_feature_importance(self) -> dict:
        return self._importances

    def explain(self, X_row: 'pd.DataFrame') -> dict:
        try:
            import shap
            if self.model is None:
                return {}
            Xp = self._prepare(X_row)
            explainer   = shap.TreeExplainer(self.model)
            shap_values = explainer.shap_values(Xp)
            sv = shap_values[0] if isinstance(shap_values, list) else shap_values[0]
            pairs = sorted(zip(FEATURE_COLS, sv), key=lambda x: abs(x[1]), reverse=True)
            return {k: round(float(v), 4) for k, v in pairs[:5]}
        except Exception:
            return {}

    def save(self, path: str) -> None:
        try:
            import joblib, os
            os.makedirs(os.path.dirname(path), exist_ok=True)
            joblib.dump({'model': self.model, 'importances': self._importances}, path)
            ok(f'SwitchProbabilityClassifier: saved to {path}')
        except Exception as e:
            warn(f'SwitchProbabilityClassifier.save: {e}')

    def load(self, path: str) -> None:
        try:
            import joblib
            data = joblib.load(path)
            self.model        = data.get('model')
            self._importances = data.get('importances', {})
            info(f'SwitchProbabilityClassifier: loaded from {path}')
        except Exception as e:
            warn(f'SwitchProbabilityClassifier.load: {e}')
