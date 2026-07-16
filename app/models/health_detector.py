"""
RelationshipHealthDetector: Detects anomalies in buyer-supplier trade relationship
time series. Uses IsolationForest (Phase 1) with a Prophet upgrade path (Phase 2).

Input:  DataFrame with columns: rel_id, year_month, total_quantity, total_value,
        shipment_count, qty_vs_baseline_pct
Output: anomaly_score (0-1), trend_direction, health_score (0-100), health_status
"""

from app.core.logger import info, ok, warn


class RelationshipHealthDetector:

    def __init__(self, settings=None):
        self.settings = settings
        self.model  = None
        self.scaler = None

    # ── feature extraction from time-series snapshots ─────────────────────────

    def _build_rel_features(self, df: 'pd.DataFrame') -> 'pd.DataFrame':
        """Aggregate snapshot rows into one feature row per rel_id."""
        import pandas as pd
        import numpy as np

        records = []
        for rel_id, grp in df.groupby('rel_id'):
            qtys  = grp['total_quantity'].fillna(0).tolist()
            n     = len(qtys)
            if n == 0:
                continue
            mean_qty = float(sum(qtys) / n)
            std_qty  = float(pd.Series(qtys).std() or 0.0)
            cv_qty   = (std_qty / mean_qty) if mean_qty > 0 else 0.0

            # Linear trend (slope of qty over time index)
            if n >= 3:
                xs    = list(range(n))
                xs_m  = sum(xs) / n
                ys_m  = mean_qty
                num   = sum((xs[i] - xs_m) * (qtys[i] - ys_m) for i in range(n))
                den   = sum((xs[i] - xs_m) ** 2 for i in range(n)) or 1e-9
                slope = num / den
            else:
                slope = 0.0

            # Gap regularity: stdev of month-over-month gaps (using index gaps = 1 if data is monthly)
            # Approximate as stdev of non-zero vs zero months
            zero_frac = len([q for q in qtys if q == 0]) / n

            # Recent 3m avg vs baseline
            last_3  = qtys[-3:] if n >= 3 else qtys
            avg_3   = sum(last_3) / len(last_3) if last_3 else 0.0
            first_6 = qtys[:6] if n >= 6 else qtys
            avg_base = sum(first_6) / len(first_6) if first_6 else 0.0
            recent_vs_baseline = (avg_3 / avg_base) if avg_base > 0 else 1.0

            records.append({
                'rel_id':               rel_id,
                'mean_qty':             mean_qty,
                'cv_qty':               cv_qty,
                'slope':                slope,
                'zero_frac':            zero_frac,
                'recent_vs_baseline':   recent_vs_baseline,
                'n_months':             n,
            })

        return pd.DataFrame(records) if records else pd.DataFrame()

    # ── fit ────────────────────────────────────────────────────────────────────

    def fit(self, df: 'pd.DataFrame') -> None:
        try:
            from sklearn.ensemble import IsolationForest
            from sklearn.preprocessing import StandardScaler
            import pandas as pd
        except ImportError:
            warn('RelationshipHealthDetector.fit: sklearn not available — using statistical fallback')
            return

        feat_df = self._build_rel_features(df)
        if feat_df.empty:
            warn('RelationshipHealthDetector.fit: no data to fit')
            return

        FEATURE_COLS = ['mean_qty', 'cv_qty', 'slope', 'zero_frac', 'recent_vs_baseline']
        X = feat_df[FEATURE_COLS].fillna(0).values

        self.scaler = StandardScaler()
        X_scaled    = self.scaler.fit_transform(X)

        self.model  = IsolationForest(contamination=0.15, random_state=42, n_jobs=-1)
        self.model.fit(X_scaled)
        ok(f'RelationshipHealthDetector: fitted on {len(feat_df)} relationships')

    # ── predict ────────────────────────────────────────────────────────────────

    def predict(self, df: 'pd.DataFrame') -> 'pd.DataFrame':
        import pandas as pd

        feat_df = self._build_rel_features(df)
        if feat_df.empty:
            return pd.DataFrame()

        FEATURE_COLS = ['mean_qty', 'cv_qty', 'slope', 'zero_frac', 'recent_vs_baseline']

        # ML anomaly score
        if self.model and self.scaler:
            try:
                X        = feat_df[FEATURE_COLS].fillna(0).values
                X_sc     = self.scaler.transform(X)
                raw_scores = self.model.decision_function(X_sc)   # higher = more normal
                # Map to 0-1 where 1 = most anomalous
                s_min, s_max = raw_scores.min(), raw_scores.max()
                span = (s_max - s_min) or 1e-9
                anomaly_scores = 1.0 - (raw_scores - s_min) / span
            except Exception as e:
                warn(f'RelationshipHealthDetector.predict: ML scoring failed ({e}), using fallback')
                anomaly_scores = self._fallback_scores(feat_df)
        else:
            anomaly_scores = self._fallback_scores(feat_df)

        results = []
        for i, row in feat_df.iterrows():
            a_score = float(anomaly_scores[list(feat_df.index).index(i)])
            slope   = float(row.get('slope') or 0)
            rvb     = float(row.get('recent_vs_baseline') or 1.0)

            # Trend direction
            if rvb < 0.20:
                trend = 'collapsed'
            elif slope < -0.1:
                trend = 'declining'
            elif slope > 0.1:
                trend = 'growing'
            else:
                trend = 'stable'

            # Health score
            health = 100 - (a_score * 60)
            days_ago = 0  # will be refined in switch_lead_engine using last_shipment_date
            if a_score > 0.7:
                health -= 20
            health = max(0, min(100, health))

            # Status
            if health >= 70:
                status = 'healthy'
            elif health >= 40:
                status = 'stressed'
            elif health >= 20:
                status = 'dormant'
            else:
                status = 'churned'

            results.append({
                'rel_id':          row['rel_id'],
                'anomaly_score':   round(a_score, 4),
                'trend_direction': trend,
                'health_score':    round(health, 1),
                'health_status':   status,
            })

        return pd.DataFrame(results)

    def predict_single(self, snapshots: list) -> dict:
        """Score one relationship from a list of snapshot dicts."""
        import pandas as pd
        if not snapshots:
            return {'anomaly_score': 0.3, 'trend_direction': 'insufficient_data',
                    'health_score': 50.0, 'health_status': 'pending', 'rel_id': ''}
        if len(snapshots) < 3:
            return {'anomaly_score': 0.3, 'trend_direction': 'insufficient_data',
                    'health_score': 50.0, 'health_status': 'pending',
                    'rel_id': str(snapshots[0].get('rel_id', ''))}
        df = pd.DataFrame(snapshots)
        df.rename(columns={'qty': 'total_quantity', 'value': 'total_value'}, inplace=True)
        if 'total_quantity' not in df.columns:
            df['total_quantity'] = 0
        result = self.predict(df)
        return result.iloc[0].to_dict() if not result.empty else {}

    def _fallback_scores(self, feat_df: 'pd.DataFrame') -> list:
        """Statistical fallback when sklearn not available."""
        scores = []
        for _, row in feat_df.iterrows():
            rvb = float(row.get('recent_vs_baseline') or 1.0)
            cv  = float(row.get('cv_qty') or 0)
            zf  = float(row.get('zero_frac') or 0)
            s   = max(0.0, min(1.0, (1.0 - rvb) * 0.5 + cv * 0.3 + zf * 0.2))
            scores.append(s)
        return scores

    # ── persistence ────────────────────────────────────────────────────────────

    def save(self, path: str) -> None:
        try:
            import joblib, os
            os.makedirs(os.path.dirname(path), exist_ok=True)
            joblib.dump({'model': self.model, 'scaler': self.scaler}, path)
            ok(f'RelationshipHealthDetector: saved to {path}')
        except Exception as e:
            warn(f'RelationshipHealthDetector.save: {e}')

    def load(self, path: str) -> None:
        try:
            import joblib
            data        = joblib.load(path)
            self.model  = data.get('model')
            self.scaler = data.get('scaler')
            info(f'RelationshipHealthDetector: loaded from {path}')
        except Exception as e:
            warn(f'RelationshipHealthDetector.load: {e}')
