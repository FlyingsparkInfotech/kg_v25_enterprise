from __future__ import annotations
from contextlib import contextmanager
from typing import Any, Dict, Optional

class MlflowTracker:
    def __init__(self, settings: Any):
        self.settings = settings
        self.enabled = bool(getattr(getattr(settings, 'mlflow', None), 'enabled', False))
        self.mlflow = None
        if self.enabled:
            try:
                import mlflow  # type: ignore
                self.mlflow = mlflow
                cfg = settings.mlflow
                if getattr(cfg, 'tracking_uri', ''):
                    mlflow.set_tracking_uri(cfg.tracking_uri)
                mlflow.set_experiment(getattr(cfg, 'experiment_name', 'kg_v25_enterprise'))
            except Exception as e:
                print(f"⚠️ MLflow disabled because import/setup failed: {e}")
                self.enabled = False
                self.mlflow = None

    @contextmanager
    def run(self, run_name: str, params: Optional[Dict[str, Any]] = None):
        if not self.enabled or not self.mlflow:
            yield self
            return
        with self.mlflow.start_run(run_name=run_name):
            if params:
                safe = {str(k): str(v)[:250] for k, v in params.items() if v is not None}
                self.mlflow.log_params(safe)
            yield self

    def metric(self, name: str, value: Any, step: Optional[int] = None):
        if not self.enabled or not self.mlflow:
            return
        try:
            self.mlflow.log_metric(name, float(value or 0), step=step)
        except Exception as e:
            print(f"⚠️ MLflow metric skipped {name}: {e}")

    def param(self, name: str, value: Any):
        if not self.enabled or not self.mlflow:
            return
        try:
            self.mlflow.log_param(name, str(value)[:250])
        except Exception:
            pass
