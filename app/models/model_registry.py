"""
ModelRegistry: Manages ML model persistence and versioning.
Single interface to save/load all 3 supplier-switch models.
"""

import os
import time

from app.core.logger import info, ok, warn


class ModelRegistry:

    def __init__(self, settings):
        self.settings = settings
        os.makedirs('models', exist_ok=True)

    def save(self, model, name: str, metrics: dict = None, tracker=None) -> None:
        path_map = {
            'health_detector':   self.settings.models.health_detector,
            'switch_classifier': self.settings.models.switch_classifier,
            'supplier_ranker':   self.settings.models.supplier_ranker,
        }
        path = path_map.get(name)
        if not path:
            warn(f'ModelRegistry.save: unknown model name "{name}"')
            return
        os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
        model.save(path)
        if tracker and metrics:
            for k, v in (metrics or {}).items():
                try:
                    tracker.metric(f'{name}_{k}', float(v))
                except Exception:
                    pass
        ok(f'ModelRegistry: saved {name} → {path}')

    def _load(self, name: str, cls_import: str):
        path_map = {
            'health_detector':   self.settings.models.health_detector,
            'switch_classifier': self.settings.models.switch_classifier,
            'supplier_ranker':   self.settings.models.supplier_ranker,
        }
        path = path_map.get(name, '')

        module_path, cls_name = cls_import.rsplit('.', 1)
        import importlib
        module = importlib.import_module(module_path)
        cls    = getattr(module, cls_name)
        instance = cls(self.settings)

        if os.path.exists(path):
            instance.load(path)
        else:
            warn(f'ModelRegistry: {name} model not found at {path} — returning unfitted instance')
        return instance

    def load_health_detector(self):
        return self._load('health_detector', 'app.models.health_detector.RelationshipHealthDetector')

    def load_switch_classifier(self):
        return self._load('switch_classifier', 'app.models.switch_classifier.SwitchProbabilityClassifier')

    def load_supplier_ranker(self):
        return self._load('supplier_ranker', 'app.models.supplier_ranker.SupplierRanker')

    def models_exist(self) -> bool:
        return all(os.path.exists(p) for p in [
            self.settings.models.health_detector,
            self.settings.models.switch_classifier,
            self.settings.models.supplier_ranker,
        ])

    def models_age_days(self) -> float:
        paths = [
            self.settings.models.health_detector,
            self.settings.models.switch_classifier,
            self.settings.models.supplier_ranker,
        ]
        existing = [p for p in paths if os.path.exists(p)]
        if not existing:
            return 9999.0
        oldest_mtime = min(os.path.getmtime(p) for p in existing)
        return (time.time() - oldest_mtime) / 86400

    def needs_retraining(self) -> bool:
        return not self.models_exist() or self.models_age_days() > self.settings.models.retrain_after_days
