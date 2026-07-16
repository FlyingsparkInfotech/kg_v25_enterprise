"""
FeedbackEngine — Seller action feedback and threshold auto-tuning.

Records seller actions (viewed / contacted / converted / rejected / archived)
on SellerLeadAssignment nodes and, when enough feedback has accumulated,
re-tunes the ScoringCfg thresholds written to config.yaml.

Tuning rules (conservative):
  - If rfq_submitted conversion rate > 40%  → lower min_score_rfq  by 2 (floor 65)
  - If rfq_submitted conversion rate < 10%  → raise min_score_rfq  by 2 (cap  85)
  - If trade leads rejected >60%             → raise min_score_trade by 2 (cap  55)
  - If DM leads converted >30%              → lower min_score_decision_maker by 2 (floor 45)
  - If evidence_strength correlation high   → raise seller_visible_evidence_min by 5 (cap 70)

Config (config.yaml):
  feedback:
    enabled: true
    min_feedback_samples: 50      # minimum actions before tuning fires
    retune_every_days: 7          # how often threshold auto-tuning runs
    config_path: config.yaml      # path to write updated thresholds
"""

import yaml
import os
from datetime import datetime, timezone, timedelta
from typing import Optional
from app.core.logger import info, ok, warn, banner


# Valid seller actions
VALID_ACTIONS = {'viewed', 'contacted', 'converted', 'rejected', 'archived'}

_RECORD_ACTION_QUERY = """
MATCH (a:SellerLeadAssignment {assignment_id: $assignment_id})
SET a.status       = $action,
    a.actioned_at  = toString(datetime()),
    a.action_notes = coalesce($notes, a.action_notes)
WITH a
MATCH (a)-[:FOR_LEAD]->(l:Lead)
SET l.seller_feedback = $action,
    l.seller_actioned_at = toString(datetime())
MERGE (fb:SellerFeedback {
    feedback_id: 'fb:' + a.assignment_id + ':' + $action
})
SET fb.assignment_id = a.assignment_id,
    fb.seller_id     = a.seller_id,
    fb.lead_id       = a.lead_id,
    fb.lead_type     = l.lead_type,
    fb.lead_priority = l.priority,
    fb.action        = $action,
    fb.notes         = $notes,
    fb.recorded_at   = toString(datetime())
MERGE (a)-[:HAS_FEEDBACK]->(fb)
RETURN count(fb) AS c
"""

_STATS_QUERY = """
MATCH (fb:SellerFeedback)
WHERE fb.recorded_at >= $since
RETURN fb.lead_type AS lead_type,
       fb.action    AS action,
       count(fb)    AS n
"""

_LAST_TUNED_QUERY = """
MERGE (meta:SystemMeta {key: 'feedback_last_tuned'})
RETURN meta.value AS last_tuned
"""

_SET_LAST_TUNED = """
MERGE (meta:SystemMeta {key: 'feedback_last_tuned'})
SET meta.value = toString(datetime())
"""


class FeedbackEngine:

    def __init__(self, neo, settings, config_path: str = 'config.yaml'):
        self.neo         = neo
        cfg              = getattr(settings, 'feedback', None)
        self.enabled     = bool(cfg and getattr(cfg, 'enabled', True))
        self.min_samples = int(getattr(cfg, 'min_feedback_samples', 50) if cfg else 50)
        self.retune_days = int(getattr(cfg, 'retune_every_days', 7)    if cfg else 7)
        self.config_path = getattr(cfg, 'config_path', config_path)    if cfg else config_path
        self.scoring     = settings.scoring

    # ── public: record a single seller action ─────────────────────────────────

    def record_action(
        self,
        assignment_id: str,
        action:        str,
        notes:         Optional[str] = None,
    ) -> bool:
        if action not in VALID_ACTIONS:
            warn(f'FeedbackEngine: unknown action "{action}" — must be one of {VALID_ACTIONS}')
            return False

        rows = self.neo.run(_RECORD_ACTION_QUERY, {
            'assignment_id': assignment_id,
            'action':        action,
            'notes':         notes or '',
        }) or []
        return bool(rows and rows[0].get('c', 0) > 0)

    # ── public: run threshold auto-tuning ─────────────────────────────────────

    def run_tuning(self) -> dict:
        if not self.enabled:
            info('FeedbackEngine: disabled in config')
            return {'tuned': False}

        if not self._is_due():
            info('FeedbackEngine: tuning not due yet')
            return {'tuned': False, 'reason': 'not_due'}

        banner('FeedbackEngine: Running threshold auto-tuning')
        since = (datetime.now(timezone.utc) - timedelta(days=self.retune_days * 4)).isoformat()
        stats = self._build_stats(since)

        if not stats:
            info('FeedbackEngine: no feedback data yet')
            return {'tuned': False, 'reason': 'no_data'}

        total_samples = sum(stats.values())
        if total_samples < self.min_samples:
            info(f'FeedbackEngine: only {total_samples} samples — need {self.min_samples} before tuning')
            return {'tuned': False, 'reason': 'insufficient_samples', 'samples': total_samples}

        changes = self._compute_adjustments(stats)
        if changes:
            self._apply_to_config(changes)
            self.neo.run(_SET_LAST_TUNED)
            ok(f'FeedbackEngine: applied {len(changes)} threshold adjustments')
            return {'tuned': True, 'changes': changes, 'samples': total_samples}

        self.neo.run(_SET_LAST_TUNED)
        return {'tuned': False, 'reason': 'no_adjustments_needed', 'samples': total_samples}

    # ── internal ──────────────────────────────────────────────────────────────

    def _is_due(self) -> bool:
        rows = self.neo.run(_LAST_TUNED_QUERY) or []
        if not rows or not rows[0].get('last_tuned'):
            return True
        try:
            last = datetime.fromisoformat(rows[0]['last_tuned'].replace('Z', '+00:00'))
            return (datetime.now(timezone.utc) - last).days >= self.retune_days
        except Exception:
            return True

    def _build_stats(self, since: str) -> dict:
        """Return dict: (lead_type, action) → count."""
        rows = self.neo.run(_STATS_QUERY, {'since': since}) or []
        stats: dict = {}
        for r in rows:
            key = (r.get('lead_type', 'unknown'), r.get('action', 'unknown'))
            stats[key] = int(r.get('n', 0))
        return stats

    def _conversion_rate(self, lead_type: str, stats: dict) -> Optional[float]:
        converted = stats.get((lead_type, 'converted'), 0)
        total = sum(v for (lt, _), v in stats.items() if lt == lead_type)
        if total == 0:
            return None
        return converted / total

    def _rejection_rate(self, lead_type: str, stats: dict) -> Optional[float]:
        rejected = stats.get((lead_type, 'rejected'), 0)
        total = sum(v for (lt, _), v in stats.items() if lt == lead_type)
        if total == 0:
            return None
        return rejected / total

    def _compute_adjustments(self, stats: dict) -> dict:
        sc = self.scoring
        changes: dict = {}

        # RFQ conversion rate
        rfq_conv = self._conversion_rate('rfq_submitted', stats)
        if rfq_conv is not None:
            if rfq_conv > 0.40:
                new = max(65, sc.min_score_rfq - 2)
                if new != sc.min_score_rfq:
                    changes['min_score_rfq'] = new
                    info(f'FeedbackEngine: rfq conv={rfq_conv:.0%} → lower min_score_rfq {sc.min_score_rfq}→{new}')
            elif rfq_conv < 0.10:
                new = min(85, sc.min_score_rfq + 2)
                if new != sc.min_score_rfq:
                    changes['min_score_rfq'] = new
                    info(f'FeedbackEngine: rfq conv={rfq_conv:.0%} → raise min_score_rfq {sc.min_score_rfq}→{new}')

        # Trade rejection rate
        trade_rej = self._rejection_rate('trade_buyer_candidate', stats)
        if trade_rej is not None and trade_rej > 0.60:
            new = min(55, sc.min_score_trade + 2)
            if new != sc.min_score_trade:
                changes['min_score_trade'] = new
                info(f'FeedbackEngine: trade rej={trade_rej:.0%} → raise min_score_trade {sc.min_score_trade}→{new}')

        # Decision-maker conversion rate
        dm_conv = self._conversion_rate('buying_committee_active', stats)
        if dm_conv is not None and dm_conv > 0.30:
            new = max(45, sc.min_score_decision_maker - 2)
            if new != sc.min_score_decision_maker:
                changes['min_score_decision_maker'] = new
                info(f'FeedbackEngine: dm conv={dm_conv:.0%} → lower min_score_dm {sc.min_score_decision_maker}→{new}')

        # Evidence strength: if visible leads are mostly rejected, raise bar
        all_rej = self._rejection_rate('warm_account', stats)
        if all_rej is not None and all_rej > 0.70:
            new = min(70, sc.seller_visible_evidence_min + 5)
            if new != sc.seller_visible_evidence_min:
                changes['seller_visible_evidence_min'] = new
                info(f'FeedbackEngine: warm rej={all_rej:.0%} → raise ev_min {sc.seller_visible_evidence_min}→{new}')

        return changes

    def _apply_to_config(self, changes: dict):
        """Write updated thresholds back to config.yaml under scoring:."""
        if not os.path.exists(self.config_path):
            warn(f'FeedbackEngine: config not found at {self.config_path} — skipping write')
            return
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f) or {}

            scoring = data.setdefault('scoring', {})
            for key, val in changes.items():
                scoring[key] = val

            # Add a tuning audit trail
            data.setdefault('feedback_tuning', {})['last_applied'] = datetime.now(timezone.utc).isoformat()
            data['feedback_tuning']['last_changes'] = changes

            with open(self.config_path, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

            ok(f'FeedbackEngine: config.yaml updated with {len(changes)} changes')
        except Exception as e:
            warn(f'FeedbackEngine: failed to write config: {e}')
