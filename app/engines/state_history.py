"""
LeadStateHistory — append-only audit trail of lead type transitions.

Every time a lead changes lead_type, a LeadStateHistory node is created:
  - state_from, state_to, reason, triggered_by, score_at_transition, timestamp

Enables: DowngradeEngine, seller debugging, feedback loop correlation.

Usage:
    from app.engines.state_history import record_transition, record_bulk_transitions
    record_transition(neo, lead_uid, state_from="rfq_submitted",
                      state_to="rfq_draft", reason="staleness_14d",
                      triggered_by="DowngradeEngine", score=65)
"""
from datetime import datetime, timezone


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def record_transition(neo, lead_uid: str, state_from: str, state_to: str,
                      reason: str, triggered_by: str = "system", score: int = 0):
    """Create a LeadStateHistory node for a single lead type transition."""
    now = _now_iso()
    history_uid = f"lsh:{lead_uid}:{state_from}:{state_to}:{now}"
    neo.run("""
        MATCH (l:Lead {lead_uid: $lead_uid})
        CREATE (h:LeadStateHistory {
            history_uid:         $history_uid,
            lead_uid:            $lead_uid,
            state_from:          $state_from,
            state_to:            $state_to,
            reason:              $reason,
            triggered_by:        $triggered_by,
            score_at_transition: $score,
            transitioned_at:     $now
        })
        MERGE (l)-[:HAS_STATE_HISTORY]->(h)
        SET l.previous_lead_type = $state_from,
            l.last_transition_at = $now,
            l.transition_count   = coalesce(l.transition_count, 0) + 1
    """, {
        "lead_uid":    lead_uid,
        "history_uid": history_uid,
        "state_from":  state_from or "unknown",
        "state_to":    state_to,
        "reason":      reason,
        "triggered_by": triggered_by,
        "score":       score,
        "now":         now,
    })


def record_bulk_transitions(neo, transitions: list):
    """
    Bulk create state history records.
    Each dict must have: lead_uid, state_from, state_to, reason.
    Optional: triggered_by (default "system"), score (default 0).
    """
    if not transitions:
        return
    now = _now_iso()
    rows = []
    for t in transitions:
        rows.append({
            "lead_uid":     t["lead_uid"],
            "history_uid":  f"lsh:{t['lead_uid']}:{t.get('state_from', '')}:{t['state_to']}:{now}",
            "state_from":   t.get("state_from") or "unknown",
            "state_to":     t["state_to"],
            "reason":       t.get("reason", ""),
            "triggered_by": t.get("triggered_by", "system"),
            "score":        int(t.get("score", 0)),
            "now":          now,
        })
    neo.run("""
        UNWIND $rows AS row
        MATCH (l:Lead {lead_uid: row.lead_uid})
        CREATE (h:LeadStateHistory {
            history_uid:         row.history_uid,
            lead_uid:            row.lead_uid,
            state_from:          row.state_from,
            state_to:            row.state_to,
            reason:              row.reason,
            triggered_by:        row.triggered_by,
            score_at_transition: row.score,
            transitioned_at:     row.now
        })
        MERGE (l)-[:HAS_STATE_HISTORY]->(h)
        SET l.previous_lead_type = row.state_from,
            l.last_transition_at = row.now,
            l.transition_count   = coalesce(l.transition_count, 0) + 1
    """, {"rows": rows})
