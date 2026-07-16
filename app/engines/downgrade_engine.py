"""
DowngradeEngine — staleness-based lead type demotion.

Leads that have had no new signals for N days are automatically
downgraded to a lower-priority type to keep seller feeds fresh.

Demotion rules (from v6 PDF Layer 7):
  rfq_submitted    → rfq_draft           after 7 days no action
  quote_ready      → rfq_submitted       after 14 days no seller response
  hot_in_market    → known_account_interest after 3 days
  engaged_person   → known_person_interest  after 21 days
  engaged_account  → known_account_interest after 30 days
  known_account_interest → intent_only   after 60 days
  known_person_interest  → intent_only   after 60 days
  intent_only      → visit_only          after 90 days
  trade_buyer_candidate → intent_only    after 45 days
  strategic_account_watch → visit_only   after 180 days
  reactivation_candidate → intent_only   after 30 days

Leads that are suppressed, blocked, or already in a terminal state
(suppressed_noise, visit_only, active_exporter) are never downgraded.

Run via:  python3 main.py downgrade-leads  (add to main.py)
"""
from datetime import datetime, timezone
from app.core.logger import info, ok, banner


# Days of inactivity before demotion (from → to → days)
DEMOTION_RULES = [
    # (state_from, state_to, stale_days, new_score)
    ("rfq_submitted",         "rfq_draft",              7,   60),
    ("quote_ready",           "rfq_submitted",          14,  75),
    ("hot_in_market",         "known_account_interest", 3,   60),
    ("engaged_person",        "known_person_interest",  21,  65),
    ("engaged_account",       "known_account_interest", 30,  60),
    ("known_account_interest","intent_only",            60,  50),
    ("known_person_interest", "intent_only",            60,  50),
    ("intent_only",           "visit_only",             90,  35),
    ("trade_buyer_candidate", "intent_only",            45,  50),
    ("strategic_account_watch","visit_only",            180, 30),
    ("reactivation_candidate","intent_only",            30,  50),
    ("competitor_displacement","known_account_interest",30,  60),
    ("active_importer",       "intent_only",            90,  50),
]

VISIBILITY_FOR_TYPE = {
    "rfq_draft":              "push_notify",
    "rfq_submitted":          "instant_alert",
    "known_account_interest": "feed",
    "known_person_interest":  "feed",
    "intent_only":            "feed",
    "visit_only":             "count_only",
}

# Lead types that are never downgraded
TERMINAL_TYPES = {
    "suppressed_noise", "visit_only", "active_exporter", "blocked",
    "partner_chain_opportunity",
}


class DowngradeEngine:

    def __init__(self, neo, settings):
        self.neo      = neo
        self.settings = settings

    def run(self) -> dict:
        banner("DowngradeEngine: Lead Staleness Demotion")
        total_demoted = 0
        results = {}

        for (state_from, state_to, stale_days, new_score) in DEMOTION_RULES:
            if state_from in TERMINAL_TYPES:
                continue
            n = self._demote(state_from, state_to, stale_days, new_score)
            if n:
                results[f"{state_from}→{state_to}"] = n
                total_demoted += n
                info(f"  {state_from} → {state_to}: {n} leads demoted (stale {stale_days}d)")

        ok(f"DowngradeEngine: {total_demoted} leads demoted across {len(results)} transitions")
        return {"demoted": total_demoted, "by_transition": results}

    def _demote(self, state_from: str, state_to: str,
                stale_days: int, new_score: int) -> int:
        """
        Demote leads of type `state_from` that haven't had a transition or
        activity update in `stale_days` days. Creates LeadStateHistory records.
        """
        now = datetime.now(timezone.utc).isoformat()
        new_visibility = VISIBILITY_FOR_TYPE.get(state_to, "feed")
        rows = self.neo.run("""
            MATCH (l:Lead {lead_type: $from_type})
            WHERE coalesce(l.suppressed, false) = false
              AND coalesce(l.distribution_status, '') <> 'suppressed'
              AND (
                l.last_transition_at IS NULL
                OR datetime(l.last_transition_at) < datetime() - duration({days: $days})
              )
              AND (
                l.classified_at IS NULL
                OR datetime(l.classified_at) < datetime() - duration({days: $days})
              )
            WITH l LIMIT 500
            SET l.lead_type        = $to_type,
                l.score_final      = $new_score,
                l.visibility_level = $new_visibility,
                l.previous_lead_type = $from_type,
                l.last_transition_at = $now,
                l.transition_count  = coalesce(l.transition_count, 0) + 1
            WITH l
            CREATE (h:LeadStateHistory {
                history_uid:         'lsh:demotion:' + l.lead_uid + ':' + $now,
                lead_uid:            l.lead_uid,
                state_from:          $from_type,
                state_to:            $to_type,
                reason:              'staleness_' + toString($days) + 'd',
                triggered_by:        'DowngradeEngine',
                score_at_transition: $new_score,
                transitioned_at:     $now
            })
            MERGE (l)-[:HAS_STATE_HISTORY]->(h)
            RETURN count(l) AS c
        """, {
            "from_type":      state_from,
            "to_type":        state_to,
            "days":           stale_days,
            "new_score":      new_score,
            "new_visibility": new_visibility,
            "now":            now,
        })
        return rows[0]["c"] if rows else 0
