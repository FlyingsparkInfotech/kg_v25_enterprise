"""
SellerRankingEngine — 6-dimensional seller priority scoring per lead.

For each lead-seller pair, computes a rank_score from 6 dimensions:
  1. fit_score × hs_depth          — product category match quality
  2. lane_coverage                 — seller's target country breadth
  3. win_rate_for_lead_type        — historical win rate for this lead type
  4. response_speed_score          — average response time vs SLA
  5. capacity_score                — available capacity (not overloaded)
  6. relationship_depth            — prior interactions with this account

Replaces the simple capacity-only check in DistributionEngine.
Called by DistributionEngine._match() when rank_sellers=True.

Usage:
    engine = SellerRankingEngine(neo, settings)
    ranked = engine.rank_sellers_for_lead(lead, sellers)
    # returns sellers sorted by rank_score desc
"""
from app.core.logger import info


# Dimension weights (must sum to 1.0)
WEIGHTS = {
    "fit_score":        0.30,
    "lane_coverage":    0.15,
    "win_rate":         0.20,
    "response_speed":   0.15,
    "capacity":         0.10,
    "relationship":     0.10,
}

# SLA hours by visibility level (response expected within)
SLA_HOURS = {
    "instant_alert": 4,
    "push_notify":   24,
    "priority":      48,
    "feed":          168,
    "watchlist":     720,
    "count_only":    9999,
}


class SellerRankingEngine:

    def __init__(self, neo, settings):
        self.neo      = neo
        self.settings = settings
        self._stats   = self._load_seller_stats()

    def _load_seller_stats(self) -> dict:
        """Load per-seller win rates and response speeds from Neo4j."""
        rows = self.neo.run("""
            MATCH (s:Seller)
            OPTIONAL MATCH (s)-[:ASSIGNED]->(a:SellerLeadAssignment)
            OPTIONAL MATCH (cf:ConversionFact {seller_id: s.seller_id})
            WITH s,
                 count(a)                            AS total_assigned,
                 count(cf)                           AS total_converted,
                 avg(CASE WHEN a.response_hours IS NOT NULL
                          THEN a.response_hours ELSE null END) AS avg_response_hours,
                 s.win_rate        AS stored_win_rate,
                 s.avg_response_h  AS stored_response
            RETURN s.seller_id         AS seller_id,
                   total_assigned,
                   total_converted,
                   CASE WHEN total_assigned > 0
                        THEN toFloat(total_converted) / total_assigned
                        ELSE coalesce(stored_win_rate, 0.0) END AS win_rate,
                   coalesce(avg_response_hours, stored_response, 48.0) AS avg_response_h
        """) or []

        stats = {}
        for r in rows:
            stats[r["seller_id"]] = {
                "win_rate":       float(r.get("win_rate") or 0.0),
                "avg_response_h": float(r.get("avg_response_h") or 48.0),
                "total_assigned": int(r.get("total_assigned") or 0),
            }
        return stats

    def rank_sellers_for_lead(self, lead: dict, sellers: list) -> list:
        """
        Rank sellers for a specific lead. Returns sellers list sorted by
        rank_score descending, with rank_score and rank_reason added to each.
        """
        lead_type    = lead.get("lead_type", "")
        hs_chapter   = (lead.get("hs_code") or "")[:2]
        buyer_country= (lead.get("buyer_country") or "").upper()
        visibility   = lead.get("visibility_level", "feed")
        sla_hours    = SLA_HOURS.get(visibility, 48)

        scored = []
        for seller in sellers:
            rank_score, reasons = self._score_seller(
                seller, hs_chapter, buyer_country, lead_type, sla_hours
            )
            s = dict(seller)
            s["rank_score"]  = rank_score
            s["rank_reason"] = reasons
            scored.append(s)

        scored.sort(key=lambda x: x["rank_score"], reverse=True)
        return scored

    def _score_seller(self, seller: dict, hs_chapter: str,
                      buyer_country: str, lead_type: str, sla_hours: int):
        sid    = seller["seller_id"]
        stats  = self._stats.get(sid, {})

        # 1. Fit score: hs chapter depth × match quality
        hs_chapters = list(seller.get("hs_chapters") or [])
        if hs_chapters:
            if hs_chapter in hs_chapters:
                fit_s = 1.0
            elif any(hs_chapter.startswith(ch[:1]) for ch in hs_chapters if ch):
                fit_s = 0.6
            else:
                fit_s = 0.0
        else:
            fit_s = 0.5  # open market seller

        # 2. Lane coverage: breadth of target countries
        target_countries = list(seller.get("target_countries") or [])
        if not target_countries or "*" in target_countries:
            lane_s = 0.5
        elif buyer_country in target_countries:
            lane_s = 1.0
        else:
            lane_s = 0.0

        # 3. Win rate for this lead type (use overall win rate as proxy)
        win_rate = float(stats.get("win_rate") or 0.0)
        win_s    = min(win_rate * 2.5, 1.0)  # normalise: 40% win rate = 1.0

        # 4. Response speed vs SLA
        avg_resp = float(stats.get("avg_response_h") or 48.0)
        if avg_resp <= sla_hours * 0.25:
            resp_s = 1.0
        elif avg_resp <= sla_hours * 0.5:
            resp_s = 0.75
        elif avg_resp <= sla_hours:
            resp_s = 0.5
        else:
            resp_s = 0.1  # consistently misses SLA

        # 5. Capacity (not overloaded)
        max_daily  = int(seller.get("max_leads_per_day") or 50)
        leads_today= int(seller.get("leads_today") or 0)
        remaining  = max(0, max_daily - leads_today)
        cap_s      = min(remaining / max(max_daily, 1), 1.0)

        # 6. Relationship depth (prior assignments = trust signal)
        total_assigned = int(stats.get("total_assigned") or 0)
        rel_s = min(total_assigned / 100.0, 1.0)  # 100 past assigns = max

        rank_score = (
            WEIGHTS["fit_score"]     * fit_s  +
            WEIGHTS["lane_coverage"] * lane_s +
            WEIGHTS["win_rate"]      * win_s  +
            WEIGHTS["response_speed"]* resp_s +
            WEIGHTS["capacity"]      * cap_s  +
            WEIGHTS["relationship"]  * rel_s
        )

        reasons = (
            f"fit={fit_s:.2f} lane={lane_s:.2f} win={win_s:.2f} "
            f"resp={resp_s:.2f} cap={cap_s:.2f} rel={rel_s:.2f}"
        )
        return round(rank_score, 4), reasons

    def update_seller_stats(self):
        """
        Recompute and store win_rate and avg_response_h on Seller nodes.
        Run as a daily batch job.
        """
        self.neo.run("""
            MATCH (s:Seller)
            OPTIONAL MATCH (s)-[:ASSIGNED]->(a:SellerLeadAssignment)
            OPTIONAL MATCH (cf:ConversionFact {seller_id: s.seller_id})
            WITH s,
                 count(a)  AS total_assigned,
                 count(cf) AS total_converted,
                 avg(CASE WHEN a.response_hours IS NOT NULL
                          THEN a.response_hours ELSE null END) AS avg_resp
            SET s.win_rate       = CASE WHEN total_assigned > 0
                                        THEN toFloat(total_converted) / total_assigned
                                        ELSE 0.0 END,
                s.avg_response_h = coalesce(avg_resp, 48.0),
                s.total_assigned = total_assigned,
                s.total_converted= total_converted,
                s.stats_updated_at = toString(datetime())
        """)
        info("SellerRankingEngine: seller stats updated on all Seller nodes")
