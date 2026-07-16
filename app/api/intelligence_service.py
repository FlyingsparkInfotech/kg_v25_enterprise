"""
IntelligenceService: Exposes KG-exclusive features that have no equivalent in the
GoGlo admin UI:
  - TradeRelationship nodes (real Trademo customs data with $ volumes)
  - BuyerProfile nodes (17,747 behavioural profiles from CRM2)
  - SupplierSwitchOpportunity nodes (rule-based vs ML detection comparison)
  - Pipeline run history
"""

from datetime import datetime, timezone
from app.core.logger import warn


class IntelligenceService:

    def __init__(self, neo, pg_conn=None):
        self.neo     = neo
        self.pg_conn = pg_conn

    # ══════════════════════════════════════════════════════════════════════════
    # TRADE RELATIONSHIPS
    # ══════════════════════════════════════════════════════════════════════════

    def get_trade_relationships(
        self,
        limit: int = 50,
        min_monthly_volume: float = 0.0,
        health_status: str = None,
        buyer_country: str = None,
    ) -> list:
        filters = ["WHERE tr.buyer_monthly_volume >= $min_vol"]
        params  = {"limit": limit, "min_vol": min_monthly_volume}

        if health_status:
            filters.append("AND toUpper(coalesce(tr.health_status, tr.rule_health_status, '')) = toUpper($hs)")
            params["hs"] = health_status
        if buyer_country:
            filters.append("AND toUpper(coalesce(tr.buyer_country, '')) = toUpper($bc)")
            params["bc"] = buyer_country

        cypher = f"""
            MATCH (tr:TradeRelationship)
            {chr(10).join(filters)}
            OPTIONAL MATCH (opp:SupplierSwitchOpportunity)-[:FROM_RELATIONSHIP]->(tr)
            RETURN
                tr.relationship_id          AS relationship_id,
                tr.buyer_org_id             AS buyer_org_id,
                tr.buyer_name               AS buyer_name,
                tr.supplier_org_id          AS supplier_org_id,
                tr.supplier_name            AS supplier_name,
                tr.hs_code                  AS hs_code,
                tr.buyer_monthly_volume     AS monthly_volume,
                tr.total_shipments          AS total_shipments,
                tr.first_shipment_date      AS first_shipment_date,
                tr.last_shipment_date       AS last_shipment_date,
                tr.relationship_age_months  AS age_months,
                tr.health_score             AS health_score,
                tr.health_status            AS health_status,
                tr.rule_health_status       AS rule_health_status,
                tr.rule_stress_score        AS rule_stress_score,
                tr.rule_stress_reason       AS rule_stress_reason,
                tr.rule_days_silent         AS days_silent,
                tr.rule_vol_drop_pct        AS vol_drop_pct,
                tr.buyer_country            AS buyer_country,
                count(opp)                  AS switch_opportunities
            ORDER BY tr.buyer_monthly_volume DESC
            LIMIT $limit
        """
        rows = self.neo.run(cypher, params) or []
        return [self._format_trade_relationship(r) for r in rows]

    def get_trade_relationship(self, relationship_id: str) -> dict | None:
        cypher = """
            MATCH (tr:TradeRelationship {relationship_id: $rid})
            OPTIONAL MATCH (opp:SupplierSwitchOpportunity)-[:FROM_RELATIONSHIP]->(tr)
            RETURN
                tr.relationship_id          AS relationship_id,
                tr.buyer_org_id             AS buyer_org_id,
                tr.buyer_name               AS buyer_name,
                tr.supplier_org_id          AS supplier_org_id,
                tr.supplier_name            AS supplier_name,
                tr.hs_code                  AS hs_code,
                tr.buyer_monthly_volume     AS monthly_volume,
                tr.total_shipments          AS total_shipments,
                tr.first_shipment_date      AS first_shipment_date,
                tr.last_shipment_date       AS last_shipment_date,
                tr.relationship_age_months  AS age_months,
                tr.health_score             AS health_score,
                tr.health_status            AS health_status,
                tr.rule_health_status       AS rule_health_status,
                tr.rule_stress_score        AS rule_stress_score,
                tr.rule_stress_reason       AS rule_stress_reason,
                tr.rule_days_silent         AS days_silent,
                tr.rule_vol_drop_pct        AS vol_drop_pct,
                tr.buyer_country            AS buyer_country,
                collect(opp.opportunity_id) AS opportunity_ids
        """
        rows = self.neo.run(cypher, {"rid": relationship_id}) or []
        if not rows:
            return None
        return self._format_trade_relationship(rows[0])

    def get_trade_stats(self) -> dict:
        cypher = """
            MATCH (tr:TradeRelationship)
            WITH
                count(tr)                                AS total,
                sum(tr.buyer_monthly_volume)             AS total_monthly_volume,
                avg(tr.buyer_monthly_volume)             AS avg_monthly_volume,
                max(tr.buyer_monthly_volume)             AS max_monthly_volume,
                avg(tr.health_score)                     AS avg_health_score,
                sum(CASE WHEN coalesce(tr.rule_health_status, tr.health_status) = 'CHURNED'  THEN 1 ELSE 0 END) AS churned,
                sum(CASE WHEN coalesce(tr.rule_health_status, tr.health_status) = 'DORMANT'  THEN 1 ELSE 0 END) AS dormant,
                sum(CASE WHEN coalesce(tr.rule_health_status, tr.health_status) = 'STRESSED' THEN 1 ELSE 0 END) AS stressed,
                sum(CASE WHEN coalesce(tr.rule_health_status, tr.health_status) = 'IRREGULAR'THEN 1 ELSE 0 END) AS irregular,
                sum(CASE WHEN coalesce(tr.rule_health_status, tr.health_status) = 'HEALTHY'  THEN 1 ELSE 0 END) AS healthy
            RETURN total, total_monthly_volume, avg_monthly_volume, max_monthly_volume,
                   avg_health_score, churned, dormant, stressed, irregular, healthy
        """
        rows = self.neo.run(cypher, {}) or [{}]
        r = rows[0]
        return {
            "total_relationships":       int(r.get("total") or 0),
            "total_monthly_volume_usd":  round(float(r.get("total_monthly_volume") or 0), 2),
            "avg_monthly_volume_usd":    round(float(r.get("avg_monthly_volume") or 0), 2),
            "max_monthly_volume_usd":    round(float(r.get("max_monthly_volume") or 0), 2),
            "avg_health_score":          round(float(r.get("avg_health_score") or 0), 1),
            "health_breakdown": {
                "CHURNED":   int(r.get("churned") or 0),
                "DORMANT":   int(r.get("dormant") or 0),
                "STRESSED":  int(r.get("stressed") or 0),
                "IRREGULAR": int(r.get("irregular") or 0),
                "HEALTHY":   int(r.get("healthy") or 0),
            },
            "data_source": "Trademo customs records (verified import/export filings)",
        }

    def _format_trade_relationship(self, r: dict) -> dict:
        return {
            "relationship_id":   r.get("relationship_id", ""),
            "buyer": {
                "org_id":  r.get("buyer_org_id", ""),
                "name":    r.get("buyer_name", ""),
                "country": r.get("buyer_country", ""),
            },
            "supplier": {
                "org_id": r.get("supplier_org_id", ""),
                "name":   r.get("supplier_name", ""),
            },
            "trade": {
                "hs_code":            r.get("hs_code", ""),
                "monthly_volume_usd": round(float(r.get("monthly_volume") or 0), 2),
                "total_shipments":    int(r.get("total_shipments") or 0),
                "first_shipment":     r.get("first_shipment_date", ""),
                "last_shipment":      r.get("last_shipment_date", ""),
                "age_months":         int(r.get("age_months") or 0),
            },
            "health": {
                "ml_health_status":   r.get("health_status", ""),
                "ml_health_score":    round(float(r.get("health_score") or 0), 1) if r.get("health_score") is not None else None,
                "rule_health_status": r.get("rule_health_status", ""),
                "rule_stress_score":  int(r.get("rule_stress_score") or 0),
                "rule_stress_reason": r.get("rule_stress_reason", ""),
                "days_silent":        int(r.get("days_silent") or 0),
                "vol_drop_pct":       round(float(r.get("vol_drop_pct") or 0), 1),
            },
            "switch_opportunities": int(r.get("switch_opportunities") or len(r.get("opportunity_ids") or [])),
        }

    # ══════════════════════════════════════════════════════════════════════════
    # BUYER PROFILES
    # ══════════════════════════════════════════════════════════════════════════

    def get_buyer_profiles(
        self,
        limit: int = 50,
        min_score: int = 0,
        has_enquiries: bool = None,
        intent_band: str = None,   # high / medium / low
    ) -> list:
        filters = ["WHERE bp.behavioral_score >= $min_score"]
        params  = {"limit": limit, "min_score": min_score}

        if has_enquiries is True:
            filters.append("AND bp.total_enquiries > 0")
        elif has_enquiries is False:
            filters.append("AND (bp.total_enquiries IS NULL OR bp.total_enquiries = 0)")

        if intent_band == "high":
            filters.append("AND bp.behavioral_score >= 70")
        elif intent_band == "medium":
            filters.append("AND bp.behavioral_score >= 50 AND bp.behavioral_score < 70")
        elif intent_band == "low":
            filters.append("AND bp.behavioral_score < 50")

        cypher = f"""
            MATCH (bp:BuyerProfile)
            {chr(10).join(filters)}
            RETURN
                bp.user_id              AS user_id,
                bp.behavioral_score     AS behavioral_score,
                bp.intent_score         AS intent_score,
                bp.total_clicks         AS total_clicks,
                bp.total_sessions       AS total_sessions,
                bp.total_scrolls        AS total_scrolls,
                bp.total_pageviews      AS total_pageviews,
                bp.total_enquiries      AS total_enquiries,
                bp.total_searches       AS total_searches,
                bp.total_messages       AS total_messages,
                bp.top_clicks           AS top_clicks,
                bp.has_message          AS has_message,
                bp.max_scroll_pct       AS max_scroll_pct,
                bp.last_seen            AS last_seen,
                bp.created_at           AS created_at
            ORDER BY bp.behavioral_score DESC, bp.total_enquiries DESC
            LIMIT $limit
        """
        rows = self.neo.run(cypher, params) or []
        return [self._format_buyer_profile(r) for r in rows]

    def get_buyer_profile(self, user_id: str) -> dict | None:
        cypher = """
            MATCH (bp:BuyerProfile {user_id: $uid})
            OPTIONAL MATCH (l:Lead {buyer_user_id: $uid})
            RETURN
                bp.user_id              AS user_id,
                bp.behavioral_score     AS behavioral_score,
                bp.intent_score         AS intent_score,
                bp.total_clicks         AS total_clicks,
                bp.total_sessions       AS total_sessions,
                bp.total_scrolls        AS total_scrolls,
                bp.total_pageviews      AS total_pageviews,
                bp.total_enquiries      AS total_enquiries,
                bp.total_searches       AS total_searches,
                bp.total_messages       AS total_messages,
                bp.top_clicks           AS top_clicks,
                bp.has_message          AS has_message,
                bp.max_scroll_pct       AS max_scroll_pct,
                bp.last_seen            AS last_seen,
                bp.created_at           AS created_at,
                collect(DISTINCT l.lead_type) AS lead_types,
                max(l.score_final)            AS max_lead_score
        """
        rows = self.neo.run(cypher, {"uid": user_id}) or []
        if not rows:
            return None
        r = rows[0]
        profile = self._format_buyer_profile(r)
        profile["lead_types"]    = r.get("lead_types") or []
        profile["max_lead_score"] = float(r.get("max_lead_score") or 0)
        return profile

    def get_buyer_profile_stats(self) -> dict:
        cypher = """
            MATCH (bp:BuyerProfile)
            RETURN
                count(bp)                                                     AS total,
                avg(bp.behavioral_score)                                      AS avg_score,
                sum(CASE WHEN bp.behavioral_score >= 70 THEN 1 ELSE 0 END)   AS high_intent,
                sum(CASE WHEN bp.behavioral_score >= 50
                          AND bp.behavioral_score < 70 THEN 1 ELSE 0 END)    AS medium_intent,
                sum(CASE WHEN bp.behavioral_score < 50 THEN 1 ELSE 0 END)    AS low_intent,
                sum(CASE WHEN bp.total_enquiries > 0 THEN 1 ELSE 0 END)      AS with_enquiries,
                sum(CASE WHEN bp.has_message = true THEN 1 ELSE 0 END)       AS chatted_with_sellers,
                sum(CASE WHEN bp.intent_score >= 6 THEN 1 ELSE 0 END)        AS strong_clickers,
                sum(CASE WHEN bp.max_scroll_pct >= 75 THEN 1 ELSE 0 END)     AS deep_readers,
                sum(bp.total_clicks)                                          AS total_platform_clicks,
                sum(bp.total_enquiries)                                       AS total_enquiries
        """
        rows = self.neo.run(cypher, {}) or [{}]
        r = rows[0]
        return {
            "total_profiles":         int(r.get("total") or 0),
            "avg_behavioral_score":   round(float(r.get("avg_score") or 0), 1),
            "intent_bands": {
                "high_intent_score_70plus":    int(r.get("high_intent") or 0),
                "medium_intent_score_50_69":   int(r.get("medium_intent") or 0),
                "low_intent_score_under_50":   int(r.get("low_intent") or 0),
            },
            "engagement": {
                "buyers_with_enquiries":       int(r.get("with_enquiries") or 0),
                "chatted_with_sellers":        int(r.get("chatted_with_sellers") or 0),
                "strong_clickers_intent_6plus": int(r.get("strong_clickers") or 0),
                "deep_readers_scroll_75plus":  int(r.get("deep_readers") or 0),
            },
            "totals": {
                "platform_clicks":   int(r.get("total_platform_clicks") or 0),
                "total_enquiries":   int(r.get("total_enquiries") or 0),
            },
            "data_source": "CRM2 tracking tables (sessions, clicks, scrolls, enquiries, searches, messages)",
        }

    def _format_buyer_profile(self, r: dict) -> dict:
        score = int(r.get("behavioral_score") or 0)
        if score >= 70:
            intent_band = "high"
        elif score >= 50:
            intent_band = "medium"
        else:
            intent_band = "low"

        return {
            "user_id":          r.get("user_id", ""),
            "behavioral_score": score,
            "intent_band":      intent_band,
            "intent_score":     int(r.get("intent_score") or 0),
            "activity": {
                "total_clicks":     int(r.get("total_clicks") or 0),
                "total_sessions":   int(r.get("total_sessions") or 0),
                "total_scrolls":    int(r.get("total_scrolls") or 0),
                "total_pageviews":  int(r.get("total_pageviews") or 0),
                "total_enquiries":  int(r.get("total_enquiries") or 0),
                "total_searches":   int(r.get("total_searches") or 0),
                "total_messages":   int(r.get("total_messages") or 0),
                "max_scroll_pct":   round(float(r.get("max_scroll_pct") or 0), 1),
                "has_messaged_seller": bool(r.get("has_message")),
            },
            "top_click_buttons": r.get("top_clicks", ""),
            "last_seen":        r.get("last_seen", ""),
            "created_at":       r.get("created_at", ""),
        }

    # ══════════════════════════════════════════════════════════════════════════
    # SWITCH OPPORTUNITIES — RULE VS ML COMPARISON
    # ══════════════════════════════════════════════════════════════════════════

    def get_switch_opportunities(
        self,
        limit: int = 50,
        detection_method: str = None,   # rule_based / ml_based / None (all)
        min_health_score: float = None,
    ) -> list:
        filters = []
        params  = {"limit": limit}

        if detection_method:
            filters.append("WHERE opp.detection_method = $dm")
            params["dm"] = detection_method
        if min_health_score is not None:
            prefix = "AND" if filters else "WHERE"
            filters.append(f"{prefix} opp.health_score >= $mhs")
            params["mhs"] = min_health_score

        cypher = f"""
            MATCH (opp:SupplierSwitchOpportunity)
            {chr(10).join(filters)}
            OPTIONAL MATCH (tr:TradeRelationship)-[:TRIGGERED]->(opp)
            RETURN
                opp.opportunity_id          AS opportunity_id,
                opp.existing_supplier_name  AS supplier_name,
                opp.buyer_org_id            AS buyer_org_id,
                opp.health_score            AS health_score,
                opp.health_status           AS health_status,
                opp.switch_probability      AS switch_probability,
                opp.detection_method        AS detection_method,
                opp.detected_at             AS detected_at,
                tr.buyer_name               AS buyer_name,
                tr.buyer_monthly_volume     AS monthly_volume,
                tr.rule_stress_reason       AS rule_stress_reason,
                tr.rule_stress_score        AS rule_stress_score,
                tr.rule_health_status       AS rule_health_status
            ORDER BY opp.health_score ASC, opp.switch_probability DESC
            LIMIT $limit
        """
        rows = self.neo.run(cypher, params) or []
        return [self._format_opportunity(r) for r in rows]

    def get_detection_comparison(self) -> dict:
        """Side-by-side stats: how many opportunities each engine found, overlap, unique."""
        cypher = """
            MATCH (opp:SupplierSwitchOpportunity)
            RETURN
                opp.detection_method        AS method,
                count(opp)                  AS total,
                avg(opp.health_score)       AS avg_health,
                avg(opp.switch_probability) AS avg_prob,
                sum(CASE WHEN opp.health_status = 'CHURNED'  THEN 1 ELSE 0 END) AS churned,
                sum(CASE WHEN opp.health_status = 'DORMANT'  THEN 1 ELSE 0 END) AS dormant,
                sum(CASE WHEN opp.health_status = 'STRESSED' THEN 1 ELSE 0 END) AS stressed
        """
        rows = self.neo.run(cypher, {}) or []

        result = {
            "rule_based": {},
            "ml_based":   {},
            "summary": {},
        }
        total_rule = 0
        total_ml   = 0

        for r in rows:
            method = r.get("method") or "unknown"
            entry  = {
                "total":              int(r.get("total") or 0),
                "avg_health_score":   round(float(r.get("avg_health") or 0), 1),
                "avg_switch_prob_pct": round(float(r.get("avg_prob") or 0) * 100, 1),
                "churned":            int(r.get("churned") or 0),
                "dormant":            int(r.get("dormant") or 0),
                "stressed":           int(r.get("stressed") or 0),
            }
            if method == "rule_based":
                result["rule_based"] = entry
                total_rule = entry["total"]
            elif method == "ml_based":
                result["ml_based"] = entry
                total_ml = entry["total"]

        result["summary"] = {
            "rule_based_total":  total_rule,
            "ml_based_total":    total_ml,
            "combined_total":    total_rule + total_ml,
            "rule_only_note":    "Rule engine fires on 7 explicit thresholds — works at any data volume",
            "ml_only_note":      "IsolationForest catches subtle multi-signal anomalies",
            "both_run_together": True,
        }
        return result

    def _format_opportunity(self, r: dict) -> dict:
        return {
            "opportunity_id":   r.get("opportunity_id", ""),
            "buyer": {
                "org_id":          r.get("buyer_org_id", ""),
                "name":            r.get("buyer_name", ""),
                "monthly_volume":  round(float(r.get("monthly_volume") or 0), 2),
            },
            "at_risk_supplier":  r.get("supplier_name", ""),
            "detection": {
                "method":             r.get("detection_method", ""),
                "detected_at":        r.get("detected_at", ""),
                "ml_health_score":    round(float(r.get("health_score") or 0), 1) if r.get("health_score") is not None else None,
                "ml_health_status":   r.get("health_status", ""),
                "ml_switch_prob_pct": round(float(r.get("switch_probability") or 0) * 100, 1),
                "rule_health_status": r.get("rule_health_status", ""),
                "rule_stress_score":  int(r.get("rule_stress_score") or 0),
                "rule_stress_reason": r.get("rule_stress_reason", ""),
            },
        }

    # ══════════════════════════════════════════════════════════════════════════
    # PIPELINE STATUS
    # ══════════════════════════════════════════════════════════════════════════

    def get_pipeline_status(self) -> dict:
        """
        Aggregate stats about pipeline outputs — counts of each node type,
        last-run timestamps, and a health summary.
        """
        cypher = """
            CALL {
                MATCH (n:Lead)              RETURN 'Lead'              AS label, count(n) AS cnt
                UNION ALL
                MATCH (n:BuyerProfile)      RETURN 'BuyerProfile'      AS label, count(n) AS cnt
                UNION ALL
                MATCH (n:TradeRelationship) RETURN 'TradeRelationship' AS label, count(n) AS cnt
                UNION ALL
                MATCH (n:SwitchLead)        RETURN 'SwitchLead'        AS label, count(n) AS cnt
                UNION ALL
                MATCH (n:SupplierSwitchOpportunity) RETURN 'SupplierSwitchOpportunity' AS label, count(n) AS cnt
                UNION ALL
                MATCH (n:Seller)            RETURN 'Seller'            AS label, count(n) AS cnt
                UNION ALL
                MATCH (n:SellerLeadAssignment) RETURN 'SellerLeadAssignment' AS label, count(n) AS cnt
                UNION ALL
                MATCH (n:ConversionFact)    RETURN 'ConversionFact'    AS label, count(n) AS cnt
            }
            RETURN label, cnt ORDER BY label
        """
        rows = self.neo.run(cypher, {}) or []
        node_counts = {r.get("label"): int(r.get("cnt") or 0) for r in rows}

        # Last lead created timestamp
        ts_row = self.neo.run(
            "MATCH (l:Lead) RETURN max(l.created_at) AS last_lead_ts", {}
        ) or [{}]
        last_lead_ts = ts_row[0].get("last_lead_ts", "") if ts_row else ""

        # Lead type breakdown
        lt_rows = self.neo.run(
            "MATCH (l:Lead) RETURN l.lead_type AS t, count(l) AS c ORDER BY c DESC", {}
        ) or []
        lead_types = {r.get("t"): int(r.get("c") or 0) for r in lt_rows}

        # Detection method breakdown for switch opportunities
        dm_rows = self.neo.run(
            "MATCH (o:SupplierSwitchOpportunity) RETURN o.detection_method AS m, count(o) AS c", {}
        ) or []
        detection_counts = {r.get("m"): int(r.get("c") or 0) for r in dm_rows}

        return {
            "node_counts":           node_counts,
            "last_lead_created_at":  last_lead_ts,
            "lead_type_breakdown":   lead_types,
            "switch_detection_breakdown": detection_counts,
            "pipeline_stages": [
                {"stage": "load-base",               "description": "Governance, UI, CRM, G-I data loaded"},
                {"stage": "init",                    "description": "Neo4j constraints and indexes created"},
                {"stage": "load-postgres",           "description": "Trademo, ZoomInfo, SKG data from Postgres"},
                {"stage": "derive-signals",          "description": "UI/CRM signals derived into Lead nodes"},
                {"stage": "classify-platform-leads", "description": "15 lead types classified and scored"},
                {"stage": "build-buyer-profiles",    "description": "BuyerProfile nodes built from CRM2 tracking"},
                {"stage": "build-trade-graph",       "description": "TradeRelationship nodes from Trademo customs data"},
                {"stage": "detect-stress-rules",     "description": "Rule-based supplier stress detection (Stage 3A)"},
                {"stage": "detect-switch-leads",     "description": "ML IsolationForest stress detection (Stage 3B)"},
                {"stage": "distribute",              "description": "Leads assigned to matched sellers"},
                {"stage": "process-feedback",        "description": "Seller feedback threshold tuning"},
                {"stage": "track-conversions",       "description": "ConversionFact nodes recorded"},
                {"stage": "billing-reset",           "description": "Monthly seller credit allocations reset"},
                {"stage": "validate",                "description": "Pipeline integrity checks"},
            ],
            "data_sources": [
                "Trademo (customs import/export records)",
                "ZoomInfo (company and contact enrichment)",
                "GoGlo CRM2 (buyer sessions, clicks, scrolls, enquiries, searches)",
                "GoGlo UI (RFQs, enquiries, quotes, platform events)",
                "SKG (supply chain knowledge graph)",
            ],
        }
