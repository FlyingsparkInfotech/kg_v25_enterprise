// ─── SUPPLIER SWITCH LEAD DETECTION SCHEMA ───────────────────────────────────
// Run after constraints.cypher and indexes.cypher

// UNIQUE CONSTRAINTS
CREATE CONSTRAINT trade_rel_unique IF NOT EXISTS
  FOR (tr:TradeRelationship) REQUIRE tr.rel_id IS UNIQUE;

CREATE CONSTRAINT snapshot_unique IF NOT EXISTS
  FOR (snap:RelationshipSnapshot) REQUIRE snap.snapshot_id IS UNIQUE;

CREATE CONSTRAINT switch_opp_unique IF NOT EXISTS
  FOR (opp:SupplierSwitchOpportunity) REQUIRE opp.opportunity_id IS UNIQUE;

CREATE CONSTRAINT supplier_match_unique IF NOT EXISTS
  FOR (sm:SupplierMatch) REQUIRE sm.match_id IS UNIQUE;

CREATE CONSTRAINT switch_lead_unique IF NOT EXISTS
  FOR (sl:SwitchLead) REQUIRE sl.lead_id IS UNIQUE;

// PERFORMANCE INDEXES
CREATE INDEX trade_rel_hs_code IF NOT EXISTS
  FOR (tr:TradeRelationship) ON (tr.hs_code);

CREATE INDEX trade_rel_health_status IF NOT EXISTS
  FOR (tr:TradeRelationship) ON (tr.health_status);

CREATE INDEX trade_rel_buyer IF NOT EXISTS
  FOR (tr:TradeRelationship) ON (tr.buyer_org_id);

CREATE INDEX trade_rel_supplier IF NOT EXISTS
  FOR (tr:TradeRelationship) ON (tr.supplier_org_id);

CREATE INDEX trade_rel_switch_prob IF NOT EXISTS
  FOR (tr:TradeRelationship) ON (tr.switch_probability);

CREATE INDEX snapshot_year_month IF NOT EXISTS
  FOR (snap:RelationshipSnapshot) ON (snap.year_month);

CREATE INDEX switch_opp_status IF NOT EXISTS
  FOR (opp:SupplierSwitchOpportunity) ON (opp.status);

CREATE INDEX switch_opp_stress IF NOT EXISTS
  FOR (opp:SupplierSwitchOpportunity) ON (opp.stress_score);

CREATE INDEX switch_lead_priority IF NOT EXISTS
  FOR (sl:SwitchLead) ON (sl.lead_priority);

CREATE INDEX switch_lead_status IF NOT EXISTS
  FOR (sl:SwitchLead) ON (sl.status);

CREATE INDEX switch_lead_supplier IF NOT EXISTS
  FOR (sl:SwitchLead) ON (sl.candidate_supplier_org_id);
