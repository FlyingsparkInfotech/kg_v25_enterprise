#!/usr/bin/env python3
"""
ui_data_injector_v5_governance.py

Entity-driven UI+CRM injector for Neo4j with 4-Layer Governance Model.
- L1: TAXONOMY (Identity Layer) - TaxonomyDef nodes
- L2: ENTITY DEFINITIONS (Schema Layer) - EntityDef nodes  
- L3: RULES (Governance Layer) - RuleDef nodes with dynamic execution
- L4: INSTANCES (Data Layer) - Actual business data with validation audit trail

NEW IN V5:
- Complete governance chain: L2→L1 (BELONGS_TO_TAXONOMY), L3→L2 (VALIDATES_ENTITY)
- Rule Execution Engine that triggers SystemCheckAutomatically queries on instance CRUD
- Confidence gate enforcement at instance creation
- Rule validation results stored as :RuleValidationResult nodes

Design decisions:
- ProductApplication & UseCase are independent nodes.
- Keywords are independent nodes.
- PipelineStage is an independent node.
- AutoQuotation is linked to RFQ.

Requires:
  pip install neo4j pymysql pandas
"""

from __future__ import annotations
import argparse
import hashlib
import json
import os
import re
import math
import base64
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Set
from datetime import datetime

import pymysql
from neo4j import GraphDatabase
import pandas as pd


# ============================== CONFIGURATION ==============================

@dataclass
class GovernanceConfig:
    """Configuration for governance rule execution."""
    # When True, blocks instance creation if rules fail
    ENFORCE_CONFIDENCE_GATES: bool = False
    # When True, stores validation results in Neo4j
    STORE_VALIDATION_RESULTS: bool = True
    # Batch size for rule execution
    RULE_BATCH_SIZE: int = 100
    # Maximum rule execution time per instance (seconds)
    MAX_RULE_EXEC_TIME: float = 5.0
    # Log level for rule execution
    RULE_LOG_LEVEL: str = "INFO"


# ============================== UTILITIES ==============================

def slugify(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[\s/|]+", "_", s)
    s = re.sub(r"[^a-z0-9_\\-]+", "", s)
    return s[:200] if s else ""


def stable_id(*parts: Any) -> str:
    raw = "||".join("" if p is None else str(p) for p in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def split_list(s: Any) -> List[str]:
    if s is None:
        return []
    if isinstance(s, (list, tuple)):
        return [str(x).strip() for x in s if str(x).strip()]
    s = str(s).strip()
    if not s:
        return []
    parts = re.split(r"[,;\n|]+", s)
    return [p.strip() for p in parts if p.strip()]


def safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if s == "" or s.lower() in {"null", "none", "nan"}:
        return None
    s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return None



def safe_iso(x: Any) -> Optional[str]:
    """Convert datetime/date/str to an ISO-8601 string (or None)."""
    if x is None:
        return None
    try:
        # MySQL returns datetime/date objects via SQLAlchemy
        import datetime as _dt
        if isinstance(x, (_dt.datetime, _dt.date)):
            # Ensure datetime is ISO; keep timezone-naive as-is
            return x.isoformat(sep=" ") if isinstance(x, _dt.datetime) else x.isoformat()
        s = str(x).strip()
        if s == "" or s.lower() in {"null", "none", "nan"}:
            return None
        return s
    except Exception:
        try:
            return str(x)
        except Exception:
            return None

def sanitize_value(value: Any) -> Any:
    """Convert DB/native Python values into Neo4j-packable primitives."""
    if value is None:
        return None
    # bool before int (bool is subclass of int)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, str)):
        return value
    if isinstance(value, float):
        # Neo4j packstream does not support NaN/Inf
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    # Decimal -> float (or int if whole)
    try:
        import decimal as _dec
        if isinstance(value, _dec.Decimal):
            if value.is_nan() or value.is_infinite():
                return None
            # preserve integers
            iv = int(value) if value == value.to_integral_value() else float(value)
            return iv
    except Exception:
        pass
    # datetime/date -> iso string
    try:
        import datetime as _dt
        if isinstance(value, (_dt.datetime, _dt.date)):
            return value.isoformat(sep=" ") if isinstance(value, _dt.datetime) else value.isoformat()
    except Exception:
        pass
    # UUID -> str
    try:
        import uuid as _uuid
        if isinstance(value, _uuid.UUID):
            return str(value)
    except Exception:
        pass
    # bytes -> base64 str (safe)
    if isinstance(value, (bytes, bytearray, memoryview)):
        b = bytes(value)
        return base64.b64encode(b).decode("ascii")
    # list/tuple/set -> list
    if isinstance(value, (list, tuple, set)):
        return [sanitize_value(v) for v in list(value)]
    # dict -> dict
    if isinstance(value, dict):
        return {str(k): sanitize_value(v) for k, v in value.items()}
    # fallback to string
    try:
        s = str(value)
        return s
    except Exception:
        return None


def sanitize_params(params: Any) -> Any:
    """Recursively sanitize params before sending to Neo4j."""
    return sanitize_value(params)



def parse_confidence_gate(gate_value: Any) -> Optional[float]:
    """Parse confidence gate values like '>=0.95', 'High', '0.7' to float."""
    if gate_value is None:
        return None
    s = str(gate_value).strip()
    
    # Handle text values
    if s.lower() in ['high', 'very high']:
        return 0.9
    if s.lower() == 'medium':
        return 0.7
    if s.lower() == 'low':
        return 0.5
    
    # Extract number from expressions like ">=0.95", "0.7", ">0.6"
    match = re.search(r'[\d.]+', s)
    if match:
        try:
            return float(match.group())
        except:
            pass
    return None


def cypher_escape_identifier(name: str) -> str:
    """Escape Neo4j labels / property names / rel types that may contain spaces or special chars."""
    name = "" if name is None else str(name)
    return "`" + name.replace("`", "``") + "`"



@dataclass
class MySQLConnInfo:
    host: str
    port: int
    user: str
    password: str
    db: str


class MySQL:
    def __init__(self, info: MySQLConnInfo):
        self.info = info
        self.conn = pymysql.connect(
            host=info.host,
            port=info.port,
            user=info.user,
            password=info.password,
            database=info.db,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
        )
        # Optional row limit for debug runs (applied to SELECT queries)
        self.limit: Optional[int] = None

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    def q(self, sql: str, params: Optional[Sequence[Any]] = None) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cur:
            # Apply per-table row limit in debug mode (only for plain SELECT without existing LIMIT)
            if self.limit and isinstance(sql, str):
                s = sql.strip().lower()
                if s.startswith('select') and ' limit ' not in s:
                    sql = sql.rstrip(';') + f' LIMIT {self.limit}'
            cur.execute(sql, params or ())
            rows = cur.fetchall()
        return list(rows)

    def table_exists(self, table: str) -> bool:
        rows = self.q(
            "SELECT 1 AS ok FROM information_schema.tables WHERE table_schema=%s AND table_name=%s LIMIT 1",
            (self.info.db, table),
        )
        return bool(rows)

    def columns(self, table: str) -> List[str]:
        rows = self.q(
            "SELECT COLUMN_NAME AS column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s",
            (self.info.db, table),
        )
        out: List[str] = []
        for r in rows:
            if "column_name" in r:
                out.append(r["column_name"])
            elif "COLUMN_NAME" in r:
                out.append(r["COLUMN_NAME"])
            else:
                try:
                    out.append(next(iter(r.values())))
                except Exception:
                    pass
        return out


# ============================== RULE EXECUTION ENGINE ==============================

@dataclass
class RuleValidationResult:
    """Result of executing a governance rule."""
    rule_id: str
    rule_type: str
    entity: str
    instance_id: str
    passed: bool
    confidence_score: float
    system_check_passed: bool
    manual_review_required: bool
    evidence: Dict[str, Any] = field(default_factory=dict)
    executed_at: str = field(default_factory=lambda: datetime.now().isoformat())
    execution_time_ms: float = 0.0
    error_message: Optional[str] = None


class RuleExecutionEngine:
    """
    Executes L3 governance rules against L4 instances.
    Triggers SystemCheckAutomatically Cypher queries and evaluates results.
    """
    
    def __init__(self, neo4j_writer: 'Neo4jWriter', config: GovernanceConfig = None):
        self.neo = neo4j_writer
        self.config = config or GovernanceConfig()
        self._rule_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_loaded = False
    
    def _load_rule_cache(self):
        """Cache all rules from Neo4j for faster execution."""
        if self._cache_loaded:
            return
        
        try:
            results = self.neo.run("""
                MATCH (r:RuleDef)
                RETURN r.ruleType as ruleType,
                       r.entity as entity,
                       r.autoMergeThreshold as threshold,
                       r.hardEvidenceRequired as evidence,
                       r.manualReviewTrigger as reviewTrigger,
                       r.systemCheckAutomatically as systemCheck,
                       r.manualReviewCondition as manualCheck,
                       r.ttlDays as ttl,
                       r.decayModel as decay
            """)
            
            for record in results:
                key = f"{record['ruleType']}:{record['entity']}"
                self._rule_cache[key] = {
                    'ruleType': record['ruleType'],
                    'entity': record['entity'],
                    'threshold': safe_float(record.get('threshold')) or 0.0,
                    'evidence': record.get('evidence', ''),
                    'reviewTrigger': record.get('reviewTrigger', ''),
                    'systemCheck': record.get('systemCheck', ''),
                    'manualCheck': record.get('manualCheck', ''),
                    'ttl': record.get('ttl', 365),
                    'decay': record.get('decay', 'none')
                }
            
            self._cache_loaded = True
            print(f"✅ Rule cache loaded: {len(self._rule_cache)} rules")
            
        except Exception as e:
            print(f"⚠️ Failed to load rule cache: {e}")
    
    def get_rules_for_entity(self, entity_label: str) -> List[Dict[str, Any]]:
        """Get all rules that apply to a specific entity type."""
        self._load_rule_cache()
        
        matching_rules = []
        for key, rule in self._rule_cache.items():
            # Match by entity name (case-insensitive, handle spaces)
            rule_entity = rule['entity'].replace(' ', '').lower()
            check_entity = entity_label.replace(' ', '').lower()
            
            if rule_entity == check_entity:
                matching_rules.append(rule)
        
        return matching_rules
    
    def execute_system_check(self, rule: Dict[str, Any], instance_props: Dict[str, Any]) -> Tuple[bool, Dict]:
        """
        Execute the SystemCheckAutomatically Cypher query.
        Returns (passed, evidence_dict).
        """
        system_check_cypher = rule.get('systemCheck', '').strip()
        
        if not system_check_cypher or system_check_cypher.lower() in ['none', 'null', '']:
            # No system check defined, auto-pass
            return True, {"reason": "No system check query defined"}
        
        try:
            start_time = time.time()
            
            # Execute the stored Cypher query
            results = self.neo.run(system_check_cypher, instance_props)
            
            execution_time = (time.time() - start_time) * 1000
            
            # Parse result - different query patterns return differently
            passed = False
            evidence = {"execution_time_ms": execution_time, "query": system_check_cypher[:200]}
            
            if results:
                # Handle various return patterns
                first_record = results[0]
                
                # Pattern 1: RETURN COUNT(x) > 0 as result
                if 'result' in first_record:
                    passed = bool(first_record['result'])
                # Pattern 2: RETURN count(*) as c
                elif 'c' in first_record:
                    passed = first_record['c'] > 0
                # Pattern 3: RETURN COUNT(x) as count
                elif 'count' in first_record:
                    passed = first_record['count'] > 0
                # Pattern 4: Any non-empty result means pass
                else:
                    passed = True
                    evidence['raw_result'] = dict(first_record)
            else:
                passed = False
                evidence['reason'] = "Query returned no results"
            
            evidence['passed'] = passed
            return passed, evidence
            
        except Exception as e:
            error_msg = str(e)
            print(f"❌ Rule execution error for {rule['ruleType']}:{rule['entity']}: {error_msg[:100]}")
            return False, {"error": error_msg, "query": system_check_cypher[:200]}
    
    def check_manual_review_needed(self, rule: Dict[str, Any], instance_props: Dict[str, Any]) -> Tuple[bool, Dict]:
        """
        Check if manual review is triggered based on ManualReviewCondition.
        Returns (review_needed, evidence_dict).
        """
        manual_check_cypher = rule.get('manualCheck', '').strip()
        
        if not manual_check_cypher or manual_check_cypher.lower() in ['none', 'null', '']:
            return False, {"reason": "No manual review condition defined"}
        
        try:
            results = self.neo.run(manual_check_cypher, instance_props)
            
            # If query returns results, manual review is triggered
            review_needed = len(results) > 0
            
            evidence = {
                "review_needed": review_needed,
                "query": manual_check_cypher[:200],
                "matches_found": len(results)
            }
            
            return review_needed, evidence
            
        except Exception as e:
            # If we can't check, default to requiring review for safety
            return True, {"error": str(e), "defaulted_to_review": True}
    
    def validate_instance(self, entity_label: str, instance_id: str, 
                        instance_props: Dict[str, Any]) -> List[RuleValidationResult]:
        """
        Run all applicable governance rules against an instance.
        Returns list of validation results.
        """
        rules = self.get_rules_for_entity(entity_label)
        
        if not rules:
            return []  # No rules to validate
        
        results = []
        
        for rule in rules:
            start_time = time.time()
            
            # Execute system check
            system_passed, system_evidence = self.execute_system_check(rule, instance_props)
            
            # Check if manual review is needed
            review_needed, review_evidence = self.check_manual_review_needed(rule, instance_props)
            
            # Calculate overall confidence
            threshold = rule.get('threshold', 0.0)
            confidence = threshold if system_passed else threshold * 0.5
            
            # Determine if rule passed overall
            rule_passed = system_passed and not review_needed
            
            execution_time = (time.time() - start_time) * 1000
            
            result = RuleValidationResult(
                rule_id=f"{rule['ruleType']}_{rule['entity']}_{stable_id(rule['ruleType'], rule['entity'], instance_id)[:8]}",
                rule_type=rule['ruleType'],
                entity=rule['entity'],
                instance_id=instance_id,
                passed=rule_passed,
                confidence_score=confidence,
                system_check_passed=system_passed,
                manual_review_required=review_needed,
                evidence={
                    "system_check": system_evidence,
                    "manual_review": review_evidence,
                    "threshold": threshold,
                    "evidence_required": rule.get('evidence', ''),
                    "review_trigger": rule.get('reviewTrigger', '')
                },
                execution_time_ms=execution_time
            )
            
            results.append(result)
            
            # Store result in Neo4j if configured
            if self.config.STORE_VALIDATION_RESULTS:
                self._store_validation_result(result, entity_label, instance_id)
        
        return results
    
    def _store_validation_result(self, result: RuleValidationResult, entity_label: str, instance_id: str):
        """Store validation result in Neo4j."""
        try:
            id_field = self._get_id_field(entity_label)
            
            cypher = f"""
            MATCH (instance:{entity_label} {{{id_field}: $instance_id}})
            MERGE (r:RuleValidationResult {{resultId: $result_id}})
            SET r.ruleType = $rule_type,
                r.entity = $entity,
                r.passed = $passed,
                r.confidenceScore = $confidence,
                r.systemCheckPassed = $system_passed,
                r.manualReviewRequired = $manual_review,
                r.evidence = $evidence,
                r.executedAt = $executed_at,
                r.executionTimeMs = $exec_time
            WITH instance, r
            MERGE (instance)-[:VALIDATED_BY]->(r)
            """
            
            self.neo.run(cypher, {
                "instance_id": instance_id,
                "result_id": result.rule_id,
                "rule_type": result.rule_type,
                "entity": result.entity,
                "passed": result.passed,
                "confidence": result.confidence_score,
                "system_passed": result.system_check_passed,
                "manual_review": result.manual_review_required,
                "evidence": json.dumps(result.evidence),
                "executed_at": result.executed_at,
                "exec_time": result.execution_time_ms
            })
            
        except Exception as e:
            print(f"⚠️ Failed to store validation result: {e}")
    
    def _get_id_field(self, entity_label: str) -> str:
        """Get the ID field name for an entity type."""
        id_map = {
            'Person': 'personId',
            'Account': 'accountId',
            'Organization': 'orgId',
            'Product': 'id',
            'Lead': 'leadId',
            'Deal': 'dealId',
            'RFQ': 'rfqId',
            'Brand': 'brandId',
            'Category': 'categoryId',
            'Store': 'storeId',
            'Session': 'sessionId',
            'Interaction': 'interactionId',
            'Signal': 'signalId',
            'DealLeg': 'dealLegId',
            'Pipeline': 'pipelineId',
            'Task': 'taskId',
            'Visit': 'visitId',
            'PageView': 'pageViewId',
            'Subscription': 'subscriptionId',
            'SubscriptionPlan': 'planId',
            'Thread': 'threadId',
            'Unit': 'unitId',
            'Keyword': 'keywordId',
            'ProductApplication': 'appId',
            'UseCase': 'useCaseId',
            'OrganizationGroup': 'groupId',
            'PipelineStage': 'stageId'
        }
        return id_map.get(entity_label, 'id')
    
    def check_confidence_gate(self, entity_label: str, confidence_value: float) -> Tuple[bool, Optional[float]]:
        """
        Check if instance meets L1 taxonomy confidence gate.
        Returns (passed, required_threshold).
        """
        try:
            result = self.neo.run("""
                MATCH (t:TaxonomyDef {label: $label})
                RETURN t.identityConfidenceGate as gate
            """, {"label": entity_label})
            
            if result and result[0].get('gate'):
                threshold = parse_confidence_gate(result[0]['gate'])
                if threshold is not None:
                    passed = confidence_value >= threshold
                    return passed, threshold
            
            return True, None  # No gate defined, allow through
            
        except Exception as e:
            print(f"⚠️ Could not check confidence gate: {e}")
            return True, None


# ============================== NEO4J WRITER ==============================

class Neo4jWriter:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def run(self, cypher: str, params: Optional[Dict[str, Any]] = None):
        safe_params = sanitize_params(params or {})
        with self.driver.session() as s:
            return list(s.run(cypher, safe_params))

    def create_runtime_constraints(self):
        """Create constraints for all entity types including governance nodes."""
        statements = [
            # L4 Instance constraints
            "CREATE CONSTRAINT account_id IF NOT EXISTS FOR (a:Account) REQUIRE a.accountId IS UNIQUE",
            "CREATE CONSTRAINT account_email_id IF NOT EXISTS FOR (e:AccountEmail) REQUIRE e.emailId IS UNIQUE",
            "CREATE CONSTRAINT person_id IF NOT EXISTS FOR (p:Person) REQUIRE p.personId IS UNIQUE",
            "CREATE CONSTRAINT org_id IF NOT EXISTS FOR (o:Organization) REQUIRE o.orgId IS UNIQUE",
            "CREATE CONSTRAINT orggroup_id IF NOT EXISTS FOR (g:OrganizationGroup) REQUIRE g.groupId IS UNIQUE",
            "CREATE CONSTRAINT store_id IF NOT EXISTS FOR (s:Store) REQUIRE s.storeId IS UNIQUE",
            "CREATE CONSTRAINT product_id IF NOT EXISTS FOR (p:Product) REQUIRE p.id IS UNIQUE",
            "CREATE CONSTRAINT category_id IF NOT EXISTS FOR (c:Category) REQUIRE c.categoryId IS UNIQUE",
            "CREATE CONSTRAINT brand_id IF NOT EXISTS FOR (b:Brand) REQUIRE b.brandId IS UNIQUE",
            "CREATE CONSTRAINT unit_id IF NOT EXISTS FOR (u:Unit) REQUIRE u.unitId IS UNIQUE",
            "CREATE CONSTRAINT app_id IF NOT EXISTS FOR (a:ProductApplication) REQUIRE a.appId IS UNIQUE",
            "CREATE CONSTRAINT usecase_id IF NOT EXISTS FOR (u:UseCase) REQUIRE u.useCaseId IS UNIQUE",
            "CREATE CONSTRAINT keyword_id IF NOT EXISTS FOR (k:Keyword) REQUIRE k.keywordId IS UNIQUE",
            "CREATE CONSTRAINT faq_id IF NOT EXISTS FOR (f:FAQ) REQUIRE f.faqId IS UNIQUE",
            "CREATE CONSTRAINT rfq_id IF NOT EXISTS FOR (r:RFQ) REQUIRE r.rfqId IS UNIQUE",
            "CREATE CONSTRAINT thread_id IF NOT EXISTS FOR (t:Thread) REQUIRE t.threadId IS UNIQUE",
            "CREATE CONSTRAINT lead_id IF NOT EXISTS FOR (l:Lead) REQUIRE l.leadId IS UNIQUE",
            "CREATE CONSTRAINT deal_id IF NOT EXISTS FOR (d:Deal) REQUIRE d.dealId IS UNIQUE",
            "CREATE CONSTRAINT dealleg_id IF NOT EXISTS FOR (dl:DealLeg) REQUIRE dl.dealLegId IS UNIQUE",
            "CREATE CONSTRAINT pipeline_id IF NOT EXISTS FOR (p:Pipeline) REQUIRE p.pipelineId IS UNIQUE",
            "CREATE CONSTRAINT pipelinestage_id IF NOT EXISTS FOR (ps:PipelineStage) REQUIRE ps.stageId IS UNIQUE",
            "CREATE CONSTRAINT task_id IF NOT EXISTS FOR (t:Task) REQUIRE t.taskId IS UNIQUE",
            "CREATE CONSTRAINT session_id IF NOT EXISTS FOR (s:Session) REQUIRE s.sessionId IS UNIQUE",
            "CREATE CONSTRAINT visit_id IF NOT EXISTS FOR (v:Visit) REQUIRE v.visitId IS UNIQUE",
            "CREATE CONSTRAINT pageview_id IF NOT EXISTS FOR (p:PageView) REQUIRE p.pageViewId IS UNIQUE",
            "CREATE CONSTRAINT interaction_id IF NOT EXISTS FOR (i:Interaction) REQUIRE i.interactionId IS UNIQUE",
            "CREATE CONSTRAINT sub_plan_id IF NOT EXISTS FOR (sp:SubscriptionPlan) REQUIRE sp.planId IS UNIQUE",
            "CREATE CONSTRAINT subscription_id IF NOT EXISTS FOR (s:Subscription) REQUIRE s.subscriptionId IS UNIQUE",
            # L1-L3 Governance constraints
            "CREATE CONSTRAINT taxonomy_def_label IF NOT EXISTS FOR (t:TaxonomyDef) REQUIRE t.label IS UNIQUE",
            "CREATE CONSTRAINT entity_def_name IF NOT EXISTS FOR (e:EntityDef) REQUIRE e.entity_name IS UNIQUE",
            "CREATE CONSTRAINT rule_def_composite IF NOT EXISTS FOR (r:RuleDef) REQUIRE (r.ruleType, r.entity) IS UNIQUE",
            "CREATE CONSTRAINT validation_result_id IF NOT EXISTS FOR (vr:RuleValidationResult) REQUIRE vr.resultId IS UNIQUE",
        ]
        for stmt in statements:
            try:
                self.run(stmt)
            except Exception as e:
                print(f"⚠️ Constraint skipped: {stmt[:50]}... :: {e}")


# ============================== INJECTOR WITH GOVERNANCE ==============================

class InjectorV5:
    """
    V5 Injector with complete 4-layer governance chain and rule execution engine.
    """
    
    def __init__(
        self,
        neo: Neo4jWriter,
        ui: MySQL,
        crm: Optional[MySQL],
        batch_size: int = 2000,
        governance_config: GovernanceConfig = None
    ):
        self.neo = neo
        self.ui = ui
        self.crm = crm
        self.batch_size = batch_size
        self.config = governance_config or GovernanceConfig()
        
        # Initialize rule execution engine
        self.rule_engine = RuleExecutionEngine(neo, self.config)
        
        # Governance chain mappings (populated during XLSX load)
        self._entity_to_taxonomy: Dict[str, str] = {}
        self._rule_to_entity: Dict[str, str] = {}

    def _safe_json_or_split(self, val: Any) -> List[str]:
        """Parse fields that may be JSON array, JSON object, or delimiter string."""
        if val is None:
            return []
        s = str(val).strip()
        if not s:
            return []

        # Try JSON first
        if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
            try:
                obj = json.loads(s)
                if isinstance(obj, list):
                    out = [str(x).strip() for x in obj if str(x).strip()]
                    return list(dict.fromkeys(out))  # dedupe preserve order
                if isinstance(obj, dict):
                    out = [str(k).strip() for k in obj.keys() if str(k).strip()]
                    return out
            except Exception:
                pass

        # Split on common delimiters
        parts = re.split(r"[,;\n|\t]+", s)
        out = [p.strip() for p in parts if p and p.strip()]
        return list(dict.fromkeys(out))

    def _batch(self, rows: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
        for i in range(0, len(rows), size):
            yield rows[i : i + size]

    # ============================== XLSX LOADERS WITH GOVERNANCE CHAIN ==============================

    def load_taxonomy_xlsx(self, path: str) -> 'pd.DataFrame':
        df = pd.read_excel(path)
        df.columns = [str(c).strip() for c in df.columns]
        keep = ["identityLayer", "neo4jLabel", "subTypes", "identityConfidenceGate", "exampleProperties", "businessLogic"]
        for k in keep:
            if k not in df.columns:
                df[k] = ""
        df = df[keep].fillna("")

        return df

    def load_relationships_xlsx(self, path: str) -> 'pd.DataFrame':
        df = pd.read_excel(path)
        df.columns = [str(c).strip() for c in df.columns]
        keep = ["sourceLabel", "relType", "targetLabel", "conditionToCreate", "properties", "businessLogic"]
        for k in keep:
            if k not in df.columns:
                df[k] = ""
        df = df[keep].fillna("")

        return df

    def load_conditional_xlsx(self, path: str) -> 'pd.DataFrame':
        df = pd.read_excel(path)
        df.columns = [str(c).strip() for c in df.columns]
        keep = ["ruleType", "entity", "autoMergeThreshold", "hardEvidenceRequired", "manualReviewTrigger",
                "ttlDays", "decayModel", "notes", "SystemCheckAutomatically", "ManualReviewCondition"]
        for k in keep:
            if k not in df.columns:
                df[k] = ""
        df = df[keep].fillna("")

        return df

    def load_entity_catalogue_xlsx(self, path: str) -> 'pd.DataFrame':
        df = pd.read_excel(path)
        df.columns = [str(c).strip() for c in df.columns]
        keep = ["nodeLabel", "keyProperties", "Database table", "sourceTable", "condition", "businessLogic"]
        for k in keep:
            if k not in df.columns:
                df[k] = ""
        df = df[keep].fillna("")

        return df

    def upsert_meta_from_xlsx(self, entity_xlsx: str = "", rel_xlsx: str = "", 
                              taxonomy_xlsx: str = "", conditional_xlsx: str = "") -> None:
        """
        Upsert L1 Taxonomy, L2 Entity Definitions, L3 Rules, and create governance chain links.
        """
        print("\n==== Loading 4-Layer Governance Model ====\n")
        
        # ========== L1: TAXONOMY ==========
        if taxonomy_xlsx:
            df = self.load_taxonomy_xlsx(taxonomy_xlsx)
            labels = set()
            edges = []
            gate_rows = []
            
            for _, row in df.iterrows():
                parent = str(row.get("neo4jLabel", "")).strip()
                if not parent:
                    continue
                labels.add(parent)
                
                # Build L2→L1 mapping
                self._entity_to_taxonomy[parent] = parent
                
                subs = split_list(row.get("subTypes", ""))
                for s in subs:
                    labels.add(s)
                    edges.append((s, parent))
                    # Subtype also maps to parent taxonomy
                    self._entity_to_taxonomy[s] = parent
                
                gate_rows.append({
                    "label": parent,
                    "identityLayer": str(row.get("identityLayer", "")).strip(),
                    "identityConfidenceGate": str(row.get("identityConfidenceGate", "")).strip(),
                    "exampleProperties": str(row.get("exampleProperties", "")).strip(),
                    "businessLogic": str(row.get("businessLogic", "")).strip(),
                })
            
            # Create TaxonomyDef nodes
            self.neo.run("UNWIND $labels AS l MERGE (t:TaxonomyDef {label:l})", {"labels": sorted(labels)})
            
            # Create IS_SUBTYPE_OF hierarchy
            if edges:
                self.neo.run("""
                    UNWIND $edges AS e
                    MATCH (c:TaxonomyDef {label: e[0]})
                    MATCH (p:TaxonomyDef {label: e[1]})
                    MERGE (c)-[:IS_SUBTYPE_OF]->(p)
                """, {"edges": edges})
            
            # Set taxonomy metadata
            if gate_rows:
                self.neo.run("""
                    UNWIND $rows AS r
                    MATCH (t:TaxonomyDef {label:r.label})
                    SET t.identityLayer = r.identityLayer,
                        t.identityConfidenceGate = r.identityConfidenceGate,
                        t.exampleProperties = r.exampleProperties,
                        t.businessLogic = r.businessLogic
                """, {"rows": gate_rows})
            
            print(f"✅ L1 Taxonomy: {len(labels)} labels, {len(edges)} subtype relationships")

        # ========== L2: ENTITY DEFINITIONS ==========
        if entity_xlsx:
            df = self.load_entity_catalogue_xlsx(entity_xlsx)
            rows = df.to_dict(orient="records")
            
            # Create EntityDef nodes
            cy = """
                UNWIND $rows AS r
                MERGE (e:EntityDef {entity_name: r.nodeLabel})
                SET e.keyProperties = r.keyProperties,
                    e.database_table = CASE WHEN r.`Database table` <> '' THEN r.`Database table` ELSE r.sourceTable END,
                    e.sourceTable = r.sourceTable,
                    e.condition = r.condition,
                    e.businessLogic = r.businessLogic,
                    e.layer = 'L2'
            """
            self.neo.run(cy, {"rows": rows})
            
            # ========== L2→L1: BELONGS_TO_TAXONOMY ==========
            # Link each EntityDef to its TaxonomyDef
            link_rows = []
            for r in rows:
                entity_name = r.get("nodeLabel", "").strip()
                if entity_name and entity_name in self._entity_to_taxonomy:
                    taxonomy_label = self._entity_to_taxonomy[entity_name]
                    link_rows.append({
                        "entity_name": entity_name,
                        "taxonomy_label": taxonomy_label
                    })
            
            if link_rows:
                self.neo.run("""
                    UNWIND $rows AS row
                    MATCH (e:EntityDef {entity_name: row.entity_name})
                    MATCH (t:TaxonomyDef {label: row.taxonomy_label})
                    MERGE (e)-[:BELONGS_TO_TAXONOMY]->(t)
                """, {"rows": link_rows})
            
            print(f"✅ L2 Entity Definitions: {len(rows)} entities, {len(link_rows)} taxonomy links")

        # ========== L3: RULES ==========
        if conditional_xlsx:
            df = self.load_conditional_xlsx(conditional_xlsx)
            rows = df.to_dict(orient="records")
            
            # Create RuleDef nodes
            cy = """
                UNWIND $rows AS r
                MERGE (rule:RuleDef {ruleType: r.ruleType, entity: r.entity})
                SET rule.autoMergeThreshold = r.autoMergeThreshold,
                    rule.hardEvidenceRequired = r.hardEvidenceRequired,
                    rule.manualReviewTrigger = r.manualReviewTrigger,
                    rule.ttlDays = r.ttlDays,
                    rule.decayModel = r.decayModel,
                    rule.notes = r.notes,
                    rule.systemCheckAutomatically = r.SystemCheckAutomatically,
                    rule.manualReviewCondition = r.ManualReviewCondition,
                    rule.layer = 'L3'
            """
            self.neo.run(cy, {"rows": rows})
            
            # ========== L3→L2: VALIDATES_ENTITY ==========
            # Link each RuleDef to its EntityDef
            link_rows = []
            for r in rows:
                entity = r.get("entity", "").strip()
                rule_type = r.get("ruleType", "").strip()
                if entity:
                    link_rows.append({
                        "rule_type": rule_type,
                        "entity": entity
                    })
                    self._rule_to_entity[f"{rule_type}:{entity}"] = entity
            
            if link_rows:
                self.neo.run("""
                    UNWIND $rows AS row
                    MATCH (rule:RuleDef {ruleType: row.rule_type, entity: row.entity})
                    MATCH (e:EntityDef {entity_name: row.entity})
                    MERGE (rule)-[:VALIDATES_ENTITY]->(e)
                """, {"rows": link_rows})
            
            print(f"✅ L3 Rules: {len(rows)} rules, {len(link_rows)} entity validation links")

        # ========== RELATIONSHIP TYPES ==========
        if rel_xlsx:
            df = self.load_relationships_xlsx(rel_xlsx)
            rows = df.to_dict(orient="records")
            cy = """
                UNWIND $rows AS r
                MERGE (rt:RelationshipType {source: r.sourceLabel, type: r.relType, target: r.targetLabel})
                SET rt.conditionToCreate = r.conditionToCreate,
                    rt.properties = r.properties,
                    rt.businessLogic = r.businessLogic
            """
            self.neo.run(cy, {"rows": rows})
            print(f"✅ Relationship Types: {len(rows)} definitions")

        print("\n✅ 4-Layer Governance Chain Complete: L2→L1 (BELONGS_TO_TAXONOMY), L3→L2 (VALIDATES_ENTITY)")

    # ============================== RULE-ENABLED INSTANCE INJECTION ==============================

    def _create_instance_with_governance(
        self, 
        label: str, 
        id_field: str, 
        id_value: str,
        props: Dict[str, Any],
        run_rules: bool = True
    ) -> Tuple[bool, List[RuleValidationResult]]:
        """
        Create or merge an instance node with optional rule validation.
        Returns (success, validation_results).
        """
        validation_results = []
        
        # Check confidence gate if configured
        if self.config.ENFORCE_CONFIDENCE_GATES:
            # Extract confidence from props or default to 0
            confidence = safe_float(props.get('confidence', props.get('identityConfidence', 0.0))) or 0.0
            gate_passed, threshold = self.rule_engine.check_confidence_gate(label, confidence)
            
            if not gate_passed:
                print(f"⛔ Confidence gate failed for {label}({id_value}): {confidence} < {threshold}")
                return False, []
        
        # Create/merge the instance
        merge_cypher = f"""
            MERGE (n:{label} {{{id_field}: $id_value}})
            SET n += $props, n.governedAt = datetime()
            RETURN n
        """
        
        try:
            self.neo.run(merge_cypher, {
                "id_value": id_value,
                "props": props
            })
            
            # Run governance rules if enabled
            if run_rules and self.config.STORE_VALIDATION_RESULTS:
                validation_results = self.rule_engine.validate_instance(
                    label, id_value, props
                )
                
                # Log rule results
                passed_count = sum(1 for r in validation_results if r.passed)
                total_count = len(validation_results)
                if validation_results:
                    print(f"   📋 Rules: {passed_count}/{total_count} passed for {label}({str(id_value)[:8]}...)")
            
            return True, validation_results
            
        except Exception as e:
            print(f"❌ Failed to create {label}({id_value}): {e}")
            return False, []

    # ============================== STANDARD INJECTION METHODS ==============================


def inject_users_persons_accounts_orgs(self, run_governance: bool = True):
    """Inject Person, Account, Organization, and OrganizationGroup from baba_stagings with DB-aligned mapping."""
    # persons
    if not self.ui.table_exists("persons"):
        print("⚠️ UI table 'persons' not found. Skipping Person injection.")
        return

    cols = set(self.ui.columns("persons"))
    select_cols = [c for c in ["id", "first_name", "last_name", "name", "phone", "email", "created_at", "updated_at"] if c in cols]
    sql = f"SELECT {', '.join([f'`{c}`' for c in select_cols])} FROM persons"
    persons = self.ui.q(sql)

    print(f"→ Persons: {len(persons)}")
    for chunk in self._batch(persons, self.batch_size):
        for r in chunk:
            pid = str(r.get("id"))
            name = r.get("name") or " ".join([r.get("first_name") or "", r.get("last_name") or ""]).strip() or None
            props = {
                "name": name,
                "email": r.get("email"),
                "phone": r.get("phone"),
                "createdAt": safe_iso(r.get("created_at")),
                "updatedAt": safe_iso(r.get("updated_at")),
                "source": "ui",
            }
            if run_governance:
                self._create_instance_with_governance("Person", "personId", pid, props)
            else:
                self.neo.run("""
                    MERGE (p:Person {personId: $pid})
                    SET p += $props
                """, {"pid": pid, "props": props})

    # users -> Account
    if self.ui.table_exists("users"):
        ucols = set(self.ui.columns("users"))
        u_sel = [c for c in ["id","email","phone","name","created_at","updated_at","person_id"] if c in ucols]
        users = self.ui.q("SELECT " + ", ".join([f"`{c}`" for c in u_sel]) + " FROM users")
        print(f"→ Users/Accounts: {len(users)}")

        for chunk in self._batch(users, self.batch_size):
            for r in chunk:
                aid = str(r.get("id"))
                props = {
                    "email": r.get("email"),
                    "phone": r.get("phone"),
                    "name": r.get("name"),
                    "createdAt": safe_iso(r.get("created_at")),
                    "updatedAt": safe_iso(r.get("updated_at")),
                    "source": "ui",
                }
                if run_governance:
                    self._create_instance_with_governance("Account", "accountId", aid, props)
                else:
                    self.neo.run("""
                        MERGE (a:Account {accountId: $aid})
                        SET a += $props
                    """, {"aid": aid, "props": props})

                if r.get("email"):
                    email_props = {
                        "email": r.get("email"),
                        "verified": True,
                        "source": "ui"
                    }
                    email_id = stable_id("acc_email", aid, r.get("email"))
                    if run_governance:
                        self._create_instance_with_governance("AccountEmail", "emailId", email_id, email_props)
                    else:
                        self.neo.run("""
                            MERGE (e:AccountEmail {emailId: $eid})
                            SET e += $props
                            WITH e
                            MATCH (a:Account {accountId: $aid})
                            MERGE (a)-[:HAS_EMAIL]->(e)
                        """, {"eid": email_id, "aid": aid, "props": email_props})

                if "person_id" in ucols and r.get("person_id"):
                    self.neo.run("""
                        MATCH (p:Person {personId: $pid})
                        MATCH (a:Account {accountId: $aid})
                        MERGE (p)-[:OWNS]->(a)
                    """, {"pid": str(r.get("person_id")), "aid": aid})

    # organization groups
    if self.ui.table_exists("organization_groups"):
        gcols = self.ui.columns("organization_groups")
        org_groups = self.ui.q("SELECT * FROM organization_groups")
        print(f"→ OrganizationGroups: {len(org_groups)}")
        for chunk in self._batch(org_groups, self.batch_size):
            payload = []
            for r in chunk:
                gid = str(r.get("id"))
                props = {k: sanitize_value(v) for k, v in r.items() if k != "id"}
                props["source"] = "ui"
                payload.append({"groupId": gid, "props": props})
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (g:OrganizationGroup {groupId: row.groupId})
                SET g += row.props
            """, {"rows": payload})

    # organizations - map EVERY column from baba_stagings.organizations
    if self.ui.table_exists("organizations"):
        orgs = self.ui.q("SELECT * FROM organizations")
        print(f"→ Organizations: {len(orgs)}")
        for chunk in self._batch(orgs, self.batch_size):
            payload = []
            store_rows = []
            member_rows = []
            group_rows = []
            for r in chunk:
                oid = str(r.get("id"))
                props = {k: sanitize_value(v) for k, v in r.items() if k != "id"}
                props["source"] = "ui"
                # convenience aliases that do not drop raw fields
                props["createdAt"] = safe_iso(r.get("created_at"))
                props["updatedAt"] = safe_iso(r.get("updated_at"))
                payload.append({"orgId": oid, "props": props})

                if r.get("slug"):
                    store_rows.append({
                        "storeId": f"store:{oid}",
                        "orgId": oid,
                        "props": {
                            "slug": sanitize_value(r.get("slug")),
                            "isActive": True,
                            "status": sanitize_value(r.get("organization_status")),
                            "createdAt": safe_iso(r.get("created_at")),
                            "updatedAt": safe_iso(r.get("updated_at")),
                            "source": "ui_derived"
                        }
                    })
                if r.get("user_id") is not None:
                    member_rows.append({"accountId": str(r.get("user_id")), "orgId": oid})
                if r.get("organization_group_id") is not None:
                    group_rows.append({"groupId": str(r.get("organization_group_id")), "orgId": oid})

            self.neo.run("""
                UNWIND $rows AS row
                MERGE (o:Organization {orgId: row.orgId})
                SET o += row.props
            """, {"rows": payload})

            if store_rows:
                self.neo.run("""
                    UNWIND $rows AS row
                    MERGE (s:Store {storeId: row.storeId})
                    SET s += row.props
                    WITH s, row
                    MATCH (o:Organization {orgId: row.orgId})
                    MERGE (o)-[:OWNS]->(s)
                """, {"rows": store_rows})

            if member_rows:
                self.neo.run("""
                    UNWIND $rows AS row
                    MATCH (a:Account {accountId: row.accountId})
                    MATCH (o:Organization {orgId: row.orgId})
                    MERGE (a)-[:MEMBER_OF]->(o)
                """, {"rows": member_rows})

            if group_rows:
                self.neo.run("""
                    UNWIND $rows AS row
                    MATCH (g:OrganizationGroup {groupId: row.groupId})
                    MATCH (o:Organization {orgId: row.orgId})
                    MERGE (g)-[:HAS_MEMBER]->(o)
                """, {"rows": group_rows})

    print("✅ Identity injection complete with governance")

    def inject_categories(self):
        """Inject categories."""
        if not self.ui.table_exists("categories"):
            print("⚠️ UI table 'categories' not found. Skipping Category.")
            return
        cols = set(self.ui.columns("categories"))
        sel = [c for c in ["id","name","parent_id","level","slug","url_key","created_at","updated_at"] if c in cols]
        rows = self.ui.q("SELECT " + ", ".join([f"`{c}`" for c in sel]) + " FROM categories")
        print(f"→ Categories: {len(rows)}")

        for chunk in self._batch(rows, self.batch_size):
            payload = []
            parent_links = []
            for r in chunk:
                cid = str(r.get("id"))
                payload.append({
                    "categoryId": cid,
                    "props": {
                        "name": r.get("name"),
                        "level": r.get("level") if "level" in cols else None,
                        "slug": r.get("slug") if "slug" in cols else None,
                        "url_key": r.get("url_key") if "url_key" in cols else None,
                        "source": "ui",
                    }
                })
                if "parent_id" in cols and r.get("parent_id") and int(r.get("parent_id")) != 0:
                    parent_links.append({"childId": cid, "parentId": str(r.get("parent_id"))})

            self.neo.run("""
            UNWIND $rows AS row
            MERGE (c:Category {categoryId: row.categoryId})
            SET c += row.props
            """, {"rows": payload})

            if parent_links:
                self.neo.run("""
                UNWIND $rows AS row
                MATCH (child:Category {categoryId: row.childId})
                MATCH (parent:Category {categoryId: row.parentId})
                MERGE (child)-[:IS_SUBCATEGORY_OF]->(parent)
                """, {"rows": parent_links})

        # taxonomy link: all categories -> TaxonomyDef(Category)
        self.neo.run("""
        MATCH (c:Category), (t:TaxonomyDef {label:'Category'})
        MERGE (c)-[:HAS_TAXONOMY]->(t)
        """)


def inject_brands(self):
    """Inject brands and link to owning Organization when possible via shared user_id."""
    if not self.ui.table_exists("brands"):
        print("⚠️ UI table 'brands' not found. Skipping Brand.")
        return
    cols = set(self.ui.columns("brands"))
    rows = self.ui.q("SELECT * FROM brands")
    print(f"→ Brands: {len(rows)}")

    for chunk in self._batch(rows, self.batch_size):
        payload = []
        owner_links = []
        for r in chunk:
            bid = str(r.get("id"))
            props = {k: sanitize_value(v) for k, v in r.items() if k != "id"}
            props["source"] = "ui"
            props["createdAt"] = safe_iso(r.get("created_at"))
            props["updatedAt"] = safe_iso(r.get("updated_at"))
            payload.append({"brandId": bid, "props": props})
            if r.get("user_id") is not None:
                owner_links.append({"brandId": bid, "accountId": str(r.get("user_id"))})

        self.neo.run("""
            UNWIND $rows AS row
            MERGE (b:Brand {brandId: row.brandId})
            SET b += row.props
        """, {"rows": payload})

        if owner_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (b:Brand {brandId: row.brandId})
                MATCH (a:Account {accountId: row.accountId})-[:MEMBER_OF]->(o:Organization)
                MERGE (b)-[:OWNED_BY]->(o)
            """, {"rows": owner_links})

    self.neo.run("""
        MATCH (b:Brand), (t:TaxonomyDef {label:'Brand'})
        MERGE (b)-[:HAS_TAXONOMY]->(t)
    """)

    def inject_units(self, run_governance: bool = False):
        """Inject Unit instances from UI DB."""
        if not self.ui.table_exists("units"):
            print("⚠️ UI table 'units' not found. Skipping Unit.")
            return

        cols = set(self.ui.columns("units"))
        sel = [c for c in ["id", "name", "created_at", "updated_at"] if c in cols]
        q = "SELECT " + ", ".join([f"`{c}`" for c in sel]) + " FROM units"
        rows = self.ui.q(q)
        print(f"→ Units: {len(rows)}")

        for chunk in self._batch(rows, self.batch_size):
            for r in chunk:
                uid = str(r.get("id"))
                props = {
                    "name": r.get("name"),
                    "createdAt": safe_iso(r.get("created_at")),
                    "updatedAt": safe_iso(r.get("updated_at")),
                    "source": "ui",
                }

                if run_governance:
                    self._create_instance_with_governance("Unit", "unitId", uid, props)
                else:
                    self.neo.run(
                        """
                        MERGE (u:Unit {unitId: $unitId})
                        SET u += $props
                        """,
                        {"unitId": uid, "props": props},
                    )

        # taxonomy link: Unit -> TaxonomyDef(Unit)
        self.neo.run("""
        MATCH (u:Unit), (t:TaxonomyDef {label:'Unit'})
        MERGE (u)-[:HAS_TAXONOMY]->(t)
        """)


def inject_products(self, run_governance: bool = False):
    """Inject Product from baba_stagings.products using the exact requested node properties only."""
    if not self.ui.table_exists("products"):
        print("⚠️ UI table 'products' not found. Skipping Product.")
        return

    cols = set(self.ui.columns("products"))
    wanted_props = [
        "id",
        "name",
        "pre_title_name",
        "product_type",
        "about_product",
        "description",
        "currency_id",
        "is_placeholder",
        "availability",
        "current_stock",
        "slug",
        "target_industry",
    ]
    helper_cols = ["category_id", "brand_id", "unit", "user_id", "created_at", "updated_at", "deleted_at"]
    sel = [c for c in wanted_props + helper_cols if c in cols]
    q = "SELECT " + ", ".join([f"`{c}`" for c in sel]) + " FROM products"
    rows = self.ui.q(q)
    print(f"→ Products: {len(rows)}")

    # Build unit name -> id lookup once (products table stores unit text, not unit_id)
    unit_name_map = {}
    if self.ui.table_exists("units"):
        try:
            units = self.ui.q("SELECT id, name, slug FROM units")
            for u in units:
                if u.get("name"):
                    unit_name_map[str(u.get("name")).strip().lower()] = str(u.get("id"))
                if u.get("slug"):
                    unit_name_map[str(u.get("slug")).strip().lower()] = str(u.get("id"))
        except Exception:
            unit_name_map = {}

    for chunk in self._batch(rows, self.batch_size):
        prod_rows = []
        cat_links = []
        brand_links = []
        unit_links = []
        supplier_links = []

        for r in chunk:
            product_id = str(r.get("id"))
            props = {
                "id": product_id,
                "name": sanitize_value(r.get("name")),
                "pre_title_name": sanitize_value(r.get("pre_title_name")),
                "product_type": sanitize_value(r.get("product_type")),
                "about_product": sanitize_value(r.get("about_product")),
                "description": sanitize_value(r.get("description")),
                "currency_id": sanitize_value(r.get("currency_id")),
                "isplaceholder": sanitize_value(r.get("is_placeholder")),
                "availability": sanitize_value(r.get("availability")),
                "current_stock": sanitize_value(r.get("current_stock")),
                "slug": sanitize_value(r.get("slug")),
                "target_industry": sanitize_value(r.get("target_industry")),
                "source": "ui",
                "createdAt": safe_iso(r.get("created_at")),
                "updatedAt": safe_iso(r.get("updated_at")),
                "deletedAt": safe_iso(r.get("deleted_at")),
            }

            if run_governance:
                self._create_instance_with_governance("Product", "id", product_id, props)
            else:
                prod_rows.append({"id": product_id, "props": props})

            if r.get("category_id") is not None:
                cat_links.append({"productId": product_id, "categoryId": str(r.get("category_id"))})
            if r.get("brand_id") is not None:
                brand_links.append({"productId": product_id, "brandId": str(r.get("brand_id"))})
            unit_val = str(r.get("unit")).strip().lower() if r.get("unit") is not None else ""
            if unit_val and unit_val in unit_name_map:
                unit_links.append({"productId": product_id, "unitId": unit_name_map[unit_val]})
            if r.get("user_id") is not None:
                supplier_links.append({"productId": product_id, "accountId": str(r.get("user_id"))})

        if prod_rows:
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (p:Product {id: row.id})
                SET p = row.props
            """, {"rows": prod_rows})

        if cat_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (p:Product {id: row.productId})
                MATCH (c:Category {categoryId: row.categoryId})
                MERGE (p)-[:HAS_CATEGORY]->(c)
            """, {"rows": cat_links})

        if brand_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (p:Product {id: row.productId})
                MATCH (b:Brand {brandId: row.brandId})
                MERGE (p)-[:HAS_BRAND]->(b)
            """, {"rows": brand_links})

        if unit_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (p:Product {id: row.productId})
                MATCH (u:Unit {unitId: row.unitId})
                MERGE (p)-[:HAS_UNIT]->(u)
            """, {"rows": unit_links})

        if supplier_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (p:Product {id: row.productId})
                MATCH (a:Account {accountId: row.accountId})-[:MEMBER_OF]->(o:Organization)
                MERGE (p)-[:SUPPLIED_BY]->(o)
            """, {"rows": supplier_links})

    self.neo.run("""
        MATCH (p:Product), (t:TaxonomyDef {label:'Product'})
        MERGE (p)-[:HAS_TAXONOMY]->(t)
    """)

    def inject_crm_pipeline(self):
        """Inject CRM Pipeline and PipelineStage using the *actual* crms.sql schema.

        crms.sql `pipeline_stages` columns:
          - id
          - pipeline_stage_name
          - user_id
          - created_at
          - updated_at
          - deleted_at

        There is no pipeline_id in this table. We therefore:
          - Create PipelineStage(stageId) with name from pipeline_stage_name
          - Create Pipeline(pipelineId) from distinct deals.pipeline_id (if present)
          - Link Deal -> Pipeline and Deal -> PipelineStage (from deals table fields)
        """
        if not self.crm:
            return

        # 1) PipelineStage
        if self.crm.table_exists("pipeline_stages"):
            cols = set(self.crm.columns("pipeline_stages"))
            sel = [c for c in ["id","pipeline_stage_name","created_at","updated_at","deleted_at"] if c in cols]
            rows = self.crm.q("SELECT " + ", ".join([f"`{c}`" for c in sel]) + " FROM pipeline_stages")
            print(f"→ PipelineStages: {len(rows)}")

            payload = []
            for r in rows:
                sid = str(r.get("id"))
                payload.append({
                    "stageId": sid,
                    "props": {
                        "name": r.get("pipeline_stage_name") if "pipeline_stage_name" in cols else None,
                        "createdAt": str(r.get("created_at")) if r.get("created_at") else None,
                        "updatedAt": str(r.get("updated_at")) if r.get("updated_at") else None,
                        "deletedAt": str(r.get("deleted_at")) if r.get("deleted_at") else None,
                        "source": "crm",
                    }
                })

            for chunk in self._batch(payload, self.batch_size):
                self.neo.run("""
                UNWIND $rows AS row
                MERGE (s:PipelineStage {stageId: row.stageId})
                SET s += row.props
                """, {"rows": chunk})
        else:
            print("⚠️ CRM table 'pipeline_stages' not found. Skipping PipelineStage.")

        # 2) Pipelines (derived from deals.pipeline_id if present)
        if self.crm.table_exists("deals"):
            dcols = set(self.crm.columns("deals"))
            if "pipeline_id" in dcols:
                rows = self.crm.q("SELECT DISTINCT pipeline_id FROM deals WHERE pipeline_id IS NOT NULL")
                pipeline_ids = [str(r.get("pipeline_id")) for r in rows if r.get("pipeline_id") is not None]
                if pipeline_ids:
                    self.neo.run("""
                    UNWIND $ids AS pid
                    MERGE (p:Pipeline {pipelineId: pid})
                    ON CREATE SET p.source = 'crm', p.createdAt = toString(datetime())
                    """, {"ids": pipeline_ids})


def inject_crm_core(self):
    """Inject CRM core entities using the *actual* crms.sql schema.

    Key schema adjustments vs earlier versions:
      - rfqs has no organization_id; it contains richer RFQ fields (rfq_number, incoterms, submission_deadline, etc.)
      - crm_leads uses buyer_user_id / seller_user_id (no account_id / organization_id / pipeline_stage_id)
      - deals has account_id, pipeline_id, pipeline_stage_id, lead_id, deal_status (no rfq_id, no buyer_id/seller_id)
      - deal_product_items has type (legType) (not leg_type)
    """
    if not self.crm:
        return

    # ---------------- RFQs ----------------
    if self.crm.table_exists("rfqs"):
        cols = set(self.crm.columns("rfqs"))
        sel = [c for c in [
            "id","user_id","rfq_number","title","source_type","status",
            "delivery_location","shipping_information","incoterms",
            "submission_deadline","payment_terms","target_budget",
            "ai_suggestion","tag","category","describe_request",
            "technical_drawing","rfq_archetype","created_at","updated_at"
        ] if c in cols]
        rows = self.crm.q("SELECT " + ", ".join([f"`{c}`" for c in sel]) + " FROM rfqs")
        print(f"→ RFQs: {len(rows)}")

        for chunk in self._batch(rows, self.batch_size):
            payload = []
            creator = []
            for r in chunk:
                rid = str(r.get("id"))
                payload.append({
                    "rfqId": rid,
                    "props": {
                        "rfqNumber": r.get("rfq_number") if "rfq_number" in cols else None,
                        "title": r.get("title") if "title" in cols else None,
                        "status": r.get("status") if "status" in cols else None,
                        "sourceType": r.get("source_type") if "source_type" in cols else None,
                        "deliveryLocation": r.get("delivery_location") if "delivery_location" in cols else None,
                        "shippingInformation": r.get("shipping_information") if "shipping_information" in cols else None,
                        "incoterms": r.get("incoterms") if "incoterms" in cols else None,
                        "submissionDeadline": str(r.get("submission_deadline")) if r.get("submission_deadline") else None,
                        "paymentTerms": r.get("payment_terms") if "payment_terms" in cols else None,
                        "targetBudget": r.get("target_budget") if "target_budget" in cols else None,
                        "aiSuggestion": r.get("ai_suggestion") if "ai_suggestion" in cols else None,
                        "tag": r.get("tag") if "tag" in cols else None,
                        "category": r.get("category") if "category" in cols else None,
                        "describeRequest": r.get("describe_request") if "describe_request" in cols else None,
                        "technicalDrawing": r.get("technical_drawing") if "technical_drawing" in cols else None,
                        "rfqArchetype": r.get("rfq_archetype") if "rfq_archetype" in cols else None,
                        "createdAt": str(r.get("created_at")) if r.get("created_at") else None,
                        "updatedAt": str(r.get("updated_at")) if r.get("updated_at") else None,
                        "source": "crm",
                    }
                })
                if "user_id" in cols and r.get("user_id") is not None:
                    creator.append({"accountId": str(r.get("user_id")), "rfqId": rid})

            self.neo.run("""
            UNWIND $rows AS row
            MERGE (r:RFQ {rfqId: row.rfqId})
            SET r += row.props
            """, {"rows": payload})

            if creator:
                self.neo.run("""
                UNWIND $rows AS row
                MATCH (a:Account {accountId: row.accountId})
                MATCH (r:RFQ {rfqId: row.rfqId})
                MERGE (a)-[:CREATES]->(r)
                """, {"rows": creator})

                self.neo.run("""
                UNWIND $rows AS row
                MATCH (a:Account {accountId: row.accountId})-[:MEMBER_OF]->(o:Organization)
                MATCH (r:RFQ {rfqId: row.rfqId})
                MERGE (o)-[:ISSUED]->(r)
                """, {"rows": creator})

    # ---------------- Leads ----------------
    rfq_ids_cache: Set[str] = set()
    try:
        rfq_ids_cache = set([str(r["rfqId"]) for r in self.neo.run("MATCH (r:RFQ) RETURN r.rfqId AS rfqId")])
    except Exception:
        rfq_ids_cache = set()

    if self.crm.table_exists("crm_leads"):
        cols = set(self.crm.columns("crm_leads"))
        sel = [c for c in [
            "id","lead_id","lead_title","lead_status_id","category_id","product_id",
            "quantity","message","buyer_user_id","seller_user_id","created_at","updated_at"
        ] if c in cols]
        rows = self.crm.q("SELECT " + ", ".join([f"`{c}`" for c in sel]) + " FROM crm_leads")
        print(f"→ Leads: {len(rows)}")

        for chunk in self._batch(rows, self.batch_size):
            payload = []
            buyer_links = []
            seller_links = []
            rfq_links = []
            for r in chunk:
                lid = str(r.get("id"))
                lead_key = str(r.get("lead_id")) if r.get("lead_id") is not None else None
                payload.append({
                    "leadId": lid,
                    "props": {
                        "leadKey": lead_key,
                        "title": r.get("lead_title") if "lead_title" in cols else None,
                        "statusId": r.get("lead_status_id") if "lead_status_id" in cols else None,
                        "categoryId": r.get("category_id") if "category_id" in cols else None,
                        "productId": r.get("product_id") if "product_id" in cols else None,
                        "quantity": r.get("quantity") if "quantity" in cols else None,
                        "message": r.get("message") if "message" in cols else None,
                        "createdAt": str(r.get("created_at")) if r.get("created_at") else None,
                        "updatedAt": str(r.get("updated_at")) if r.get("updated_at") else None,
                        "source": "crm",
                    }
                })

                if r.get("buyer_user_id") is not None:
                    buyer_links.append({"leadId": lid, "accountId": str(r.get("buyer_user_id"))})
                if r.get("seller_user_id") is not None:
                    seller_links.append({"leadId": lid, "accountId": str(r.get("seller_user_id"))})

                # Heuristic: if lead_id matches an RFQ id, link it.
                if lead_key and lead_key in rfq_ids_cache:
                    rfq_links.append({"leadId": lid, "rfqId": lead_key})

            self.neo.run("""
            UNWIND $rows AS row
            MERGE (l:Lead {leadId: row.leadId})
            SET l += row.props
            """, {"rows": payload})

            if buyer_links:
                self.neo.run("""
                UNWIND $rows AS row
                MATCH (l:Lead {leadId: row.leadId})
                MATCH (a:Account {accountId: row.accountId})
                MERGE (l)-[:BUYER_ACCOUNT]->(a)
                """, {"rows": buyer_links})

            if seller_links:
                self.neo.run("""
                UNWIND $rows AS row
                MATCH (l:Lead {leadId: row.leadId})
                MATCH (a:Account {accountId: row.accountId})
                MERGE (l)-[:SELLER_ACCOUNT]->(a)
                """, {"rows": seller_links})

            if rfq_links:
                self.neo.run("""
                UNWIND $rows AS row
                MATCH (l:Lead {leadId: row.leadId})
                MATCH (r:RFQ {rfqId: row.rfqId})
                MERGE (l)-[:HAS_RFQ {source:'crm_leads.lead_id', confidence:0.7}]->(r)
                """, {"rows": rfq_links})

    # ---------------- Deals ----------------
    if self.crm.table_exists("deals"):
        cols = set(self.crm.columns("deals"))
        sel = [c for c in [
            "id","deal_name","deal_status","amount","priority","type",
            "account_id","lead_id","pipeline_id","pipeline_stage_id",
            "closing_date","created_at","updated_at","deleted_at"
        ] if c in cols]
        rows = self.crm.q("SELECT " + ", ".join([f"`{c}`" for c in sel]) + " FROM deals")
        print(f"→ Deals: {len(rows)}")

        for chunk in self._batch(rows, self.batch_size):
            payload = []
            lead_links = []
            account_links = []
            stage_links = []
            pipeline_links = []
            for r in chunk:
                did = str(r.get("id"))
                payload.append({
                    "dealId": did,
                    "props": {
                        "name": r.get("deal_name") if "deal_name" in cols else None,
                        "status": r.get("deal_status") if "deal_status" in cols else None,
                        "amount": r.get("amount") if "amount" in cols else None,
                        "priority": r.get("priority") if "priority" in cols else None,
                        "type": r.get("type") if "type" in cols else None,
                        "closingDate": str(r.get("closing_date")) if r.get("closing_date") else None,
                        "createdAt": str(r.get("created_at")) if r.get("created_at") else None,
                        "updatedAt": str(r.get("updated_at")) if r.get("updated_at") else None,
                        "deletedAt": str(r.get("deleted_at")) if r.get("deleted_at") else None,
                        "source": "crm",
                    }
                })
                if r.get("lead_id") is not None:
                    lead_links.append({"dealId": did, "leadId": str(r.get("lead_id"))})
                if r.get("account_id") is not None:
                    account_links.append({"dealId": did, "accountId": str(r.get("account_id"))})
                if r.get("pipeline_stage_id") is not None:
                    stage_links.append({"dealId": did, "stageId": str(r.get("pipeline_stage_id"))})
                if r.get("pipeline_id") is not None:
                    pipeline_links.append({"dealId": did, "pipelineId": str(r.get("pipeline_id"))})

            self.neo.run("""
            UNWIND $rows AS row
            MERGE (d:Deal {dealId: row.dealId})
            SET d += row.props
            """, {"rows": payload})

            if lead_links:
                self.neo.run("""
                UNWIND $rows AS row
                MATCH (d:Deal {dealId: row.dealId})
                MATCH (l:Lead {leadId: row.leadId})
                MERGE (l)-[:HAS_DEAL]->(d)
                """, {"rows": lead_links})

            if account_links:
                self.neo.run("""
                UNWIND $rows AS row
                MATCH (d:Deal {dealId: row.dealId})
                MATCH (a:Account {accountId: row.accountId})
                MERGE (a)-[:OWNS_DEAL]->(d)
                """, {"rows": account_links})

            if stage_links:
                self.neo.run("""
                UNWIND $rows AS row
                MATCH (d:Deal {dealId: row.dealId})
                MATCH (s:PipelineStage {stageId: row.stageId})
                MERGE (d)-[:IN_STAGE]->(s)
                """, {"rows": stage_links})

            if pipeline_links:
                self.neo.run("""
                UNWIND $rows AS row
                MATCH (d:Deal {dealId: row.dealId})
                MATCH (p:Pipeline {pipelineId: row.pipelineId})
                MERGE (d)-[:IN_PIPELINE]->(p)
                """, {"rows": pipeline_links})

    # ---------------- Deal Legs (deal_product_items) ----------------
    if self.crm.table_exists("deal_product_items"):
        cols = set(self.crm.columns("deal_product_items"))
        sel = [c for c in [
            "id","deal_id","product_id","type","quantity","price","total_price",
            "product_name","brand_name","created_at","updated_at","deleted_at"
        ] if c in cols]
        rows = self.crm.q("SELECT " + ", ".join([f"`{c}`" for c in sel]) + " FROM deal_product_items")
        print(f"→ DealProductItems: {len(rows)}")

        for chunk in self._batch(rows, self.batch_size):
            payload = []
            deal_links = []
            prod_links = []
            for r in chunk:
                leg_id = str(r.get("id"))
                deal_id = str(r.get("deal_id")) if r.get("deal_id") is not None else None
                product_id = str(r.get("product_id")) if r.get("product_id") is not None else None

                payload.append({
                    "dealLegId": leg_id,
                    "props": {
                        "dealId": deal_id,
                        "productId": product_id,
                        "legType": r.get("type") if "type" in cols else None,
                        "quantity": r.get("quantity") if "quantity" in cols else None,
                        "price": r.get("price") if "price" in cols else None,
                        "totalPrice": r.get("total_price") if "total_price" in cols else None,
                        "productName": r.get("product_name") if "product_name" in cols else None,
                        "brandName": r.get("brand_name") if "brand_name" in cols else None,
                        "createdAt": str(r.get("created_at")) if r.get("created_at") else None,
                        "updatedAt": str(r.get("updated_at")) if r.get("updated_at") else None,
                        "deletedAt": str(r.get("deleted_at")) if r.get("deleted_at") else None,
                        "source": "crm",
                    }
                })

                if deal_id:
                    deal_links.append({"dealId": deal_id, "dealLegId": leg_id})
                if product_id:
                    prod_links.append({"productId": product_id, "dealLegId": leg_id})

            self.neo.run("""
            UNWIND $rows AS row
            MERGE (l:DealLeg {dealLegId: row.dealLegId})
            SET l += row.props
            """, {"rows": payload})

            if deal_links:
                self.neo.run("""
                UNWIND $rows AS row
                MATCH (d:Deal {dealId: row.dealId})
                MATCH (l:DealLeg {dealLegId: row.dealLegId})
                MERGE (d)-[:HAS_LEG]->(l)
                """, {"rows": deal_links})

            if prod_links:
                self.neo.run("""
                UNWIND $rows AS row
                MATCH (p:Product {id: row.productId})
                MATCH (l:DealLeg {dealLegId: row.dealLegId})
                MERGE (l)-[:TARGETS]->(p)
                """, {"rows": prod_links})


    def inject_faqs(self, run_governance: bool = False):
        """Map UI FAQ from baba_stagings.faq_manager -> (:FAQ)."""
        if not self.ui.table_exists("faq_manager"):
            print("→ FAQ: table faq_manager not found (skip)")
            return
        cols = set(self.ui.columns("faq_manager"))
        sel = []
        # required columns
        for c in ["id","module_type","question","answer","status","created_at","updated_at"]:
            if c in cols:
                sel.append(c)
        sql = "SELECT " + ",".join(sel) + " FROM faq_manager"
        rows = self.ui.q(sql)
        print(f"→ FAQs: {len(rows)}")
        payload = []
        for r in rows:
            fid = r.get("id")
            if fid is None:
                continue
            props = {
                "faqId": str(fid),
                "moduleType": r.get("module_type"),
                "question": r.get("question"),
                "answer": r.get("answer"),
                "status": r.get("status"),
                "source": "ui",
                "createdAt": safe_iso(r.get("created_at")),
                "updatedAt": safe_iso(r.get("updated_at")),
            }
            payload.append({"faqId": str(fid), "props": props})
        if not payload:
            return
        self.neo.run(
            """
            UNWIND $rows AS row
            MERGE (f:FAQ {faqId: row.faqId})
            SET f += row.props
            """,
            {"rows": payload},
        )
        if run_governance:
            for row in payload:
                self._create_instance_with_governance("FAQ", "faqId", row["faqId"], row["props"])


    def inject_product_applications(self, run_governance: bool = False):
        """Map UI ProductApplication from baba_stagings.product_application -> (:ProductApplication) and link to Product."""
        if not self.ui.table_exists("product_application"):
            print("→ ProductApplication: table product_application not found (skip)")
            return
        cols = set(self.ui.columns("product_application"))
        sel = [c for c in [
            "id","user_id","name","description","type","status","product_id","created_by","updated_by","created_at","updated_at","deleted_at"
        ] if c in cols]
        sql = "SELECT " + ",".join(sel) + " FROM product_application"
        rows = self.ui.q(sql)
        print(f"→ ProductApplications: {len(rows)}")
        payload = []
        links_prod = []
        links_owner = []
        for r in rows:
            aid = r.get("id")
            if aid is None:
                continue
            app_id = str(aid)
            product_id = r.get("product_id")
            props = {
                "appId": app_id,
                "productId": str(product_id) if product_id is not None else None,
                "name": r.get("name"),
                "description": r.get("description"),
                "type": r.get("type"),
                "status": r.get("status"),
                "createdBy": r.get("created_by"),
                "updatedBy": r.get("updated_by"),
                "source": "ui",
                "createdAt": safe_iso(r.get("created_at")),
                "updatedAt": safe_iso(r.get("updated_at")),
                "deletedAt": safe_iso(r.get("deleted_at")),
            }
            payload.append({"appId": app_id, "props": props})
            if product_id is not None:
                links_prod.append({"appId": app_id, "productId": str(product_id)})
            if r.get("user_id") is not None:
                links_owner.append({"appId": app_id, "accountId": str(r.get("user_id"))})
        if payload:
            self.neo.run(
                """
                UNWIND $rows AS row
                MERGE (a:ProductApplication {appId: row.appId})
                SET a += row.props
                """,
                {"rows": payload},
            )
        if links_prod:
            self.neo.run(
                """
                UNWIND $rows AS row
                MATCH (a:ProductApplication {appId: row.appId})
                MATCH (p:Product {id: row.productId})
                MERGE (p)-[:HAS_APPLICATION]->(a)
                """,
                {"rows": links_prod},
            )
        if links_owner:
            self.neo.run(
                """
                UNWIND $rows AS row
                MATCH (a:ProductApplication {appId: row.appId})
                MATCH (acct:Account {accountId: row.accountId})
                MERGE (acct)-[:CREATES]->(a)
                """,
                {"rows": links_owner},
            )
        if run_governance:
            for row in payload:
                self._create_instance_with_governance("ProductApplication", "appId", row["appId"], row["props"])


    def inject_use_cases_and_keywords(self, run_governance: bool = False):
        """Map UI UseCase from baba_stagings.use_cases -> (:UseCase) and derive Keyword nodes."""
        if not self.ui.table_exists("use_cases"):
            print("→ UseCases: table use_cases not found (skip)")
            return
        cols = set(self.ui.columns("use_cases"))
        sel = [c for c in [
            "id","user_id","name","description","type","is_parent","keywords","related_categories",
            "related_use_cases","related_applications","related_products","created_at","updated_at","deleted_at"
        ] if c in cols]
        sql = "SELECT " + ",".join(sel) + " FROM use_cases"
        rows = self.ui.q(sql)
        print(f"→ UseCases: {len(rows)}")
        uc_payload=[]
        kw_set=set()
        uc_kw_links=[]
        uc_prod_links=[]
        for r in rows:
            uid=r.get("id")
            if uid is None:
                continue
            use_id=str(uid)
            kw_list=[]
            raw_kw=r.get("keywords")
            if raw_kw:
                # supports JSON list or CSV string
                parsed=self._safe_json_or_split(raw_kw)
                if isinstance(parsed, list):
                    kw_list=[str(x).strip() for x in parsed if str(x).strip()]
                else:
                    kw_list=[str(raw_kw).strip()]
            for k in kw_list:
                if not k:
                    continue
                kw_set.add(k.lower())
                uc_kw_links.append({"useCaseId": use_id, "kw": k.lower(), "name": k})
            # related_products can be list of ids in json/csv
            raw_rp=r.get("related_products")
            rp_ids=[]
            if raw_rp:
                parsed=self._safe_json_or_split(raw_rp)
                if isinstance(parsed, list):
                    rp_ids=[x for x in parsed]
                else:
                    # maybe csv numbers
                    rp_ids=[x.strip() for x in str(raw_rp).split(",") if x.strip()]
            for pid in rp_ids:
                try:
                    pid_s=str(int(pid))
                except Exception:
                    pid_s=str(pid)
                uc_prod_links.append({"useCaseId": use_id, "productId": pid_s})
            props={
                "useCaseId": use_id,
                "name": r.get("name"),
                "description": r.get("description"),
                "type": r.get("type"),
                "status": None,
                "isParent": r.get("is_parent"),
                "keywords": kw_list,
                "relatedProducts": rp_ids,
                "relatedApplications": self._safe_json_or_split(r.get("related_applications")) if r.get("related_applications") else None,
                "source": "ui",
                "createdAt": safe_iso(r.get("created_at")),
                "updatedAt": safe_iso(r.get("updated_at")),
                "deletedAt": safe_iso(r.get("deleted_at")),
            }
            uc_payload.append({"useCaseId": use_id, "props": props})
        if uc_payload:
            self.neo.run(
                """
                UNWIND $rows AS row
                MERGE (u:UseCase {useCaseId: row.useCaseId})
                SET u += row.props
                """,
                {"rows": uc_payload},
            )
        # Keyword nodes
        if kw_set:
            kw_payload=[{"keywordId": k, "props": {"keywordId": k, "name": k, "source": "derived", "createdAt": None}} for k in sorted(kw_set)]
            self.neo.run(
                """
                UNWIND $rows AS row
                MERGE (k:Keyword {keywordId: row.keywordId})
                ON CREATE SET k.createdAt = toString(datetime())
                SET k += row.props
                """,
                {"rows": kw_payload},
            )
        if uc_kw_links:
            self.neo.run(
                """
                UNWIND $rows AS row
                MATCH (u:UseCase {useCaseId: row.useCaseId})
                MATCH (k:Keyword {keywordId: row.kw})
                MERGE (u)-[:HAS_KEYWORD]->(k)
                """,
                {"rows": uc_kw_links},
            )
        if uc_prod_links:
            self.neo.run(
                """
                UNWIND $rows AS row
                MATCH (u:UseCase {useCaseId: row.useCaseId})
                MATCH (p:Product {id: row.productId})
                MERGE (u)-[:RELATED_PRODUCT]->(p)
                """,
                {"rows": uc_prod_links},
            )
        if run_governance:
            for row in uc_payload:
                self._create_instance_with_governance("UseCase", "useCaseId", row["useCaseId"], row["props"])
            # also governance for keyword
            for k in kw_set:
                self._create_instance_with_governance("Keyword", "keywordId", k, {"keywordId": k, "name": k, "source": "derived"})


    def inject_ui_sessions(self, run_governance: bool = False):
        """Map UI sessions table -> (:Session) and link to Account."""
        if not self.ui.table_exists("sessions"):
            print("→ Sessions: table sessions not found (skip)")
            return
        cols=set(self.ui.columns("sessions"))
        sel=[c for c in ["id","user_id","ip_address","user_agent","payload","last_activity"] if c in cols]
        sql="SELECT "+",".join(sel)+" FROM sessions"
        rows=self.ui.q(sql)
        print(f"→ Sessions(UI): {len(rows)}")
        payload=[]
        links=[]
        for r in rows:
            sid=r.get("id")
            if sid is None:
                continue
            sid=str(sid)
            props={
                "sessionId": sid,
                "ipAddress": r.get("ip_address"),
                "userAgent": r.get("user_agent"),
                "payload": r.get("payload"),
                "lastActivity": r.get("last_activity"),
                "source": "ui",
            }
            payload.append({"sessionId": sid, "props": props})
            if r.get("user_id") is not None:
                links.append({"sessionId": sid, "accountId": str(r.get("user_id"))})
        if payload:
            self.neo.run(
                """
                UNWIND $rows AS row
                MERGE (s:Session {sessionId: row.sessionId})
                SET s += row.props
                """,
                {"rows": payload},
            )
        if links:
            self.neo.run(
                """
                UNWIND $rows AS row
                MATCH (s:Session {sessionId: row.sessionId})
                MATCH (a:Account {accountId: row.accountId})
                MERGE (a)-[:HAS_SESSION]->(s)
                """,
                {"rows": links},
            )
        if run_governance:
            for row in payload:
                self._create_instance_with_governance("Session", "sessionId", row["sessionId"], row["props"])


    def inject_crm_threads_tasks_pageviews(self, run_governance: bool = False):
        """Map CRM lead_threads -> Thread, crm_task_informations -> Task, page_visits -> PageView + Session."""
        # Threads
        if self.crm.table_exists("lead_threads"):
            cols=set(self.crm.columns("lead_threads"))
            sel=[c for c in ["id","user_id","thread_name","no_of_lead","thread_score","created_at","updated_at","deleted_at"] if c in cols]
            rows=self.crm.q("SELECT "+",".join(sel)+" FROM lead_threads")
            print(f"→ Threads: {len(rows)}")
            payload=[]
            for r in rows:
                tid=r.get("id")
                if tid is None: 
                    continue
                tid=str(tid)
                props={
                    "threadId": tid,
                    "threadName": r.get("thread_name"),
                    "noOfLead": r.get("no_of_lead"),
                    "threadScore": r.get("thread_score"),
                    "source": "crm",
                    "createdAt": safe_iso(r.get("created_at")),
                    "updatedAt": safe_iso(r.get("updated_at")),
                    "deletedAt": safe_iso(r.get("deleted_at")),
                }
                payload.append({"threadId": tid, "props": props, "userId": r.get("user_id")})
            if payload:
                self.neo.run(
                    """
                    UNWIND $rows AS row
                    MERGE (t:Thread {threadId: row.threadId})
                    SET t += row.props
                    """,
                    {"rows": [{"threadId":x["threadId"],"props":x["props"]} for x in payload]},
                )
                # link owner
                owner=[{"threadId":x["threadId"],"accountId":str(x["userId"])} for x in payload if x.get("userId") is not None]
                if owner:
                    self.neo.run(
                        """
                        UNWIND $rows AS row
                        MATCH (t:Thread {threadId: row.threadId})
                        MATCH (a:Account {accountId: row.accountId})
                        MERGE (a)-[:CREATES]->(t)
                        """,
                        {"rows": owner},
                    )
            # thread to lead links
            if self.crm.table_exists("lead_thread_leads"):
                link_rows=self.crm.q("SELECT id,lead_id,thread_id,created_at FROM lead_thread_leads")
                links=[{"threadId":str(r.get("thread_id")), "leadId": str(r.get("lead_id")), "createdAt": safe_iso(r.get("created_at"))} 
                       for r in link_rows if r.get("thread_id") is not None and r.get("lead_id") is not None]
                if links:
                    self.neo.run(
                        """
                        UNWIND $rows AS row
                        MATCH (t:Thread {threadId: row.threadId})
                        MATCH (l:Lead {leadId: row.leadId})
                        MERGE (l)-[r:HAS_THREAD]->(t)
                        ON CREATE SET r.createdAt = row.createdAt
                        """,
                        {"rows": links},
                    )
        # Tasks
        if self.crm.table_exists("crm_task_informations"):
            cols=set(self.crm.columns("crm_task_informations"))
            sel=[c for c in [
                "id","user_id","unique_id","subject","status","priority","task_date","description","completed_at",
                "deal_id","account_id","contact_id","created_at","updated_at","deleted_at"
            ] if c in cols]
            rows=self.crm.q("SELECT "+",".join(sel)+" FROM crm_task_informations")
            print(f"→ Tasks: {len(rows)}")
            payload=[]
            for r in rows:
                tid=r.get("id")
                if tid is None: 
                    continue
                taskId=str(tid)
                props={
                    "taskId": taskId,
                    "uniqueId": r.get("unique_id"),
                    "subject": r.get("subject"),
                    "status": r.get("status"),
                    "priority": r.get("priority"),
                    "taskDate": safe_iso(r.get("task_date")),
                    "description": r.get("description"),
                    "completedAt": safe_iso(r.get("completed_at")),
                    "source": "crm",
                    "createdAt": safe_iso(r.get("created_at")),
                    "updatedAt": safe_iso(r.get("updated_at")),
                    "deletedAt": safe_iso(r.get("deleted_at")),
                }
                payload.append({"taskId": taskId, "props": props, "userId": r.get("user_id"), "dealId": r.get("deal_id"), "accountId": r.get("account_id")})
            if payload:
                self.neo.run(
                    """
                    UNWIND $rows AS row
                    MERGE (t:Task {taskId: row.taskId})
                    SET t += row.props
                    """,
                    {"rows": [{"taskId":x["taskId"],"props":x["props"]} for x in payload]},
                )
                owners=[{"taskId":x["taskId"],"accountId":str(x["userId"])} for x in payload if x.get("userId") is not None]
                if owners:
                    self.neo.run(
                        """
                        UNWIND $rows AS row
                        MATCH (t:Task {taskId: row.taskId})
                        MATCH (a:Account {accountId: row.accountId})
                        MERGE (a)-[:CREATES]->(t)
                        """,
                        {"rows": owners},
                    )
                deal_links=[{"taskId":x["taskId"],"dealId":str(x["dealId"])} for x in payload if x.get("dealId") is not None]
                if deal_links:
                    self.neo.run(
                        """
                        UNWIND $rows AS row
                        MATCH (t:Task {taskId: row.taskId})
                        MATCH (d:Deal {dealId: row.dealId})
                        MERGE (d)-[:HAS_TASK]->(t)
                        """,
                        {"rows": deal_links},
                    )
            # map task to lead via pivot table if present
            if self.crm.table_exists("task_information_to_lead"):
                piv=self.crm.q("SELECT lead_id, task_info_id FROM task_information_to_lead")
                links=[{"leadId":str(r.get("lead_id")), "taskId":str(r.get("task_info_id"))} for r in piv if r.get("lead_id") and r.get("task_info_id")]
                if links:
                    self.neo.run(
                        """
                        UNWIND $rows AS row
                        MATCH (l:Lead {leadId: row.leadId})
                        MATCH (t:Task {taskId: row.taskId})
                        MERGE (l)-[:HAS_TASK]->(t)
                        """,
                        {"rows": links},
                    )
        # PageViews from page_visits
        if self.crm.table_exists("page_visits"):
            cols=set(self.crm.columns("page_visits"))
            sel=[c for c in ["id","session_id","page_url","page_name","user_id","seller_id","product_id","category_id","ip","page_event_ts","page_time_spent","created_at","updated_at"] if c in cols]
            rows=self.crm.q("SELECT "+",".join(sel)+" FROM page_visits")
            print(f"→ PageViews: {len(rows)}")
            pv_payload=[]
            sess_ids=set()
            sess_links=[]
            acct_links=[]
            prod_links=[]
            cat_links=[]
            for r in rows:
                pvid=r.get("id")
                if pvid is None:
                    continue
                pvid=str(pvid)
                sid=r.get("session_id")
                if sid is not None:
                    sid=str(sid)
                    sess_ids.add(sid)
                    sess_links.append({"sessionId":sid,"pageViewId":pvid})
                props={
                    "pageViewId": pvid,
                    "sessionId": str(r.get("session_id")) if r.get("session_id") is not None else None,
                    "pageUrl": r.get("page_url"),
                    "pageName": r.get("page_name"),
                    "ip": r.get("ip"),
                    "eventTs": safe_iso(r.get("page_event_ts")),
                    "timeSpent": r.get("page_time_spent"),
                    "source": "crm",
                    "createdAt": safe_iso(r.get("created_at")),
                    "updatedAt": safe_iso(r.get("updated_at")),
                }
                pv_payload.append({"pageViewId":pvid,"props":props,"userId":r.get("user_id"),"productId":r.get("product_id"),"categoryId":r.get("category_id")})
                if r.get("user_id") is not None:
                    acct_links.append({"pageViewId":pvid,"accountId":str(r.get("user_id"))})
                if r.get("product_id") is not None:
                    prod_links.append({"pageViewId":pvid,"productId":str(r.get("product_id"))})
                if r.get("category_id") is not None:
                    cat_links.append({"pageViewId":pvid,"categoryId":str(r.get("category_id"))})
            if pv_payload:
                self.neo.run(
                    """
                    UNWIND $rows AS row
                    MERGE (pv:PageView {pageViewId: row.pageViewId})
                    SET pv += row.props
                    """,
                    {"rows":[{"pageViewId":x["pageViewId"],"props":x["props"]} for x in pv_payload]},
                )
            if sess_ids:
                self.neo.run(
                    """
                    UNWIND $ids AS sid
                    MERGE (s:Session {sessionId: sid})
                    ON CREATE SET s.source='crm', s.createdAt=toString(datetime())
                    """,
                    {"ids": list(sess_ids)},
                )
            if sess_links:
                self.neo.run(
                    """
                    UNWIND $rows AS row
                    MATCH (s:Session {sessionId: row.sessionId})
                    MATCH (pv:PageView {pageViewId: row.pageViewId})
                    MERGE (s)-[:HAS_PAGEVIEW]->(pv)
                    """,
                    {"rows": sess_links},
                )
            if acct_links:
                self.neo.run(
                    """
                    UNWIND $rows AS row
                    MATCH (pv:PageView {pageViewId: row.pageViewId})
                    MATCH (a:Account {accountId: row.accountId})
                    MERGE (a)-[:GENERATED]->(pv)
                    """,
                    {"rows": acct_links},
                )
            if prod_links:
                self.neo.run(
                    """
                    UNWIND $rows AS row
                    MATCH (pv:PageView {pageViewId: row.pageViewId})
                    MATCH (p:Product {id: row.productId})
                    MERGE (pv)-[:TARGETS]->(p)
                    """,
                    {"rows": prod_links},
                )
            if cat_links:
                self.neo.run(
                    """
                    UNWIND $rows AS row
                    MATCH (pv:PageView {pageViewId: row.pageViewId})
                    MATCH (c:Category {categoryId: row.categoryId})
                    MERGE (pv)-[:IN_CATEGORY]->(c)
                    """,
                    {"rows": cat_links},
                )

    def apply_taxonomy_classification(self) -> None:
        """Classify instance nodes (L4) against TaxonomyDef (L1) using DB signals."""
        print("\n==== Classification (L4 -> L1 Taxonomy) ====")

        # Ensure core taxonomy nodes exist
        for label in ["Buyer", "Seller", "RegisteredUser(Person)", "CandidatePerson", "SellerStore", "BuyerOrg", "SellerOrg"]:
            try:
                self.neo.run(
                    "MERGE (t:TaxonomyDef {label:$label}) ON CREATE SET t.source='classifier' ",
                    {"label": label},
                )
            except Exception:
                pass

        # Seller classification from UI DB
        seller_user_ids = []
        try:
            seller_user_ids = [r["user_id"] for r in self.ui.q("SELECT DISTINCT user_id FROM sellers WHERE deleted_at IS NULL") if r.get("user_id") is not None]
        except Exception as e:
            print(f"(warn) could not read sellers.user_id: {e}")

        # Buyer classification from CRM DB
        buyer_user_ids = []
        try:
            if self.crm:
                buyer_user_ids = [r["user_id"] for r in self.crm.q("SELECT DISTINCT user_id FROM rfqs WHERE user_id IS NOT NULL") if r.get("user_id") is not None]
        except Exception as e:
            print(f"(warn) could not read rfqs.user_id: {e}")

        # Dedupe and convert
        seller_user_ids = sorted({int(x) for x in seller_user_ids if str(x).strip().isdigit()})
        buyer_user_ids = sorted({int(x) for x in buyer_user_ids if str(x).strip().isdigit()})

        print(f"→ Candidate seller_user_ids: {len(seller_user_ids)}")
        print(f"→ Candidate buyer_user_ids : {len(buyer_user_ids)}")

        # Helper to chunk big IN lists
        def chunks(lst, size=2000):
            for i in range(0, len(lst), size):
                yield lst[i:i+size]

        seller_links = 0
        buyer_links = 0

        # Apply Seller to Account
        for batch in chunks(seller_user_ids):
            res = self.neo.run(
                """MATCH (a:Account) WHERE toInteger(a.accountId) IN $ids
                   MATCH (t:TaxonomyDef {label:'Seller'})
                   MERGE (a)-[:HAS_TAXONOMY]->(t)
                   RETURN count(a) AS c""",
                {"ids": batch},
            )
            seller_links += (res[0]["c"] if res else 0)

        for batch in chunks(buyer_user_ids):
            res = self.neo.run(
                """MATCH (a:Account) WHERE toInteger(a.accountId) IN $ids
                   MATCH (t:TaxonomyDef {label:'Buyer'})
                   MERGE (a)-[:HAS_TAXONOMY]->(t)
                   RETURN count(a) AS c""",
                {"ids": batch},
            )
            buyer_links += (res[0]["c"] if res else 0)

        print(f"→ Account HAS_TAXONOMY Seller links: {seller_links}")
        print(f"→ Account HAS_TAXONOMY Buyer  links: {buyer_links}")

        # Propagate taxonomy from Account to Person via OWNS
        try:
            self.neo.run(
                """MATCH (p:Person)-[:OWNS]->(a:Account)-[:HAS_TAXONOMY]->(t:TaxonomyDef)
                   WHERE t.label IN ['Buyer','Seller']
                   MERGE (p)-[:HAS_TAXONOMY]->(t)"""
            )
            print("→ Propagated Buyer/Seller taxonomy Account -> Person")
        except Exception as e:
            print(f"(warn) could not propagate taxonomy to Person: {e}")

        print("✅ Classification complete.")

    # ============================== ORCHESTRATION ==============================

    def run_all(
        self,
        do_ui: bool = True,
        do_crm: bool = True,
        classify: bool = False,
        xlsx_entity: str = None,
        xlsx_relationships: str = None,
        xlsx_taxonomy: str = None,
        xlsx_conditional: str = None,
        run_governance: bool = True
    ) -> None:
        """
        Main orchestration with governance chain loading and rule execution.
        """
        print("\n" + "="*60)
        print("UI/CRM INJECTOR V5 - 4-LAYER GOVERNANCE MODEL")
        print("="*60 + "\n")
        
        # Step 1: Load governance chain from XLSX
        self.upsert_meta_from_xlsx(
            entity_xlsx=xlsx_entity,
            rel_xlsx=xlsx_relationships,
            taxonomy_xlsx=xlsx_taxonomy,
            conditional_xlsx=xlsx_conditional
        )
        
        # Step 2: Create runtime constraints
        self.neo.create_runtime_constraints()
        
        # Step 3: Inject data with governance
        if do_ui:
            print("\n---- Injecting UI Data ----")
            self.inject_users_persons_accounts_orgs(run_governance=run_governance)
            self.inject_categories()
            self.inject_brands()
            self.inject_units(run_governance=run_governance)
            self.inject_products(run_governance=run_governance)
            self.inject_faqs(run_governance=run_governance)
            self.inject_product_applications(run_governance=run_governance)
            self.inject_use_cases_and_keywords(run_governance=run_governance)
            self.inject_ui_sessions(run_governance=run_governance)
            
        if do_crm:
            print("\n---- Injecting CRM Data ----")
            self.inject_crm_pipeline()
            self.inject_crm_core()
            self.inject_crm_threads_tasks_pageviews(run_governance=run_governance)
        
        # Step 4: Apply taxonomy classification
        if classify:
            print("\n---- Applying Taxonomy Classification ----")
            self.apply_taxonomy_classification()
        
        print("\n" + "="*60)
        print("INJECTION COMPLETE")
        print("="*60)


# ============================== CLI ==============================

def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="UI/CRM → Neo4j with 4-Layer Governance (v5)")
    ap.add_argument("--neo4j-uri", required=True)
    ap.add_argument("--neo4j-user", required=True)
    ap.add_argument("--neo4j-pass", required=True)

    ap.add_argument("--ui-mysql-host", default="127.0.0.1")
    ap.add_argument("--ui-mysql-port", type=int, default=3307)
    ap.add_argument("--ui-mysql-user", default="root")
    ap.add_argument("--ui-mysql-pass", required=True)
    ap.add_argument("--ui-db", required=True)

    ap.add_argument("--crm-mysql-host", default="127.0.0.1")
    ap.add_argument("--crm-mysql-port", type=int, default=3306)
    ap.add_argument("--crm-mysql-user", default="root")
    ap.add_argument("--crm-mysql-pass", required=True)
    ap.add_argument("--crm-db", required=True)

    ap.add_argument("--batch-size", type=int, default=2000)
    ap.add_argument("--limit", type=int, default=None, help="Limit rows per table (debug)")
    ap.add_argument("--skip-ui", action="store_true")
    ap.add_argument("--skip-crm", action="store_true")
    
    # Governance options
    ap.add_argument("--entity-xlsx", default="", help="Path to entity-catalogue XLSX (L2)")
    ap.add_argument("--relationships-xlsx", default="", help="Path to all-relationships XLSX")
    ap.add_argument("--taxonomy-xlsx", default="", help="Path to identity-taxonomy XLSX (L1)")
    ap.add_argument("--conditional-xlsx", default="", help="Path to conditional-logic XLSX (L3)")
    ap.add_argument("--classify", action="store_true", help="Apply taxonomy classification")
    ap.add_argument("--enforce-gates", action="store_true", help="Enforce confidence gates")
    ap.add_argument("--skip-governance", action="store_true", help="Skip rule execution")
    ap.add_argument("--fast-local", action="store_true", help="Tune for fastest local injection: larger batches and no validation-result writes")
    
    return ap


def main():
    ap = build_arg_parser()
    args = ap.parse_args()

    # Setup governance config
    gov_config = GovernanceConfig()
    gov_config.ENFORCE_CONFIDENCE_GATES = args.enforce_gates
    gov_config.STORE_VALIDATION_RESULTS = not args.skip_governance
    batch_size = args.batch_size
    if args.fast_local:
        batch_size = max(batch_size, 15000)

    neo = Neo4jWriter(args.neo4j_uri, args.neo4j_user, args.neo4j_pass)

    ui = MySQL(MySQLConnInfo(
        host=args.ui_mysql_host,
        port=args.ui_mysql_port,
        user=args.ui_mysql_user,
        password=args.ui_mysql_pass,
        db=args.ui_db,
    ))

    crm = None
    if not args.skip_crm:
        crm = MySQL(MySQLConnInfo(
            host=args.crm_mysql_host,
            port=args.crm_mysql_port,
            user=args.crm_mysql_user,
            password=args.crm_mysql_pass,
            db=args.crm_db,
        ))

    try:
        injector = InjectorV5(
            neo=neo, 
            ui=ui, 
            crm=crm, 
            batch_size=batch_size,
            governance_config=gov_config
        )
        
        # Defensive dispatch: some historical variants used different method names.
        runner = getattr(injector, "run_all", None)
        if not callable(runner):
            runner = getattr(injector, "run", None)
        if not callable(runner):
            runner = getattr(injector, "run_pipeline", None)
        if not callable(runner):
            raise AttributeError("Injector has no runnable entrypoint (expected run_all/run/run_pipeline)")

        runner(
            do_ui=not args.skip_ui, 
            do_crm=not args.skip_crm, 
            classify=args.classify,
            xlsx_entity=args.entity_xlsx,
            xlsx_relationships=args.relationships_xlsx,
            xlsx_taxonomy=args.taxonomy_xlsx,
            xlsx_conditional=args.conditional_xlsx,
            run_governance=not args.skip_governance
        )
        
    finally:
        try:
            ui.close()
        except Exception:
            pass
        try:
            if crm:
                crm.close()
        except Exception:
            pass
        neo.close()


# ============================== V14 EXTENSIONS / PATCHES ==============================

def _kg_safe_value(v):
    if v is None:
        return None
    if isinstance(v, (datetime,)):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray)):
        try:
            return v.decode('utf-8', 'ignore')
        except Exception:
            return base64.b64encode(v).decode('ascii')
    if isinstance(v, (list, dict, tuple, set)):
        try:
            return json.dumps(v, ensure_ascii=False)
        except Exception:
            return str(v)
    if hasattr(v, 'isoformat'):
        try:
            return v.isoformat()
        except Exception:
            pass
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
    return v


def _row_props_all(row, aliases=None, exclude=None, include_raw=True):
    aliases = aliases or {}
    exclude = set(exclude or [])
    props = {}
    if include_raw:
        for k, v in row.items():
            if k in exclude:
                continue
            props[k] = _kg_safe_value(v)
    for outk, ink in aliases.items():
        if callable(ink):
            try:
                props[outk] = _kg_safe_value(ink(row))
            except Exception:
                props[outk] = None
        else:
            props[outk] = _kg_safe_value(row.get(ink))
    return props


def _find_first_table(db, names):
    for name in names:
        try:
            if db and db.table_exists(name):
                return name
        except Exception:
            pass
    return None


def _split_keywords(*values):
    out = []
    seen = set()
    for val in values:
        if not val:
            continue
        txt = str(val)
        for part in re.split(r'[|,;\n]+', txt):
            k = part.strip()
            if not k:
                continue
            lk = k.lower()
            if lk not in seen:
                seen.add(lk)
                out.append(k)
    return out



def patched_inject_users_persons_accounts_orgs(self, run_governance: bool = True):
    """Extended identity injection optimized for local speed with bulk UNWIND writes."""
    # Persons
    if self.ui.table_exists("persons"):
        rows = self.ui.q("SELECT * FROM persons")
        print(f"→ Persons: {len(rows)}")
        for chunk in self._batch(rows, self.batch_size):
            payload = []
            for r in chunk:
                pid = r.get("id")
                if pid is None:
                    continue
                pid = str(pid)
                props = _row_props_all(r, aliases={
                    "name": lambda x: x.get("name") or " ".join([str(x.get("first_name") or "").strip(), str(x.get("last_name") or "").strip()]).strip() or None,
                    "email": "email",
                    "phone": "phone",
                    "createdAt": lambda x: safe_iso(x.get("created_at")),
                    "updatedAt": lambda x: safe_iso(x.get("updated_at")),
                }, exclude={"id"})
                props["source"] = "ui"
                payload.append({"personId": pid, "props": props})
            if payload:
                self.neo.run("""
                    UNWIND $rows AS row
                    MERGE (p:Person {personId: row.personId})
                    SET p += row.props
                """, {"rows": payload})
    else:
        print("⚠️ UI table 'persons' not found. Skipping Person injection.")

    # Users -> Accounts + AccountEmail + OWNS
    if self.ui.table_exists("users"):
        rows = self.ui.q("SELECT * FROM users")
        print(f"→ Users/Accounts: {len(rows)}")
        for i, chunk in enumerate(self._batch(rows, self.batch_size), start=1):
            account_rows, email_rows, owns_rows = [], [], []
            for r in chunk:
                aid = r.get("id")
                if aid is None:
                    continue
                aid = str(aid)
                props = _row_props_all(r, aliases={
                    "name": "name",
                    "email": "email",
                    "phone": "phone",
                    "createdAt": lambda x: safe_iso(x.get("created_at")),
                    "updatedAt": lambda x: safe_iso(x.get("updated_at")),
                }, exclude={"id"})
                props["source"] = "ui"
                account_rows.append({"accountId": aid, "props": props})
                if r.get("email"):
                    email_id = stable_id("acc_email", aid, r.get("email"))
                    email_props = {
                        "email": r.get("email"),
                        "verified": True,
                        "source": "ui",
                    }
                    email_rows.append({"accountId": aid, "emailId": email_id, "props": email_props})
                if r.get("person_id") is not None:
                    owns_rows.append({"personId": str(r.get("person_id")), "accountId": aid})

            if account_rows:
                self.neo.run("""
                    UNWIND $rows AS row
                    MERGE (a:Account {accountId: row.accountId})
                    SET a += row.props
                """, {"rows": account_rows})
            if email_rows:
                self.neo.run("""
                    UNWIND $rows AS row
                    MERGE (e:AccountEmail {emailId: row.emailId})
                    SET e += row.props
                    WITH row, e
                    MATCH (a:Account {accountId: row.accountId})
                    MERGE (a)-[:HAS_EMAIL]->(e)
                """, {"rows": email_rows})
            if owns_rows:
                self.neo.run("""
                    UNWIND $rows AS row
                    MATCH (p:Person {personId: row.personId})
                    MATCH (a:Account {accountId: row.accountId})
                    MERGE (p)-[:OWNS]->(a)
                """, {"rows": owns_rows})
            if i % 5 == 0:
                print(f"   processed account chunks: {i} (~{min(i*self.batch_size, len(rows))}/{len(rows)})")
    else:
        print("⚠️ UI table 'users' not found. Skipping Account injection.")

    # Organization groups
    org_group_table = _find_first_table(self.ui, ["organization_groups", "organisation_groups"])
    if org_group_table:
        rows = self.ui.q(f"SELECT * FROM `{org_group_table}`")
        print(f"→ OrganizationGroups: {len(rows)}")
        for chunk in self._batch(rows, self.batch_size):
            payload = []
            for r in chunk:
                gid = r.get("id")
                if gid is None:
                    continue
                payload.append({
                    "groupId": str(gid),
                    "props": {**_row_props_all(r, aliases={
                        "name": "name",
                        "createdAt": lambda x: safe_iso(x.get("created_at")),
                        "updatedAt": lambda x: safe_iso(x.get("updated_at")),
                        "deletedAt": lambda x: safe_iso(x.get("deleted_at")),
                    }, exclude={"id"}), "source": "ui"}
                })
            if payload:
                self.neo.run("""
                    UNWIND $rows AS row
                    MERGE (g:OrganizationGroup {groupId: row.groupId})
                    SET g += row.props
                """, {"rows": payload})
    # Organizations
    if self.ui.table_exists("organizations"):
        rows = self.ui.q("SELECT * FROM organizations")
        print(f"→ Organizations: {len(rows)}")
        for chunk in self._batch(rows, self.batch_size):
            payload, group_links, user_links, store_links = [], [], [], []
            for r in chunk:
                oid = r.get("id")
                if oid is None:
                    continue
                oid = str(oid)
                props = _row_props_all(r, aliases={
                    "name": "name",
                    "slug": "slug",
                    "website": lambda x: x.get("website") or x.get("registration_website"),
                    "email": lambda x: x.get("email") or x.get("shop_email"),
                    "phone": "phone",
                    "address": "address",
                    "userId": "user_id",
                    "createdAt": lambda x: safe_iso(x.get("created_at")),
                    "updatedAt": lambda x: safe_iso(x.get("updated_at")),
                    "deletedAt": lambda x: safe_iso(x.get("deleted_at")),
                }, exclude={"id"})
                props["source"] = "ui"
                payload.append({"orgId": oid, "props": props})
                gid = r.get("organization_group_id") if r.get("organization_group_id") is not None else r.get("group_id")
                if gid is not None:
                    group_links.append({"orgId": oid, "groupId": str(gid)})
                if r.get("user_id") is not None:
                    user_links.append({"orgId": oid, "accountId": str(r.get("user_id"))})
                if r.get("slug"):
                    store_links.append({"orgId": oid, "storeId": f"store:{oid}", "props": {
                        "slug": r.get("slug"),
                        "status": r.get("organization_status"),
                        "isActive": True,
                        "source": "ui_derived",
                        "createdAt": safe_iso(r.get("created_at")),
                        "updatedAt": safe_iso(r.get("updated_at")),
                    }})
            if payload:
                self.neo.run("""
                    UNWIND $rows AS row
                    MERGE (o:Organization {orgId: row.orgId})
                    SET o += row.props
                """, {"rows": payload})
            if group_links:
                self.neo.run("""
                    UNWIND $rows AS row
                    MATCH (o:Organization {orgId: row.orgId})
                    MATCH (g:OrganizationGroup {groupId: row.groupId})
                    MERGE (o)-[:MEMBER_OF_GROUP]->(g)
                """, {"rows": group_links})
            if user_links:
                self.neo.run("""
                    UNWIND $rows AS row
                    MATCH (o:Organization {orgId: row.orgId})
                    MATCH (a:Account {accountId: row.accountId})
                    MERGE (a)-[:OWNS]->(o)
                """, {"rows": user_links})
            if store_links:
                self.neo.run("""
                    UNWIND $rows AS row
                    MERGE (s:Store {storeId: row.storeId})
                    SET s += row.props
                    WITH row, s
                    MATCH (o:Organization {orgId: row.orgId})
                    MERGE (o)-[:OWNS]->(s)
                """, {"rows": store_links})
    print("✅ Identity injection complete (extended v14)")

def patched_inject_products(self, run_governance: bool = False):
    """Full-column Product injection + seller + keyword links."""
    if not self.ui.table_exists("products"):
        print("⚠️ UI table 'products' not found. Skipping Product.")
        return
    rows = self.ui.q("SELECT * FROM products")
    print(f"→ Products(full): {len(rows)}")
    for chunk in self._batch(rows, self.batch_size):
        prod_rows, cat_links, brand_links, unit_links, owner_links, kw_nodes, kw_links = [], [], [], [], [], [], []
        for r in chunk:
            pid = r.get("id")
            if pid is None:
                continue
            pid = str(pid)
            unit_ref = r.get("unit_id") if r.get("unit_id") is not None else r.get("uom_id")
            props = _row_props_all(r, aliases={
                "name": lambda x: x.get("name") or x.get("title") or x.get("product_name"),
                "sku": lambda x: x.get("sku") or x.get("unique_number"),
                "model": lambda x: x.get("model") or x.get("model_number"),
                "description": lambda x: x.get("description") or x.get("short_description") or x.get("about_product"),
                "price": lambda x: x.get("price") or x.get("unit_price"),
                "purchasePrice": "purchase_price",
                "sellingPrice": "selling_price",
                "currentStock": "current_stock",
                "userId": "user_id",
                "createdAt": lambda x: safe_iso(x.get("created_at")),
                "updatedAt": lambda x: safe_iso(x.get("updated_at")),
                "deletedAt": lambda x: safe_iso(x.get("deleted_at")),
            }, exclude={"id"})
            props["source"] = "ui"
            prod_rows.append({"productId": pid, "props": props})
            if r.get("category_id") is not None:
                cat_links.append({"productId": pid, "categoryId": str(r.get("category_id"))})
            if r.get("brand_id") is not None:
                brand_links.append({"productId": pid, "brandId": str(r.get("brand_id"))})
            if unit_ref is not None:
                unit_links.append({"productId": pid, "unitId": str(unit_ref)})
            if r.get("user_id") is not None:
                owner_links.append({"productId": pid, "accountId": str(r.get("user_id"))})
            for kw in _split_keywords(r.get("tags"), r.get("meta_keyword")):
                kw_id = stable_id("kw", kw.lower())
                kw_nodes.append({"keywordId": kw_id, "props": {"name": kw, "source": "ui_product"}})
                kw_links.append({"productId": pid, "keywordId": kw_id})
        if prod_rows:
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (p:Product {id: row.productId})
                SET p += row.props
            """, {"rows": prod_rows})
        if cat_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (p:Product {id: row.productId})
                MATCH (c:Category {categoryId: row.categoryId})
                MERGE (p)-[:IN_CATEGORY]->(c)
            """, {"rows": cat_links})
        if brand_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (p:Product {id: row.productId})
                MATCH (b:Brand {brandId: row.brandId})
                MERGE (p)-[:OF_BRAND]->(b)
            """, {"rows": brand_links})
        if unit_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (p:Product {id: row.productId})
                MATCH (u:Unit {unitId: row.unitId})
                MERGE (p)-[:MEASURED_IN]->(u)
            """, {"rows": unit_links})
        if owner_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (a:Account {accountId: row.accountId})
                MATCH (p:Product {id: row.productId})
                MERGE (a)-[:OWNS]->(p)
            """, {"rows": owner_links})
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (a:Account {accountId: row.accountId})-[:OWNS]->(o:Organization)
                MATCH (p:Product {id: row.productId})
                MERGE (o)-[:SELLS]->(p)
            """, {"rows": owner_links})
        if kw_nodes:
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (k:Keyword {keywordId: row.keywordId})
                SET k += row.props
            """, {"rows": kw_nodes})
        if kw_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (p:Product {id: row.productId})
                MATCH (k:Keyword {keywordId: row.keywordId})
                MERGE (p)-[:HAS_KEYWORD]->(k)
            """, {"rows": kw_links})
    self.neo.run("MATCH (p:Product), (t:TaxonomyDef {label:'Product'}) MERGE (p)-[:HAS_TAXONOMY]->(t)")


def inject_product_applications_ext(self, run_governance: bool = False):
    table = _find_first_table(self.ui, ["product_application", "product_applications"])
    if not table:
        print("→ ProductApplication: source table not found (skip)")
        return
    rows = self.ui.q(f"SELECT * FROM `{table}`")
    print(f"→ ProductApplications: {len(rows)}")
    payload, prod_links, owner_links = [], [], []
    for r in rows:
        app_id = r.get("id")
        if app_id is None:
            continue
        app_id = str(app_id)
        product_id = r.get("product_id")
        payload.append({
            "appId": app_id,
            "props": {**_row_props_all(r, aliases={
                "productId": lambda x: str(x.get("product_id")) if x.get("product_id") is not None else None,
                "createdAt": lambda x: safe_iso(x.get("created_at")),
                "updatedAt": lambda x: safe_iso(x.get("updated_at")),
                "deletedAt": lambda x: safe_iso(x.get("deleted_at")),
            }, exclude={"id"}), "source": "ui", "sourceTable": table}
        })
        if product_id is not None:
            prod_links.append({"appId": app_id, "productId": str(product_id)})
        if r.get("user_id") is not None:
            owner_links.append({"appId": app_id, "accountId": str(r.get("user_id"))})
    if payload:
        self.neo.run("""
            UNWIND $rows AS row
            MERGE (pa:ProductApplication {appId: row.appId})
            SET pa += row.props
        """, {"rows": payload})
    if prod_links:
        self.neo.run("""
            UNWIND $rows AS row
            MATCH (p:Product {id: row.productId})
            MATCH (pa:ProductApplication {appId: row.appId})
            MERGE (p)-[:HAS_APPLICATION]->(pa)
        """, {"rows": prod_links})
    if owner_links:
        self.neo.run("""
            UNWIND $rows AS row
            MATCH (a:Account {accountId: row.accountId})
            MATCH (pa:ProductApplication {appId: row.appId})
            MERGE (a)-[:CREATES]->(pa)
        """, {"rows": owner_links})


def inject_use_cases_and_keywords_ext(self, run_governance: bool = False):
    if not self.ui.table_exists("use_cases"):
        print("→ UseCase: table use_cases not found (skip)")
        return
    rows = self.ui.q("SELECT * FROM use_cases")
    print(f"→ UseCases: {len(rows)}")
    uc_payload, kw_nodes, kw_links, prod_links, app_links = [], [], [], [], []
    for r in rows:
        uid = r.get("id")
        if uid is None:
            continue
        uid = str(uid)
        uc_payload.append({
            "useCaseId": uid,
            "props": {**_row_props_all(r, aliases={
                "createdAt": lambda x: safe_iso(x.get("created_at")),
                "updatedAt": lambda x: safe_iso(x.get("updated_at")),
                "deletedAt": lambda x: safe_iso(x.get("deleted_at")),
            }, exclude={"id"}), "source": "ui"}
        })
        for kw in _split_keywords(r.get("keywords")):
            kw_id = stable_id("kw", kw.lower())
            kw_nodes.append({"keywordId": kw_id, "props": {"name": kw, "source": "ui_use_case"}})
            kw_links.append({"useCaseId": uid, "keywordId": kw_id})
        for pid in _split_keywords(r.get("related_products")):
            if pid.isdigit():
                prod_links.append({"useCaseId": uid, "productId": pid})
        for aid in _split_keywords(r.get("related_applications")):
            if aid.isdigit():
                app_links.append({"useCaseId": uid, "appId": aid})
    if uc_payload:
        self.neo.run("""
            UNWIND $rows AS row
            MERGE (u:UseCase {useCaseId: row.useCaseId})
            SET u += row.props
        """, {"rows": uc_payload})
    if kw_nodes:
        self.neo.run("""
            UNWIND $rows AS row
            MERGE (k:Keyword {keywordId: row.keywordId})
            SET k += row.props
        """, {"rows": kw_nodes})
    if kw_links:
        self.neo.run("""
            UNWIND $rows AS row
            MATCH (u:UseCase {useCaseId: row.useCaseId})
            MATCH (k:Keyword {keywordId: row.keywordId})
            MERGE (u)-[:HAS_KEYWORD]->(k)
            WITH u,k
            MATCH (p:Product)-[:HAS_KEYWORD]->(k)
            MERGE (p)-[:HAS_USE_CASE]->(u)
        """, {"rows": kw_links})
    if prod_links:
        self.neo.run("""
            UNWIND $rows AS row
            MATCH (u:UseCase {useCaseId: row.useCaseId})
            MATCH (p:Product {id: row.productId})
            MERGE (u)-[:RELATED_PRODUCT]->(p)
            MERGE (p)-[:HAS_USE_CASE]->(u)
        """, {"rows": prod_links})
    if app_links:
        self.neo.run("""
            UNWIND $rows AS row
            MATCH (u:UseCase {useCaseId: row.useCaseId})
            MATCH (pa:ProductApplication {appId: row.appId})
            MERGE (u)-[:RELATED_APPLICATION]->(pa)
        """, {"rows": app_links})


def inject_master_keywords_ext(self):
    if not self.ui.table_exists("master_keywords"):
        print("→ master_keywords: table not found (skip)")
        return
    rows = self.ui.q("SELECT * FROM master_keywords")
    print(f"→ MasterKeywords: {len(rows)}")
    payload, cat_links = [], []
    for r in rows:
        kid = r.get("id")
        if kid is None:
            continue
        kid = str(kid)
        payload.append({
            "keywordId": kid,
            "props": {**_row_props_all(r, aliases={
                "name": "name",
                "score": "score",
                "createdAt": lambda x: safe_iso(x.get("created_at")),
                "updatedAt": lambda x: safe_iso(x.get("updated_at")),
                "deletedAt": lambda x: safe_iso(x.get("deleted_at")),
            }, exclude={"id"}), "source": "ui_master"}
        })
        if r.get("category_id") is not None:
            cat_links.append({"keywordId": kid, "categoryId": str(r.get("category_id"))})
    if payload:
        self.neo.run("""
            UNWIND $rows AS row
            MERGE (k:Keyword {keywordId: row.keywordId})
            SET k += row.props
        """, {"rows": payload})
    if cat_links:
        self.neo.run("""
            UNWIND $rows AS row
            MATCH (k:Keyword {keywordId: row.keywordId})
            MATCH (c:Category {categoryId: row.categoryId})
            MERGE (k)-[:IN_CATEGORY]->(c)
        """, {"rows": cat_links})


def inject_facilities_ext(self):
    table = _find_first_table(self.ui, ["campany_facility", "company_facility", "company_facilities", "company_services"])
    if not table:
        print("→ Facility: source table not found (skip)")
        return
    rows = self.ui.q(f"SELECT * FROM `{table}`")
    print(f"→ Facilities ({table}): {len(rows)}")
    payload, org_links = [], []
    for r in rows:
        fid = r.get("id")
        if fid is None:
            continue
        fid = str(fid)
        shop_id = r.get("shop_id") if r.get("shop_id") is not None else r.get("organization_id")
        payload.append({
            "facilityId": fid,
            "props": {**_row_props_all(r, aliases={
                "name": lambda x: x.get("name") or x.get("title"),
                "createdAt": lambda x: safe_iso(x.get("created_at")),
                "updatedAt": lambda x: safe_iso(x.get("updated_at")),
                "deletedAt": lambda x: safe_iso(x.get("deleted_at")),
            }, exclude={"id"}), "source": "ui", "sourceTable": table}
        })
        if shop_id is not None:
            org_links.append({"facilityId": fid, "orgId": str(shop_id)})
    if payload:
        self.neo.run("""
            UNWIND $rows AS row
            MERGE (f:Facility {facilityId: row.facilityId})
            SET f += row.props
        """, {"rows": payload})
    if org_links:
        self.neo.run("""
            UNWIND $rows AS row
            MATCH (f:Facility {facilityId: row.facilityId})
            MATCH (o:Organization {orgId: row.orgId})
            MERGE (o)-[:HAS_FACILITY]->(f)
        """, {"rows": org_links})


def inject_feature_packages_ext(self):
    plan_table = _find_first_table(self.ui, ["feature_packages", "feature_package"])
    if not plan_table:
        print("→ Feature packages: source table not found (skip)")
        return
    plan_rows = self.ui.q(f"SELECT * FROM `{plan_table}`")
    print(f"→ SubscriptionPlans ({plan_table}): {len(plan_rows)}")
    payload = []
    for r in plan_rows:
        pid = r.get("id")
        if pid is None:
            continue
        payload.append({
            "planId": str(pid),
            "props": {**_row_props_all(r, aliases={
                "createdAt": lambda x: safe_iso(x.get("created_at")),
                "updatedAt": lambda x: safe_iso(x.get("updated_at")),
                "deletedAt": lambda x: safe_iso(x.get("deleted_at")),
            }, exclude={"id"}), "source": "ui", "sourceTable": plan_table}
        })
    if payload:
        self.neo.run("""
            UNWIND $rows AS row
            MERGE (p:SubscriptionPlan {planId: row.planId})
            SET p += row.props
        """, {"rows": payload})
    if self.ui.table_exists("user_access_feature_plan"):
        rows = self.ui.q("SELECT * FROM user_access_feature_plan")
        subs, acct_links, plan_links = [], [], []
        for r in rows:
            sid = r.get("id")
            if sid is None:
                continue
            sid = str(sid)
            subs.append({
                "subscriptionId": sid,
                "props": {**_row_props_all(r, aliases={
                    "createdAt": lambda x: safe_iso(x.get("created_at")),
                    "updatedAt": lambda x: safe_iso(x.get("updated_at")),
                    "deletedAt": lambda x: safe_iso(x.get("deleted_at")),
                }, exclude={"id"}), "source": "ui", "sourceTable": "user_access_feature_plan"}
            })
            if r.get("user_id") is not None:
                acct_links.append({"subscriptionId": sid, "accountId": str(r.get("user_id"))})
            if r.get("plan_id") is not None:
                plan_links.append({"subscriptionId": sid, "planId": str(r.get("plan_id"))})
        if subs:
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (s:Subscription {subscriptionId: row.subscriptionId})
                SET s += row.props
            """, {"rows": subs})
        if acct_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (a:Account {accountId: row.accountId})
                MATCH (s:Subscription {subscriptionId: row.subscriptionId})
                MERGE (a)-[:HAS_SUBSCRIPTION]->(s)
            """, {"rows": acct_links})
        if plan_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (s:Subscription {subscriptionId: row.subscriptionId})
                MATCH (p:SubscriptionPlan {planId: row.planId})
                MERGE (s)-[:ON_PLAN]->(p)
            """, {"rows": plan_links})


def inject_crm_activity_ext(self):
    # page visits with all columns
    page_table = _find_first_table(self.crm, ["page_visits", "page_visit"])
    if page_table:
        rows = self.crm.q(f"SELECT * FROM `{page_table}`")
        print(f"→ PageViews ({page_table}): {len(rows)}")
        payload, sess_links, acct_links, seller_links, prod_links, cat_links = [], [], [], [], [], []
        for r in rows:
            pvid = r.get("id")
            if pvid is None:
                continue
            pvid = str(pvid)
            payload.append({
                "pageViewId": pvid,
                "props": {**_row_props_all(r, aliases={
                    "sessionId": lambda x: str(x.get("session_id")) if x.get("session_id") is not None else None,
                    "eventTs": lambda x: safe_iso(x.get("page_event_ts")),
                    "timeSpent": "page_time_spent",
                    "createdAt": lambda x: safe_iso(x.get("created_at")),
                    "updatedAt": lambda x: safe_iso(x.get("updated_at")),
                }, exclude={"id"}), "source": "crm", "sourceTable": page_table}
            })
            if r.get("session_id") is not None:
                sess_links.append({"pageViewId": pvid, "sessionId": str(r.get("session_id"))})
            if r.get("user_id") is not None:
                acct_links.append({"pageViewId": pvid, "accountId": str(r.get("user_id"))})
            if r.get("seller_id") is not None:
                seller_links.append({"pageViewId": pvid, "orgId": str(r.get("seller_id"))})
            if r.get("product_id") is not None:
                prod_links.append({"pageViewId": pvid, "productId": str(r.get("product_id"))})
            if r.get("category_id") is not None:
                cat_links.append({"pageViewId": pvid, "categoryId": str(r.get("category_id"))})
        if payload:
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (pv:PageView {pageViewId: row.pageViewId})
                SET pv += row.props
            """, {"rows": payload})
        if sess_links:
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (s:Session {sessionId: row.sessionId})
                ON CREATE SET s.source='crm'
                WITH row, s
                MATCH (pv:PageView {pageViewId: row.pageViewId})
                MERGE (s)-[:HAS_PAGEVIEW]->(pv)
            """, {"rows": sess_links})
        if acct_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (a:Account {accountId: row.accountId})
                MATCH (pv:PageView {pageViewId: row.pageViewId})
                MERGE (a)-[:GENERATED]->(pv)
            """, {"rows": acct_links})
        if seller_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (o:Organization {orgId: row.orgId})
                MATCH (pv:PageView {pageViewId: row.pageViewId})
                MERGE (pv)-[:TARGETS_SELLER]->(o)
            """, {"rows": seller_links})
        if prod_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (pv:PageView {pageViewId: row.pageViewId})
                MATCH (p:Product {id: row.productId})
                MERGE (pv)-[:TARGETS]->(p)
            """, {"rows": prod_links})
        if cat_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (pv:PageView {pageViewId: row.pageViewId})
                MATCH (c:Category {categoryId: row.categoryId})
                MERGE (pv)-[:IN_CATEGORY]->(c)
            """, {"rows": cat_links})
    else:
        print("→ PageViews: source table not found (skip)")

    # meetings
    if self.crm.table_exists("crm_meeting_informations"):
        rows = self.crm.q("SELECT * FROM crm_meeting_informations")
        print(f"→ Meetings: {len(rows)}")
        payload, acct_links, deal_links, lead_links = [], [], [], []
        for r in rows:
            mid = r.get("id")
            if mid is None:
                continue
            mid = str(mid)
            payload.append({
                "meetingId": mid,
                "props": {**_row_props_all(r, aliases={
                    "fromAt": lambda x: safe_iso(x.get("from")),
                    "toAt": lambda x: safe_iso(x.get("to")),
                    "createdAt": lambda x: safe_iso(x.get("created_at")),
                    "updatedAt": lambda x: safe_iso(x.get("updated_at")),
                    "deletedAt": lambda x: safe_iso(x.get("deleted_at")),
                    "completedAt": lambda x: safe_iso(x.get("completed_at")),
                }, exclude={"id"}), "source": "crm"}
            })
            if r.get("user_id") is not None:
                acct_links.append({"meetingId": mid, "accountId": str(r.get("user_id"))})
            if r.get("deal_id") is not None:
                deal_links.append({"meetingId": mid, "dealId": str(r.get("deal_id"))})
            if r.get("lead_id") is not None:
                lead_links.append({"meetingId": mid, "leadId": str(r.get("lead_id"))})
        if payload:
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (m:Meeting {meetingId: row.meetingId})
                SET m += row.props
            """, {"rows": payload})
        if acct_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (a:Account {accountId: row.accountId})
                MATCH (m:Meeting {meetingId: row.meetingId})
                MERGE (a)-[:CREATES]->(m)
            """, {"rows": acct_links})
        if deal_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (d:Deal {dealId: row.dealId})
                MATCH (m:Meeting {meetingId: row.meetingId})
                MERGE (d)-[:HAS_MEETING]->(m)
            """, {"rows": deal_links})
        if lead_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (l:Lead {leadId: row.leadId})
                MATCH (m:Meeting {meetingId: row.meetingId})
                MERGE (l)-[:HAS_MEETING]->(m)
            """, {"rows": lead_links})
    else:
        print("→ Meetings: table crm_meeting_informations not found (skip)")

    # scoring rules
    if self.crm.table_exists("crm_scoring_rule"):
        rows = self.crm.q("SELECT * FROM crm_scoring_rule")
        print(f"→ ScoringRules: {len(rows)}")
        payload, acct_links = [], []
        for r in rows:
            rid = r.get("id")
            if rid is None:
                continue
            rid = str(rid)
            payload.append({
                "ruleId": rid,
                "props": {**_row_props_all(r, aliases={
                    "createdAt": lambda x: safe_iso(x.get("created_at")),
                    "updatedAt": lambda x: safe_iso(x.get("updated_at")),
                    "deletedAt": lambda x: safe_iso(x.get("deleted_at")),
                }, exclude={"id"}), "source": "crm"}
            })
            if r.get("user_id") is not None:
                acct_links.append({"ruleId": rid, "accountId": str(r.get("user_id"))})
        if payload:
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (sr:ScoringRule {ruleId: row.ruleId})
                SET sr += row.props
            """, {"rows": payload})
        if acct_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (a:Account {accountId: row.accountId})
                MATCH (sr:ScoringRule {ruleId: row.ruleId})
                MERGE (a)-[:USES_SCORING_RULE]->(sr)
            """, {"rows": acct_links})
    else:
        print("→ ScoringRules: table crm_scoring_rule not found (skip)")


def patched_run_all(self, do_ui=True, do_crm=True, classify=False, xlsx_entity=None, xlsx_relationships=None, xlsx_taxonomy=None, xlsx_conditional=None, run_governance=True):
    print("\n" + "="*60)
    print("UI/CRM INJECTOR V14 - EXTENDED GOVERNANCE MODEL")
    print("="*60 + "\n")
    self.upsert_meta_from_xlsx(entity_xlsx=xlsx_entity, rel_xlsx=xlsx_relationships, taxonomy_xlsx=xlsx_taxonomy, conditional_xlsx=xlsx_conditional)
    self.neo.create_runtime_constraints()
    if do_ui:
        print("\n---- Injecting UI Data ----")
        self.inject_users_persons_accounts_orgs(run_governance=run_governance)
        self.inject_categories()
        self.inject_brands()
        self.inject_units(run_governance=run_governance)
        self.inject_products(run_governance=run_governance)
        self.inject_product_applications(run_governance=run_governance)
        self.inject_use_cases_and_keywords(run_governance=run_governance)
        self.inject_master_keywords()
        self.inject_facilities()
        self.inject_feature_packages()
    if do_crm:
        print("\n---- Injecting CRM Data ----")
        self.inject_crm_pipeline()
        self.inject_crm_core()
        self.inject_crm_activity()
    if classify:
        print("\n---- Applying Taxonomy Classification ----")
        self.apply_taxonomy_classification()
    print("\n" + "="*60)
    print("INJECTION COMPLETE (V14 EXTENDED)")
    print("="*60)




def patched_inject_categories(self):
    """Inject categories in bulk."""
    if not self.ui.table_exists("categories"):
        print("⚠️ UI table 'categories' not found. Skipping Category.")
        return
    cols = set(self.ui.columns("categories"))
    sel = [c for c in ["id","name","parent_id","level","slug","url_key","created_at","updated_at"] if c in cols]
    rows = self.ui.q("SELECT " + ", ".join([f"`{c}`" for c in sel]) + " FROM categories")
    print(f"→ Categories: {len(rows)}")
    for chunk in self._batch(rows, self.batch_size):
        payload = []
        parent_links = []
        for r in chunk:
            cid = str(r.get("id"))
            payload.append({
                "categoryId": cid,
                "props": {
                    "name": r.get("name"),
                    "level": r.get("level") if "level" in cols else None,
                    "slug": r.get("slug") if "slug" in cols else None,
                    "url_key": r.get("url_key") if "url_key" in cols else None,
                    "createdAt": safe_iso(r.get("created_at")) if "created_at" in cols else None,
                    "updatedAt": safe_iso(r.get("updated_at")) if "updated_at" in cols else None,
                    "source": "ui",
                }
            })
            if "parent_id" in cols and r.get("parent_id") not in (None, "", 0, "0"):
                parent_links.append({"childId": cid, "parentId": str(r.get("parent_id"))})
        if payload:
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (c:Category {categoryId: row.categoryId})
                SET c += row.props
            """, {"rows": payload})
        if parent_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (child:Category {categoryId: row.childId})
                MATCH (parent:Category {categoryId: row.parentId})
                MERGE (child)-[:IS_SUBCATEGORY_OF]->(parent)
            """, {"rows": parent_links})
    self.neo.run("""
        MATCH (c:Category), (t:TaxonomyDef {label:'Category'})
        MERGE (c)-[:HAS_TAXONOMY]->(t)
    """)


def patched_inject_brands(self):
    """Inject brands and link them to owning Organization when possible."""
    if not self.ui.table_exists("brands"):
        print("⚠️ UI table 'brands' not found. Skipping Brand.")
        return
    rows = self.ui.q("SELECT * FROM brands")
    print(f"→ Brands: {len(rows)}")
    for chunk in self._batch(rows, self.batch_size):
        payload = []
        owner_links = []
        for r in chunk:
            bid = str(r.get("id"))
            props = {k: sanitize_value(v) for k, v in r.items() if k != "id"}
            props["source"] = "ui"
            if "created_at" in r:
                props["createdAt"] = safe_iso(r.get("created_at"))
            if "updated_at" in r:
                props["updatedAt"] = safe_iso(r.get("updated_at"))
            payload.append({"brandId": bid, "props": props})
            if r.get("user_id") is not None:
                owner_links.append({"brandId": bid, "accountId": str(r.get("user_id"))})
        if payload:
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (b:Brand {brandId: row.brandId})
                SET b += row.props
            """, {"rows": payload})
        if owner_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (b:Brand {brandId: row.brandId})
                MATCH (a:Account {accountId: row.accountId})-[:MEMBER_OF]->(o:Organization)
                MERGE (b)-[:OWNED_BY]->(o)
            """, {"rows": owner_links})
    self.neo.run("""
        MATCH (b:Brand), (t:TaxonomyDef {label:'Brand'})
        MERGE (b)-[:HAS_TAXONOMY]->(t)
    """)


def patched_inject_units(self, run_governance: bool = False):
    """Inject units in bulk."""
    if not self.ui.table_exists("units"):
        print("⚠️ UI table 'units' not found. Skipping Unit.")
        return
    cols = set(self.ui.columns("units"))
    sel = [c for c in ["id", "name", "created_at", "updated_at"] if c in cols]
    rows = self.ui.q("SELECT " + ", ".join([f"`{c}`" for c in sel]) + " FROM units")
    print(f"→ Units: {len(rows)}")
    payload = []
    for r in rows:
        uid = str(r.get("id"))
        payload.append({
            "unitId": uid,
            "props": {
                "name": r.get("name"),
                "createdAt": safe_iso(r.get("created_at")) if "created_at" in cols else None,
                "updatedAt": safe_iso(r.get("updated_at")) if "updated_at" in cols else None,
                "source": "ui",
            }
        })
    for chunk in self._batch(payload, self.batch_size):
        self.neo.run("""
            UNWIND $rows AS row
            MERGE (u:Unit {unitId: row.unitId})
            SET u += row.props
        """, {"rows": chunk})
    self.neo.run("""
        MATCH (u:Unit), (t:TaxonomyDef {label:'Unit'})
        MERGE (u)-[:HAS_TAXONOMY]->(t)
    """)

InjectorV5.inject_categories = patched_inject_categories
InjectorV5.inject_brands = patched_inject_brands
InjectorV5.inject_units = patched_inject_units
InjectorV5.inject_users_persons_accounts_orgs = patched_inject_users_persons_accounts_orgs
InjectorV5.inject_products = patched_inject_products
InjectorV5.inject_product_applications = inject_product_applications_ext
InjectorV5.inject_use_cases_and_keywords = inject_use_cases_and_keywords_ext
InjectorV5.inject_master_keywords = inject_master_keywords_ext
InjectorV5.inject_facilities = inject_facilities_ext
InjectorV5.inject_feature_packages = inject_feature_packages_ext
InjectorV5.inject_crm_activity = inject_crm_activity_ext
InjectorV5.run_all = patched_run_all



def patched_inject_units_from_products(self, run_governance: bool = False):
    """Inject Unit nodes from baba_stagings.products using unit + unit_price, then link only from Product."""
    if not self.ui.table_exists("products"):
        print("⚠️ UI table 'products' not found. Skipping Unit.")
        return
    cols = set(self.ui.columns("products"))
    if "unit" not in cols and "unit_price" not in cols:
        print("⚠️ products.unit / products.unit_price not found. Skipping Unit.")
        return
    sel = [c for c in ["unit", "unit_price", "currency_id"] if c in cols]
    rows = self.ui.q("SELECT " + ", ".join([f"`{c}`" for c in sel]) + " FROM products")
    seen = {}
    for r in rows:
        unit_name = sanitize_value(r.get("unit")) if "unit" in cols else None
        unit_price = sanitize_value(r.get("unit_price")) if "unit_price" in cols else None
        currency_id = sanitize_value(r.get("currency_id")) if "currency_id" in cols else None
        if unit_name in (None, "") and unit_price in (None, ""):
            continue
        uid = stable_id("unit", unit_name or "", unit_price or "")
        seen[uid] = {
            "unitId": uid,
            "props": {
                "name": unit_name,
                "unitPrice": unit_price,
                "currencyId": currency_id,
                "source": "ui_product"
            }
        }
    payload = list(seen.values())
    print(f"→ Units(from products): {len(payload)}")
    for chunk in self._batch(payload, self.batch_size):
        self.neo.run("""
            UNWIND $rows AS row
            MERGE (u:Unit {unitId: row.unitId})
            SET u += row.props
        """, {"rows": chunk})


def patched_inject_products_v2(self, run_governance: bool = False):
    """Product injection aligned to requested mapping with Unit from products.unit + unit_price."""
    if not self.ui.table_exists("products"):
        print("⚠️ UI table 'products' not found. Skipping Product.")
        return
    rows = self.ui.q("SELECT * FROM products")
    print(f"→ Products(full): {len(rows)}")
    wanted = ["id","name","pre_title_name","product_type","about_product","description","currency_id","isplaceholder","availability","current_stock","slug","target_industry"]
    for chunk in self._batch(rows, self.batch_size):
        prod_rows, cat_links, brand_links, unit_links, owner_links, kw_nodes, kw_links = [], [], [], [], [], [], []
        for r in chunk:
            pid = r.get("id")
            if pid is None:
                continue
            pid = str(pid)
            props = {}
            for k in wanted:
                if k in r:
                    props[k] = sanitize_value(r.get(k))
            # tolerate typo request by exposing alias too when source exists
            if "target_industry" in props and "target_insdustry" not in props:
                props["target_insdustry"] = props.get("target_industry")
            props["source"] = "ui"
            prod_rows.append({"productId": pid, "props": props})
            if r.get("category_id") is not None:
                cat_links.append({"productId": pid, "categoryId": str(r.get("category_id"))})
            if r.get("brand_id") is not None:
                brand_links.append({"productId": pid, "brandId": str(r.get("brand_id"))})
            unit_name = sanitize_value(r.get("unit"))
            unit_price = sanitize_value(r.get("unit_price"))
            if unit_name not in (None, "") or unit_price not in (None, ""):
                unit_links.append({"productId": pid, "unitId": stable_id("unit", unit_name or "", unit_price or "")})
            if r.get("user_id") is not None:
                owner_links.append({"productId": pid, "accountId": str(r.get("user_id"))})
            for kw in _split_keywords(r.get("tags"), r.get("meta_keyword")):
                kw_id = stable_id("kw", kw.lower())
                kw_nodes.append({"keywordId": kw_id, "props": {"name": kw, "source": "ui_product"}})
                kw_links.append({"productId": pid, "keywordId": kw_id})
        if prod_rows:
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (p:Product {id: row.productId})
                SET p = row.props
                SET p.id = row.productId
            """, {"rows": prod_rows})
        if cat_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (p:Product {id: row.productId})
                MATCH (c:Category {categoryId: row.categoryId})
                MERGE (p)-[:HAS_CATEGORY]->(c)
            """, {"rows": cat_links})
        if brand_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (p:Product {id: row.productId})
                MATCH (b:Brand {brandId: row.brandId})
                MERGE (p)-[:HAS_BRAND]->(b)
            """, {"rows": brand_links})
        if unit_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (p:Product {id: row.productId})
                MATCH (u:Unit {unitId: row.unitId})
                MERGE (p)-[:HAS_UNIT]->(u)
            """, {"rows": unit_links})
        if owner_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (a:Account {accountId: row.accountId})
                MATCH (p:Product {id: row.productId})
                MERGE (a)-[:OWNS]->(p)
            """, {"rows": owner_links})
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (a:Account {accountId: row.accountId})-[:MEMBER_OF|OWNS]->(o:Organization)
                MATCH (p:Product {id: row.productId})
                MERGE (p)-[:SUPPLIED_BY]->(o)
            """, {"rows": owner_links})
        if kw_nodes:
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (k:Keyword {keywordId: row.keywordId})
                SET k += row.props
            """, {"rows": kw_nodes})
        if kw_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (p:Product {id: row.productId})
                MATCH (k:Keyword {keywordId: row.keywordId})
                MERGE (p)-[:HAS_KEYWORD]->(k)
            """, {"rows": kw_links})
    self.neo.run("""
        MATCH (p:Product), (t:TaxonomyDef {label:'Product'})
        MERGE (p)-[:HAS_TAXONOMY]->(t)
    """)


def patched_inject_crm_pipeline(self):
    """Inject CRM pipeline/stages derived from crm tables and deal links."""
    if not self.crm:
        return
    # stages
    if self.crm.table_exists("pipeline_stages"):
        cols = set(self.crm.columns("pipeline_stages"))
        sel = [c for c in ["id","pipeline_stage_name","user_id","created_at","updated_at","deleted_at"] if c in cols]
        rows = self.crm.q("SELECT " + ", ".join([f"`{c}`" for c in sel]) + " FROM pipeline_stages")
        print(f"→ PipelineStages: {len(rows)}")
        payload, owner_links = [], []
        for r in rows:
            sid = r.get("id")
            if sid is None:
                continue
            sid = str(sid)
            payload.append({
                "stageId": sid,
                "props": {
                    "name": r.get("pipeline_stage_name"),
                    "createdAt": safe_iso(r.get("created_at")),
                    "updatedAt": safe_iso(r.get("updated_at")),
                    "deletedAt": safe_iso(r.get("deleted_at")),
                    "source": "crm"
                }
            })
            if r.get("user_id") is not None:
                owner_links.append({"stageId": sid, "accountId": str(r.get("user_id"))})
        for chunk in self._batch(payload, self.batch_size):
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (s:PipelineStage {stageId: row.stageId})
                SET s += row.props
            """, {"rows": chunk})
        if owner_links:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (a:Account {accountId: row.accountId})-[:MEMBER_OF|OWNS]->(o:Organization)
                MATCH (s:PipelineStage {stageId: row.stageId})
                MERGE (o)-[:OWNS_STAGE]->(s)
            """, {"rows": owner_links})
    else:
        print("→ PipelineStages: table pipeline_stages not found (skip)")

    if self.crm.table_exists("deals"):
        dcols = set(self.crm.columns("deals"))
        sel = [c for c in ["id","pipeline_id","pipeline_stage_id","user_id","organization_id","created_at","updated_at"] if c in dcols]
        rows = self.crm.q("SELECT " + ", ".join([f"`{c}`" for c in sel]) + " FROM deals")
        pipeline_map, deal_pipe_links, deal_stage_links, org_pipe_links = {}, [], [], []
        for r in rows:
            did = r.get("id")
            if did is None:
                continue
            did = str(did)
            pipe = r.get("pipeline_id")
            if pipe not in (None, ""):
                pid = str(pipe)
                pipeline_map[pid] = {"pipelineId": pid, "props": {"source": "crm"}}
                deal_pipe_links.append({"dealId": did, "pipelineId": pid})
                if r.get("organization_id") is not None:
                    org_pipe_links.append({"orgId": str(r.get("organization_id")), "pipelineId": pid})
            stage = r.get("pipeline_stage_id")
            if stage not in (None, ""):
                deal_stage_links.append({"dealId": did, "stageId": str(stage)})
        print(f"→ Pipelines(derived): {len(pipeline_map)}")
        if pipeline_map:
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (p:Pipeline {pipelineId: row.pipelineId})
                SET p += row.props
            """, {"rows": list(pipeline_map.values())})
        if deal_pipe_links:
            for chunk in self._batch(deal_pipe_links, self.batch_size):
                self.neo.run("""
                    UNWIND $rows AS row
                    MATCH (d:Deal {dealId: row.dealId})
                    MATCH (p:Pipeline {pipelineId: row.pipelineId})
                    MERGE (d)-[:IN_PIPELINE]->(p)
                """, {"rows": chunk})
        if deal_stage_links:
            for chunk in self._batch(deal_stage_links, self.batch_size):
                self.neo.run("""
                    UNWIND $rows AS row
                    MATCH (d:Deal {dealId: row.dealId})
                    MATCH (s:PipelineStage {stageId: row.stageId})
                    MERGE (d)-[:AT_STAGE]->(s)
                """, {"rows": chunk})
        if org_pipe_links:
            for chunk in self._batch(org_pipe_links, self.batch_size):
                self.neo.run("""
                    UNWIND $rows AS row
                    MATCH (o:Organization {orgId: row.orgId})
                    MATCH (p:Pipeline {pipelineId: row.pipelineId})
                    MERGE (o)-[:OWNS_PIPELINE]->(p)
                """, {"rows": chunk})
        # derive pipeline->stage from deals co-occurrence
        if deal_pipe_links and deal_stage_links:
            rows2 = self.crm.q("SELECT id, pipeline_id, pipeline_stage_id FROM deals WHERE pipeline_id IS NOT NULL AND pipeline_stage_id IS NOT NULL")
            ps_links = {}
            for r in rows2:
                ps_links[(str(r.get("pipeline_id")), str(r.get("pipeline_stage_id")))] = {"pipelineId": str(r.get("pipeline_id")), "stageId": str(r.get("pipeline_stage_id"))}
            if ps_links:
                self.neo.run("""
                    UNWIND $rows AS row
                    MATCH (p:Pipeline {pipelineId: row.pipelineId})
                    MATCH (s:PipelineStage {stageId: row.stageId})
                    MERGE (p)-[:HAS_STAGE]->(s)
                """, {"rows": list(ps_links.values())})
    else:
        print("→ Pipelines(derived): deals table not found (skip)")



# Final method bindings for top-level functions accidentally defined outside the class
InjectorV5.inject_crm_core = inject_crm_core
InjectorV5.inject_categories = patched_inject_categories
InjectorV5.inject_brands = patched_inject_brands
InjectorV5.inject_units = patched_inject_units_from_products
InjectorV5.inject_products = patched_inject_products_v2
InjectorV5.inject_crm_pipeline = patched_inject_crm_pipeline



# ============================== V16 FULL GOVERNANCE PATCHES ==============================

def run_governance_validation_pass(self, labels=None, per_label_limit=None):
    """Run full rule execution + RuleValidationResult creation against already-injected instances."""
    self.rule_engine._load_rule_cache()
    if labels is None:
        labels = []
        seen = set()
        for rule in self.rule_engine._rule_cache.values():
            ent = str(rule.get("entity") or "").strip()
            if ent and ent not in seen:
                seen.add(ent)
                labels.append(ent)
    total_results = 0
    skipped_labels = 0
    for label in labels:
        id_field = self.rule_engine._get_id_field(label)
        escaped_label = cypher_escape_identifier(label)
        escaped_id_field = cypher_escape_identifier(id_field)
        q = f"MATCH (n:{escaped_label}) WHERE n.{escaped_id_field} IS NOT NULL RETURN n.{escaped_id_field} AS id, properties(n) AS props"
        try:
            if per_label_limit:
                q += " LIMIT $limit"
                rows = self.neo.run(q, {"limit": int(per_label_limit)})
            else:
                rows = self.neo.run(q)
        except Exception as e:
            skipped_labels += 1
            print(f"⚠️ Skipping governance validation for label '{label}': {e}")
            continue
        if not rows:
            continue
        print(f"→ Governance validation {label}: {len(rows)}")
        for row in rows:
            instance_id = str(row.get("id"))
            props = row.get("props") or {}
            try:
                results = self.rule_engine.validate_instance(label, instance_id, props)
                total_results += len(results)
            except Exception as e:
                print(f"⚠️ Governance validation failed for {label}({instance_id}): {e}")
    print(f"✅ Governance validation pass complete: {total_results} RuleValidationResult rows processed; skipped_labels={skipped_labels}")


def patched_run_all_full(self, do_ui=True, do_crm=True, classify=False, xlsx_entity=None, xlsx_relationships=None, xlsx_taxonomy=None, xlsx_conditional=None, run_governance=True):
    print("\n" + "="*60)
    print("UI/CRM INJECTOR V16 - FULL GOVERNANCE MODEL")
    print("="*60 + "\n")
    self.upsert_meta_from_xlsx(entity_xlsx=xlsx_entity, rel_xlsx=xlsx_relationships, taxonomy_xlsx=xlsx_taxonomy, conditional_xlsx=xlsx_conditional)
    self.neo.create_runtime_constraints()
    if do_ui:
        print("\n---- Injecting UI Data ----")
        self.inject_users_persons_accounts_orgs(run_governance=False)
        self.inject_categories()
        self.inject_brands()
        self.inject_units(run_governance=False)
        self.inject_products(run_governance=False)
        self.inject_product_applications(run_governance=False)
        self.inject_use_cases_and_keywords(run_governance=False)
        self.inject_master_keywords()
        self.inject_facilities()
        self.inject_feature_packages()
    if do_crm:
        print("\n---- Injecting CRM Data ----")
        self.inject_crm_pipeline()
        self.inject_crm_core()
        self.inject_crm_activity()
    if run_governance and self.config.STORE_VALIDATION_RESULTS:
        print("\n---- Running Full Governance Validation ----")
        self.run_governance_validation_pass()
    if classify:
        print("\n---- Applying Taxonomy Classification ----")
        self.apply_taxonomy_classification()
    print("\n" + "="*60)
    print("INJECTION COMPLETE (V16 FULL GOVERNANCE)")
    print("="*60)


InjectorV5.run_governance_validation_pass = run_governance_validation_pass
InjectorV5.run_all = patched_run_all_full



# ============================== V17 G/I INTEGRATION PATCH ==============================

@dataclass
class ModuleFieldRule:
    db_name: str
    db_table: str
    db_column: str
    graph_label: str
    graph_property: str
    ui_field: str


class ModuleFieldMapper:
    """Best-effort parser for the latest module-fields workbook."""

    def __init__(self, xlsx_path: str):
        self.xlsx_path = xlsx_path
        self.rules: List[ModuleFieldRule] = []
        self._by_table: Dict[Tuple[str, str], List[ModuleFieldRule]] = {}
        if xlsx_path and os.path.exists(xlsx_path):
            self._load()

    def _guess_label(self, table_name: str) -> str:
        mapping = {
            'users': 'Account',
            'persons': 'Person',
            'organizations': 'Organization',
            'organization_groups': 'OrganizationGroup',
            'organisation_groups': 'OrganizationGroup',
            'stores': 'Store',
            'company_details_permissions': 'Permission',
            'custom_role_permission_overrides': 'PolicyRule',
            'feature_packages': 'Capability',
            'feature_package': 'Capability',
            'user_access_feature_plan': 'Subscription',
        }
        t = slugify(table_name)
        return mapping.get(t, table_name[:-1].title() if str(table_name).endswith('s') else str(table_name).title())

    def _load(self):
        try:
            df = pd.read_excel(self.xlsx_path).fillna("")
        except Exception as e:
            print(f"⚠️ Could not read module-fields mapping workbook: {e}")
            return
        for _, row in df.iterrows():
            db_name = str(row.get('Database', '')).strip().lower()
            db_table_raw = str(row.get('DB Table', '')).strip()
            db_col_raw = str(row.get('DB Column', '')).strip()
            ui_field = str(row.get('Unnamed: 5', '') or row.get('Sub Fields', '') or row.get('Additional Fields', '')).strip()
            if not db_name or not db_table_raw or not db_col_raw:
                continue
            tables = [t.strip() for t in re.split(r'[;|,]+', db_table_raw) if t.strip()]
            cols = [c.strip() for c in re.split(r'[;|,]+', db_col_raw) if c.strip()]
            if len(cols) == 1 and len(tables) > 1:
                cols = cols * len(tables)
            for idx, table_name in enumerate(tables):
                col_name = cols[idx] if idx < len(cols) else cols[-1]
                rule = ModuleFieldRule(
                    db_name=slugify(db_name),
                    db_table=slugify(table_name),
                    db_column=slugify(col_name),
                    graph_label=self._guess_label(table_name),
                    graph_property=slugify(col_name),
                    ui_field=ui_field or col_name,
                )
                self.rules.append(rule)
                self._by_table.setdefault((rule.db_name, rule.db_table), []).append(rule)

    def columns_for(self, db_name: str, table_name: str) -> List[str]:
        key = (slugify(db_name), slugify(table_name))
        return sorted({r.db_column for r in self._by_table.get(key, [])})

    def mapped_props(self, db_name: str, table_name: str, row: Dict[str, Any]) -> Dict[str, Any]:
        props: Dict[str, Any] = {}
        key = (slugify(db_name), slugify(table_name))
        for rule in self._by_table.get(key, []):
            if rule.db_column in row:
                props[rule.graph_property] = _kg_safe_value(row.get(rule.db_column))
            else:
                for rk, rv in row.items():
                    if slugify(rk) == rule.db_column:
                        props[rule.graph_property] = _kg_safe_value(rv)
                        break
        return props


def _get_mapper(self) -> Optional[ModuleFieldMapper]:
    mapper = getattr(self, '_module_field_mapper', None)
    if mapper is None:
        xlsx = getattr(self, 'module_fields_xlsx', '')
        if xlsx:
            mapper = ModuleFieldMapper(xlsx)
        self._module_field_mapper = mapper
    return mapper


def _select_columns_from_mapping(db: MySQL, db_name: str, table_name: str, fallback: Optional[List[str]] = None) -> List[str]:
    fallback = fallback or []
    try:
        mapper = getattr(db, '_injector_mapper', None)
        if mapper:
            cols = mapper.columns_for(db_name, table_name)
            existing = set(db.columns(table_name))
            cols = [c for c in cols if c in existing]
            merged = list(dict.fromkeys((fallback or []) + cols))
            return merged if merged else sorted(existing)
    except Exception:
        pass
    try:
        existing = db.columns(table_name)
        if fallback:
            merged = list(dict.fromkeys([c for c in fallback if c in existing] + existing))
            return merged
        return existing
    except Exception:
        return fallback


def _policy_name_from_row(row: Dict[str, Any], fallbacks: List[str]) -> Optional[str]:
    for key in fallbacks:
        v = row.get(key)
        if v not in (None, ''):
            s = str(v).strip()
            if s:
                return s
    return None


def _extend_governance_id_map():
    old = RuleExecutionEngine._get_id_field
    def _patched(self, entity_label: str) -> str:
        extra = {
            'ActiveContext': 'contextId',
            'RoleAssignment': 'roleAssignmentId',
            'Scope': 'scopeId',
            'Permission': 'permissionId',
            'PolicyRule': 'policyRuleId',
            'Capability': 'capabilityId',
        }
        if entity_label in extra:
            return extra[entity_label]
        return old(self, entity_label)
    RuleExecutionEngine._get_id_field = _patched


_extend_governance_id_map()


def extended_create_runtime_constraints(self):
    old_create_runtime_constraints(self)
    extra = [
        "CREATE CONSTRAINT active_context_id IF NOT EXISTS FOR (n:ActiveContext) REQUIRE n.contextId IS UNIQUE",
        "CREATE CONSTRAINT role_assignment_id IF NOT EXISTS FOR (n:RoleAssignment) REQUIRE n.roleAssignmentId IS UNIQUE",
        "CREATE CONSTRAINT scope_id IF NOT EXISTS FOR (n:Scope) REQUIRE n.scopeId IS UNIQUE",
        "CREATE CONSTRAINT permission_id IF NOT EXISTS FOR (n:Permission) REQUIRE n.permissionId IS UNIQUE",
        "CREATE CONSTRAINT policy_rule_id IF NOT EXISTS FOR (n:PolicyRule) REQUIRE n.policyRuleId IS UNIQUE",
        "CREATE CONSTRAINT capability_id IF NOT EXISTS FOR (n:Capability) REQUIRE n.capabilityId IS UNIQUE",
    ]
    for stmt in extra:
        try:
            self.run(stmt)
        except Exception as e:
            print(f"⚠️ Constraint skipped: {stmt[:50]}... :: {e}")


old_create_runtime_constraints = Neo4jWriter.create_runtime_constraints
Neo4jWriter.create_runtime_constraints = extended_create_runtime_constraints


def upsert_hat_rbac_governance_metadata(self):
    """Supplement the 4-XLSX governance model with G and I entities/relationships when missing."""
    taxonomy_rows = [
        {'label': 'ActiveContext', 'identityLayer': 'G Hat System', 'identityConfidenceGate': '0.6', 'exampleProperties': 'contextId, contextType, roleName', 'businessLogic': 'A user can act in multiple operational hats'},
        {'label': 'RoleAssignment', 'identityLayer': 'I RBAC/ABAC', 'identityConfidenceGate': '0.7', 'exampleProperties': 'roleAssignmentId, roleName, sourceTable', 'businessLogic': 'Role binding for an account within a scope'},
        {'label': 'Scope', 'identityLayer': 'I RBAC/ABAC', 'identityConfidenceGate': '0.5', 'exampleProperties': 'scopeId, scopeType, scopeKey', 'businessLogic': 'Boundary over which a permission or role is effective'},
        {'label': 'Permission', 'identityLayer': 'I RBAC/ABAC', 'identityConfidenceGate': '0.7', 'exampleProperties': 'permissionId, name, moduleName, actionName', 'businessLogic': 'Atomic permission derived from backend permission definitions'},
        {'label': 'PolicyRule', 'identityLayer': 'I RBAC/ABAC', 'identityConfidenceGate': '0.7', 'exampleProperties': 'policyRuleId, effect, sourceTable', 'businessLogic': 'Allow/deny rule and policy override record'},
        {'label': 'Capability', 'identityLayer': 'F Capability / I RBAC/ABAC', 'identityConfidenceGate': '0.5', 'exampleProperties': 'capabilityId, name, sourceTable', 'businessLogic': 'Feature or package entitlement that can imply permissions'},
    ]
    self.neo.run("""
        UNWIND $rows AS row
        MERGE (t:TaxonomyDef {label: row.label})
        SET t.identityLayer = coalesce(t.identityLayer, row.identityLayer),
            t.identityConfidenceGate = coalesce(t.identityConfidenceGate, row.identityConfidenceGate),
            t.exampleProperties = coalesce(t.exampleProperties, row.exampleProperties),
            t.businessLogic = coalesce(t.businessLogic, row.businessLogic)
    """, {'rows': taxonomy_rows})

    entity_rows = [
        {'entity_name': 'ActiveContext', 'database_table': 'derived_from_users_organizations_stores', 'sourceTable': 'derived', 'condition': 'Account must exist', 'businessLogic': 'Derived active hats from account ownership and organization/store presence'},
        {'entity_name': 'RoleAssignment', 'database_table': 'users', 'sourceTable': 'users', 'condition': 'default_role/user_type/account_type present', 'businessLogic': 'Role assignments derived from latest backend user fields'},
        {'entity_name': 'Scope', 'database_table': 'users;organizations;stores;organization_groups', 'sourceTable': 'derived', 'condition': 'All scope boundary entities are created first', 'businessLogic': 'Scope nodes are derived from operational boundaries'},
        {'entity_name': 'Permission', 'database_table': 'company_details_permissions', 'sourceTable': 'company_details_permissions', 'condition': 'permission definition rows exist', 'businessLogic': 'Atomic permissions projected from backend permission table'},
        {'entity_name': 'PolicyRule', 'database_table': 'custom_role_permission_overrides;company_details_permissions', 'sourceTable': 'custom_role_permission_overrides', 'condition': 'override or policy rows exist', 'businessLogic': 'Policy rules represent allow/deny semantics and overrides'},
        {'entity_name': 'Capability', 'database_table': 'feature_packages', 'sourceTable': 'feature_packages', 'condition': 'feature package rows exist', 'businessLogic': 'Capabilities are projected from feature packages'},
    ]
    self.neo.run("""
        UNWIND $rows AS row
        MERGE (e:EntityDef {entity_name: row.entity_name})
        SET e.database_table = row.database_table,
            e.sourceTable = row.sourceTable,
            e.condition = row.condition,
            e.businessLogic = row.businessLogic,
            e.layer = 'L2'
    """, {'rows': entity_rows})
    self.neo.run("""
        UNWIND $rows AS row
        MATCH (e:EntityDef {entity_name: row.entity_name})
        MATCH (t:TaxonomyDef {label: row.entity_name})
        MERGE (e)-[:BELONGS_TO_TAXONOMY]->(t)
    """, {'rows': entity_rows})

    rule_rows = [
        {'ruleType': 'ContextIntegrity', 'entity': 'ActiveContext', 'threshold': '0.60', 'notes': 'Context must point to existing account and valid operational target'},
        {'ruleType': 'RoleScopeConsistency', 'entity': 'RoleAssignment', 'threshold': '0.70', 'notes': 'Role assignments must be bound to at least one scope'},
        {'ruleType': 'PermissionValidity', 'entity': 'Permission', 'threshold': '0.70', 'notes': 'Permission must have a non-empty normalized name'},
        {'ruleType': 'PolicyOverrideValidity', 'entity': 'PolicyRule', 'threshold': '0.70', 'notes': 'Override rules must have a resolvable target or permission'},
        {'ruleType': 'CapabilityIntegrity', 'entity': 'Capability', 'threshold': '0.50', 'notes': 'Capability should map to a feature package or entitlement source'},
    ]
    self.neo.run("""
        UNWIND $rows AS row
        MERGE (r:RuleDef {ruleType: row.ruleType, entity: row.entity})
        SET r.autoMergeThreshold = row.threshold,
            r.hardEvidenceRequired = 'backend_table_or_derived',
            r.manualReviewTrigger = 'missing critical role/scope links',
            r.ttlDays = '365',
            r.decayModel = 'none',
            r.notes = row.notes,
            r.systemCheckAutomatically = 'MATCH (n:' + row.entity + ') RETURN count(n) > 0 as ok',
            r.manualReviewCondition = 'Investigate if missing edge bindings',
            r.layer = 'L3'
    """, {'rows': rule_rows})
    self.neo.run("""
        UNWIND $rows AS row
        MATCH (r:RuleDef {ruleType: row.ruleType, entity: row.entity})
        MATCH (e:EntityDef {entity_name: row.entity})
        MERGE (r)-[:VALIDATES_ENTITY]->(e)
    """, {'rows': rule_rows})

    rel_rows = [
        {'source': 'Account', 'type': 'HAS_ACTIVE_CONTEXT', 'target': 'ActiveContext', 'logic': 'Account owns one or more operational hats'},
        {'source': 'ActiveContext', 'type': 'ACTS_AS', 'target': 'Account', 'logic': 'Hat acts as the owning account'},
        {'source': 'ActiveContext', 'type': 'OPERATES_IN', 'target': 'Organization', 'logic': 'Hat may operate in an organization'},
        {'source': 'ActiveContext', 'type': 'OPERATES_IN', 'target': 'Store', 'logic': 'Hat may operate in a store'},
        {'source': 'ActiveContext', 'type': 'REPRESENTS_PERSON', 'target': 'Person', 'logic': 'Hat represents the human identity behind the account'},
        {'source': 'Account', 'type': 'HAS_ROLE', 'target': 'RoleAssignment', 'logic': 'Account receives role assignments'},
        {'source': 'RoleAssignment', 'type': 'SCOPED_TO', 'target': 'Scope', 'logic': 'Role assignment is limited by scope boundaries'},
        {'source': 'PolicyRule', 'type': 'GRANTS_ROLE_TO', 'target': 'RoleAssignment', 'logic': 'Policy may grant role assignment semantics'},
        {'source': 'PolicyRule', 'type': 'GRANTS_PERMISSION', 'target': 'Permission', 'logic': 'Policy grants atomic permissions'},
        {'source': 'PolicyRule', 'type': 'APPLIES_TO', 'target': 'Scope', 'logic': 'Policy applies to one or more scopes'},
        {'source': 'Organization', 'type': 'HAS_POLICY', 'target': 'PolicyRule', 'logic': 'Organization carries policy definitions'},
        {'source': 'OrganizationGroup', 'type': 'HAS_POLICY', 'target': 'PolicyRule', 'logic': 'Organization group carries inherited policies'},
        {'source': 'Organization', 'type': 'INHERITS_POLICY', 'target': 'OrganizationGroup', 'logic': 'Organization inherits group-level policy posture'},
        {'source': 'PolicyRule', 'type': 'OVERRIDES', 'target': 'Permission', 'logic': 'Override rules can mutate effective permission result'},
        {'source': 'Account', 'type': 'ENABLED_CAPABILITY', 'target': 'Capability', 'logic': 'Feature package enables capability for account'},
        {'source': 'Organization', 'type': 'ENABLED_CAPABILITY', 'target': 'Capability', 'logic': 'Feature package enables capability for organization'},
    ]
    self.neo.run("""
        UNWIND $rows AS row
        MERGE (rt:RelationshipType {source: row.source, type: row.type, target: row.target})
        SET rt.businessLogic = row.logic
    """, {'rows': rel_rows})


def inject_hat_system_ext(self, run_governance: bool = False):
    print("→ G Hat System (ActiveContext)")
    # Create a global scope singleton first; used by role fallback later too.
    self.neo.run("""
        MERGE (s:Scope {scopeId: 'scope:global'})
        SET s.scopeType = 'global', s.scopeKey = 'global', s.name = 'Global Scope', s.source = 'ui_derived'
    """)
    # Account root contexts
    self.neo.run("""
        MATCH (a:Account)
        MERGE (ctx:ActiveContext {contextId: 'ctx:account:' + a.accountId})
        SET ctx.contextType = 'account',
            ctx.name = coalesce(a.name, a.email, 'Account Context'),
            ctx.roleName = coalesce(a.default_role, a.user_type, a.account_type, 'account_owner'),
            ctx.source = 'ui_derived',
            ctx.accountId = a.accountId
        MERGE (a)-[:HAS_ACTIVE_CONTEXT]->(ctx)
        MERGE (ctx)-[:ACTS_AS]->(a)
        WITH a, ctx
        OPTIONAL MATCH (p:Person)-[:OWNS]->(a)
        FOREACH (_ IN CASE WHEN p IS NULL THEN [] ELSE [1] END |
            MERGE (ctx)-[:REPRESENTS_PERSON]->(p)
        )
    """)
    # Organization hats
    self.neo.run("""
        MATCH (a:Account)-[:OWNS]->(o:Organization)
        MERGE (ctx:ActiveContext {contextId: 'ctx:org:' + a.accountId + ':' + o.orgId})
        SET ctx.contextType = 'organization',
            ctx.name = coalesce(o.name, a.name, 'Organization Hat'),
            ctx.roleName = coalesce(a.default_role, a.user_type, a.account_type, 'organization_operator'),
            ctx.source = 'ui_derived',
            ctx.accountId = a.accountId,
            ctx.orgId = o.orgId
        MERGE (a)-[:HAS_ACTIVE_CONTEXT]->(ctx)
        MERGE (ctx)-[:ACTS_AS]->(a)
        MERGE (ctx)-[:OPERATES_IN]->(o)
        WITH a, ctx
        OPTIONAL MATCH (p:Person)-[:OWNS]->(a)
        FOREACH (_ IN CASE WHEN p IS NULL THEN [] ELSE [1] END |
            MERGE (ctx)-[:REPRESENTS_PERSON]->(p)
        )
    """)
    # Store hats
    self.neo.run("""
        MATCH (a:Account)-[:OWNS]->(o:Organization)-[:OWNS]->(s:Store)
        MERGE (ctx:ActiveContext {contextId: 'ctx:store:' + a.accountId + ':' + s.storeId})
        SET ctx.contextType = 'store',
            ctx.name = coalesce(s.slug, o.name, a.name, 'Store Hat'),
            ctx.roleName = coalesce(a.default_role, a.user_type, a.account_type, 'store_operator'),
            ctx.source = 'ui_derived',
            ctx.accountId = a.accountId,
            ctx.orgId = o.orgId,
            ctx.storeId = s.storeId
        MERGE (a)-[:HAS_ACTIVE_CONTEXT]->(ctx)
        MERGE (ctx)-[:ACTS_AS]->(a)
        MERGE (ctx)-[:OPERATES_IN]->(o)
        MERGE (ctx)-[:OPERATES_IN]->(s)
        WITH a, ctx
        OPTIONAL MATCH (p:Person)-[:OWNS]->(a)
        FOREACH (_ IN CASE WHEN p IS NULL THEN [] ELSE [1] END |
            MERGE (ctx)-[:REPRESENTS_PERSON]->(p)
        )
    """)


def inject_rbac_abac_ext(self, run_governance: bool = False):
    print("→ I RBAC/ABAC policy graph")
    mapper = _get_mapper(self)
    try:
        self.ui._injector_mapper = mapper
    except Exception:
        pass
    # Scopes derived from existing operational nodes
    self.neo.run("""
        MATCH (a:Account)
        MERGE (s:Scope {scopeId: 'scope:account:' + a.accountId})
        SET s.scopeType = 'account', s.scopeKey = a.accountId, s.name = coalesce(a.name, a.email, 'Account Scope'), s.source = 'ui_derived'
        MERGE (a)-[:SCOPED_TO]->(s)
    """)
    self.neo.run("""
        MATCH (g:OrganizationGroup)
        MERGE (s:Scope {scopeId: 'scope:group:' + g.groupId})
        SET s.scopeType = 'organization_group', s.scopeKey = g.groupId, s.name = coalesce(g.name, 'Organization Group Scope'), s.source = 'ui_derived'
        MERGE (g)-[:SCOPED_TO]->(s)
    """)
    self.neo.run("""
        MATCH (o:Organization)
        MERGE (s:Scope {scopeId: 'scope:org:' + o.orgId})
        SET s.scopeType = 'organization', s.scopeKey = o.orgId, s.name = coalesce(o.name, 'Organization Scope'), s.source = 'ui_derived'
        MERGE (o)-[:SCOPED_TO]->(s)
        WITH o, s
        OPTIONAL MATCH (o)-[:MEMBER_OF_GROUP]->(g:OrganizationGroup)
        FOREACH (_ IN CASE WHEN g IS NULL THEN [] ELSE [1] END |
            MERGE (o)-[:INHERITS_POLICY]->(g)
        )
        WITH o, s, g
        FOREACH (_ IN CASE WHEN g IS NULL THEN [] ELSE [1] END |
            MERGE (parent:Scope {scopeId: 'scope:group:' + g.groupId})
            ON CREATE SET parent.scopeType = 'organization_group',
                          parent.scopeKey = g.groupId,
                          parent.name = coalesce(g.name, 'Organization Group Scope'),
                          parent.source = 'ui_derived'
            MERGE (s)-[:PARENT_SCOPE]->(parent)
        )
    """)
    self.neo.run("""
        MATCH (snode:Store)
        MERGE (s:Scope {scopeId: 'scope:store:' + snode.storeId})
        SET s.scopeType = 'store', s.scopeKey = snode.storeId, s.name = coalesce(snode.slug, 'Store Scope'), s.source = 'ui_derived'
        MERGE (snode)-[:SCOPED_TO]->(s)
        WITH snode, s
        OPTIONAL MATCH (o:Organization)-[:OWNS]->(snode)
        FOREACH (_ IN CASE WHEN o IS NULL THEN [] ELSE [1] END |
            MERGE (parent:Scope {scopeId: 'scope:org:' + o.orgId})
            ON CREATE SET parent.scopeType = 'organization',
                          parent.scopeKey = o.orgId,
                          parent.name = coalesce(o.name, 'Organization Scope'),
                          parent.source = 'ui_derived'
            MERGE (s)-[:PARENT_SCOPE]->(parent)
        )
    """)

    # Role assignments derived from latest users table
    if self.ui.table_exists('users'):
        ucols = set(_select_columns_from_mapping(self.ui, 'baba_stagings', 'users', fallback=['id','default_role','role','role_name','user_type','account_type','person_id','organization_id','store_id','organization_group_id','created_at','updated_at']))
        required = ['id']
        select_cols = [c for c in dict.fromkeys(required + list(ucols)) if c in set(self.ui.columns('users'))]
        rows = self.ui.q("SELECT " + ", ".join([f"`{c}`" for c in select_cols]) + " FROM `users`")
        role_rows: List[Dict[str, Any]] = []
        role_scope_rows: List[Dict[str, Any]] = []
        account_role_rows: List[Dict[str, Any]] = []
        for r in rows:
            account_id = r.get('id')
            if account_id is None:
                continue
            account_id = str(account_id)
            role_names = []
            for key in ['default_role','role_name','role','user_type','account_type']:
                val = r.get(key)
                if val not in (None, ''):
                    sval = str(val).strip()
                    if sval and sval.lower() not in {x.lower() for x in role_names}:
                        role_names.append(sval)
            if not role_names:
                role_names = ['account_user']
            mapped = mapper.mapped_props('baba_stagings', 'users', r) if mapper else {}
            for role_name in role_names:
                ra_id = stable_id('role_assignment', account_id, role_name)
                props = {
                    'roleName': role_name,
                    'source': 'ui',
                    'sourceTable': 'users',
                    'createdAt': safe_iso(r.get('created_at')),
                    'updatedAt': safe_iso(r.get('updated_at')),
                    **mapped,
                }
                role_rows.append({'roleAssignmentId': ra_id, 'props': props})
                account_role_rows.append({'accountId': account_id, 'roleAssignmentId': ra_id})
                target_scope_ids = ['scope:account:' + account_id]
                if r.get('organization_id') is not None:
                    target_scope_ids.append('scope:org:' + str(r.get('organization_id')))
                if r.get('organization_group_id') is not None:
                    target_scope_ids.append('scope:group:' + str(r.get('organization_group_id')))
                if r.get('store_id') is not None:
                    target_scope_ids.append('scope:store:' + str(r.get('store_id')))
                if not target_scope_ids:
                    target_scope_ids = ['scope:global']
                for scope_id in list(dict.fromkeys(target_scope_ids)):
                    role_scope_rows.append({'roleAssignmentId': ra_id, 'scopeId': scope_id})
        for chunk in self._batch(role_rows, self.batch_size):
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (ra:RoleAssignment {roleAssignmentId: row.roleAssignmentId})
                SET ra += row.props
            """, {'rows': chunk})
        if account_role_rows:
            for chunk in self._batch(account_role_rows, self.batch_size):
                self.neo.run("""
                    UNWIND $rows AS row
                    MATCH (a:Account {accountId: row.accountId})
                    MATCH (ra:RoleAssignment {roleAssignmentId: row.roleAssignmentId})
                    MERGE (a)-[:HAS_ROLE]->(ra)
                """, {'rows': chunk})
        if role_scope_rows:
            for chunk in self._batch(role_scope_rows, self.batch_size):
                self.neo.run("""
                    UNWIND $rows AS row
                    MATCH (ra:RoleAssignment {roleAssignmentId: row.roleAssignmentId})
                    MATCH (s:Scope {scopeId: row.scopeId})
                    MERGE (ra)-[:SCOPED_TO]->(s)
                """, {'rows': chunk})

    # Permissions from company_details_permissions
    perm_table = _find_first_table(self.ui, ['company_details_permissions', 'company_detail_permissions'])
    if perm_table:
        pcols = set(_select_columns_from_mapping(self.ui, 'baba_stagings', perm_table, fallback=['id','user_id','organization_id','organization_group_id','store_id','module','module_name','sub_module','permission','permission_name','action','is_allowed','allow','deny','created_at','updated_at']))
        rows = self.ui.q("SELECT " + ", ".join([f"`{c}`" for c in pcols if c in set(self.ui.columns(perm_table))]) + f" FROM `{perm_table}`")
        perm_nodes, policy_nodes, grant_rows, applies_rows, org_policy_rows, group_policy_rows, account_policy_rows = [], [], [], [], [], [], []
        for r in rows:
            rid = r.get('id')
            mapped = mapper.mapped_props('baba_stagings', perm_table, r) if mapper else {}
            module_name = _policy_name_from_row(r, ['module_name','module','sub_module']) or 'general'
            action_name = _policy_name_from_row(r, ['action','permission_name','permission']) or 'access'
            perm_name = f"{module_name}:{action_name}"
            perm_id = stable_id('permission', perm_name)
            perm_nodes.append({'permissionId': perm_id, 'props': {'name': perm_name, 'moduleName': module_name, 'actionName': action_name, 'source': 'ui', 'sourceTable': perm_table, **mapped}})
            effect_val = r.get('effect')
            if effect_val in (None, ''):
                if r.get('deny') not in (None, ''):
                    effect_val = 'deny' if str(r.get('deny')).strip() not in ('0','false','False','') else 'allow'
                elif r.get('is_allowed') not in (None, '') or r.get('allow') not in (None, ''):
                    allowed = r.get('is_allowed') if r.get('is_allowed') not in (None, '') else r.get('allow')
                    effect_val = 'allow' if str(allowed).strip() not in ('0','false','False','') else 'deny'
                else:
                    effect_val = 'allow'
            policy_id = stable_id('policy_rule', perm_table, rid or perm_name)
            policy_nodes.append({'policyRuleId': policy_id, 'props': {'name': perm_name, 'effect': str(effect_val), 'source': 'ui', 'sourceTable': perm_table, 'createdAt': safe_iso(r.get('created_at')), 'updatedAt': safe_iso(r.get('updated_at')), **mapped}})
            grant_rows.append({'policyRuleId': policy_id, 'permissionId': perm_id})
            scope_ids = ['scope:global']
            if r.get('user_id') is not None:
                scope_ids.append('scope:account:' + str(r.get('user_id')))
                account_policy_rows.append({'accountId': str(r.get('user_id')), 'policyRuleId': policy_id})
            if r.get('organization_id') is not None:
                scope_ids.append('scope:org:' + str(r.get('organization_id')))
                org_policy_rows.append({'orgId': str(r.get('organization_id')), 'policyRuleId': policy_id})
            if r.get('organization_group_id') is not None:
                scope_ids.append('scope:group:' + str(r.get('organization_group_id')))
                group_policy_rows.append({'groupId': str(r.get('organization_group_id')), 'policyRuleId': policy_id})
            if r.get('store_id') is not None:
                scope_ids.append('scope:store:' + str(r.get('store_id')))
            for scope_id in list(dict.fromkeys(scope_ids)):
                applies_rows.append({'policyRuleId': policy_id, 'scopeId': scope_id})
        if perm_nodes:
            for chunk in self._batch(perm_nodes, self.batch_size):
                self.neo.run("""
                    UNWIND $rows AS row
                    MERGE (p:Permission {permissionId: row.permissionId})
                    SET p += row.props
                """, {'rows': chunk})
        if policy_nodes:
            for chunk in self._batch(policy_nodes, self.batch_size):
                self.neo.run("""
                    UNWIND $rows AS row
                    MERGE (pr:PolicyRule {policyRuleId: row.policyRuleId})
                    SET pr += row.props
                """, {'rows': chunk})
        if grant_rows:
            for chunk in self._batch(grant_rows, self.batch_size):
                self.neo.run("""
                    UNWIND $rows AS row
                    MATCH (pr:PolicyRule {policyRuleId: row.policyRuleId})
                    MATCH (p:Permission {permissionId: row.permissionId})
                    MERGE (pr)-[:GRANTS_PERMISSION]->(p)
                """, {'rows': chunk})
        if applies_rows:
            for chunk in self._batch(applies_rows, self.batch_size):
                self.neo.run("""
                    UNWIND $rows AS row
                    MATCH (pr:PolicyRule {policyRuleId: row.policyRuleId})
                    MATCH (s:Scope {scopeId: row.scopeId})
                    MERGE (pr)-[:APPLIES_TO]->(s)
                """, {'rows': chunk})
        if org_policy_rows:
            for chunk in self._batch(org_policy_rows, self.batch_size):
                self.neo.run("""
                    UNWIND $rows AS row
                    MATCH (o:Organization {orgId: row.orgId})
                    MATCH (pr:PolicyRule {policyRuleId: row.policyRuleId})
                    MERGE (o)-[:HAS_POLICY]->(pr)
                """, {'rows': chunk})
        if group_policy_rows:
            for chunk in self._batch(group_policy_rows, self.batch_size):
                self.neo.run("""
                    UNWIND $rows AS row
                    MATCH (g:OrganizationGroup {groupId: row.groupId})
                    MATCH (pr:PolicyRule {policyRuleId: row.policyRuleId})
                    MERGE (g)-[:HAS_POLICY]->(pr)
                """, {'rows': chunk})
        if account_policy_rows:
            for chunk in self._batch(account_policy_rows, self.batch_size):
                self.neo.run("""
                    UNWIND $rows AS row
                    MATCH (a:Account {accountId: row.accountId})-[:HAS_ROLE]->(ra:RoleAssignment)
                    MATCH (pr:PolicyRule {policyRuleId: row.policyRuleId})
                    MERGE (pr)-[:GRANTS_ROLE_TO]->(ra)
                """, {'rows': chunk})

    # Overrides from custom_role_permission_overrides
    override_table = _find_first_table(self.ui, ['custom_role_permission_overrides'])
    if override_table:
        ocols = set(_select_columns_from_mapping(self.ui, 'baba_stagings', override_table, fallback=['id','user_id','organization_id','organization_group_id','store_id','role','role_name','permission','permission_name','module','action','effect','is_allowed','created_at','updated_at']))
        rows = self.ui.q("SELECT " + ", ".join([f"`{c}`" for c in ocols if c in set(self.ui.columns(override_table))]) + f" FROM `{override_table}`")
        override_nodes, override_links, applies_rows, target_role_rows = [], [], [], []
        for r in rows:
            rid = r.get('id')
            mapped = mapper.mapped_props('baba_stagings', override_table, r) if mapper else {}
            module_name = _policy_name_from_row(r, ['module','module_name']) or 'general'
            action_name = _policy_name_from_row(r, ['action','permission_name','permission']) or 'access'
            perm_name = f"{module_name}:{action_name}"
            perm_id = stable_id('permission', perm_name)
            self.neo.run("""
                MERGE (p:Permission {permissionId: $permissionId})
                SET p.name = $name, p.moduleName = $moduleName, p.actionName = $actionName, p.source = 'ui', p.sourceTable = $sourceTable
            """, {'permissionId': perm_id, 'name': perm_name, 'moduleName': module_name, 'actionName': action_name, 'sourceTable': override_table})
            effect_val = r.get('effect')
            if effect_val in (None, ''):
                allowed = r.get('is_allowed')
                effect_val = 'allow' if str(allowed).strip() not in ('0','false','False','') else 'deny'
            policy_id = stable_id('override', override_table, rid or perm_name)
            override_nodes.append({'policyRuleId': policy_id, 'props': {'name': perm_name, 'effect': str(effect_val), 'isOverride': True, 'source': 'ui', 'sourceTable': override_table, 'createdAt': safe_iso(r.get('created_at')), 'updatedAt': safe_iso(r.get('updated_at')), **mapped}})
            override_links.append({'policyRuleId': policy_id, 'permissionId': perm_id})
            scope_ids = ['scope:global']
            if r.get('user_id') is not None:
                scope_ids.append('scope:account:' + str(r.get('user_id')))
            if r.get('organization_id') is not None:
                scope_ids.append('scope:org:' + str(r.get('organization_id')))
            if r.get('organization_group_id') is not None:
                scope_ids.append('scope:group:' + str(r.get('organization_group_id')))
            if r.get('store_id') is not None:
                scope_ids.append('scope:store:' + str(r.get('store_id')))
            for scope_id in list(dict.fromkeys(scope_ids)):
                applies_rows.append({'policyRuleId': policy_id, 'scopeId': scope_id})
            role_name = _policy_name_from_row(r, ['role_name','role'])
            if r.get('user_id') is not None and role_name:
                target_role_rows.append({'policyRuleId': policy_id, 'accountId': str(r.get('user_id')), 'roleName': role_name})
        if override_nodes:
            for chunk in self._batch(override_nodes, self.batch_size):
                self.neo.run("""
                    UNWIND $rows AS row
                    MERGE (pr:PolicyRule {policyRuleId: row.policyRuleId})
                    SET pr += row.props
                """, {'rows': chunk})
        if override_links:
            for chunk in self._batch(override_links, self.batch_size):
                self.neo.run("""
                    UNWIND $rows AS row
                    MATCH (pr:PolicyRule {policyRuleId: row.policyRuleId})
                    MATCH (p:Permission {permissionId: row.permissionId})
                    MERGE (pr)-[:OVERRIDES]->(p)
                """, {'rows': chunk})
        if applies_rows:
            for chunk in self._batch(applies_rows, self.batch_size):
                self.neo.run("""
                    UNWIND $rows AS row
                    MATCH (pr:PolicyRule {policyRuleId: row.policyRuleId})
                    MATCH (s:Scope {scopeId: row.scopeId})
                    MERGE (pr)-[:APPLIES_TO]->(s)
                """, {'rows': chunk})
        if target_role_rows:
            for chunk in self._batch(target_role_rows, self.batch_size):
                self.neo.run("""
                    UNWIND $rows AS row
                    MATCH (a:Account {accountId: row.accountId})-[:HAS_ROLE]->(ra:RoleAssignment)
                    WHERE toLower(coalesce(ra.roleName, '')) = toLower(row.roleName)
                    MATCH (pr:PolicyRule {policyRuleId: row.policyRuleId})
                    MERGE (pr)-[:GRANTS_ROLE_TO]->(ra)
                """, {'rows': chunk})

    # Capabilities from feature packages and subscriptions
    cap_table = _find_first_table(self.ui, ['feature_packages', 'feature_package'])
    if cap_table:
        ccols = set(_select_columns_from_mapping(self.ui, 'baba_stagings', cap_table, fallback=['id','name','title','slug','description','created_at','updated_at','deleted_at']))
        rows = self.ui.q("SELECT " + ", ".join([f"`{c}`" for c in ccols if c in set(self.ui.columns(cap_table))]) + f" FROM `{cap_table}`")
        cap_rows = []
        for r in rows:
            cid = r.get('id')
            if cid is None:
                continue
            mapped = mapper.mapped_props('baba_stagings', cap_table, r) if mapper else {}
            cap_rows.append({'capabilityId': str(cid), 'props': {'name': r.get('name') or r.get('title') or r.get('slug') or f'capability:{cid}', 'source': 'ui', 'sourceTable': cap_table, 'createdAt': safe_iso(r.get('created_at')), 'updatedAt': safe_iso(r.get('updated_at')), 'deletedAt': safe_iso(r.get('deleted_at')), **mapped}})
        if cap_rows:
            for chunk in self._batch(cap_rows, self.batch_size):
                self.neo.run("""
                    UNWIND $rows AS row
                    MERGE (c:Capability {capabilityId: row.capabilityId})
                    SET c += row.props
                """, {'rows': chunk})
        if self.ui.table_exists('user_access_feature_plan'):
            rows = self.ui.q("SELECT * FROM `user_access_feature_plan`")
            account_cap_rows = []
            for r in rows:
                user_id = r.get('user_id')
                plan_id = r.get('plan_id') or r.get('feature_package_id')
                if user_id is not None and plan_id is not None:
                    account_cap_rows.append({'accountId': str(user_id), 'capabilityId': str(plan_id)})
            if account_cap_rows:
                for chunk in self._batch(account_cap_rows, self.batch_size):
                    self.neo.run("""
                        UNWIND $rows AS row
                        MATCH (a:Account {accountId: row.accountId})
                        MATCH (c:Capability {capabilityId: row.capabilityId})
                        MERGE (a)-[:ENABLED_CAPABILITY]->(c)
                        WITH a, c
                        OPTIONAL MATCH (a)-[:OWNS]->(o:Organization)
                        FOREACH (_ IN CASE WHEN o IS NULL THEN [] ELSE [1] END |
                            MERGE (o)-[:ENABLED_CAPABILITY]->(c)
                        )
                    """, {'rows': chunk})


def patched_run_all_full_gi(self, do_ui=True, do_crm=True, classify=False, xlsx_entity=None, xlsx_relationships=None, xlsx_taxonomy=None, xlsx_conditional=None, run_governance=True):
    print("\n" + "="*60)
    print("UI/CRM INJECTOR V16 - FULL GOVERNANCE + G/I")
    print("="*60 + "\n")
    self.upsert_meta_from_xlsx(entity_xlsx=xlsx_entity, rel_xlsx=xlsx_relationships, taxonomy_xlsx=xlsx_taxonomy, conditional_xlsx=xlsx_conditional)
    self.upsert_hat_rbac_governance_metadata()
    self.neo.create_runtime_constraints()
    if do_ui:
        print("\n---- Injecting UI Data ----")
        self.inject_users_persons_accounts_orgs(run_governance=False)
        self.inject_categories()
        self.inject_brands()
        self.inject_units(run_governance=False)
        self.inject_products(run_governance=False)
        self.inject_product_applications(run_governance=False)
        self.inject_use_cases_and_keywords(run_governance=False)
        self.inject_master_keywords()
        self.inject_facilities()
        self.inject_feature_packages()
        print("\n---- Injecting G/I Graph Layers ----")
        self.inject_hat_system(run_governance=False)
        self.inject_rbac_abac(run_governance=False)
    if do_crm:
        print("\n---- Injecting CRM Data ----")
        self.inject_crm_pipeline()
        self.inject_crm_core()
        self.inject_crm_activity()
    if run_governance and self.config.STORE_VALIDATION_RESULTS:
        print("\n---- Running Full Governance Validation ----")
        self.run_governance_validation_pass()
    if classify:
        print("\n---- Applying Taxonomy Classification ----")
        self.apply_taxonomy_classification()
    print("\n" + "="*60)
    print("INJECTION COMPLETE (V16 FULL GOVERNANCE + G/I)")
    print("="*60)


old_build_arg_parser = build_arg_parser

def build_arg_parser() -> argparse.ArgumentParser:
    ap = old_build_arg_parser()
    ap.add_argument("--module-fields-xlsx", default="", help="Latest module fields mapping workbook used to normalize/align DB->graph properties")
    return ap


def main():
    ap = build_arg_parser()
    args = ap.parse_args()

    gov_config = GovernanceConfig()
    gov_config.ENFORCE_CONFIDENCE_GATES = args.enforce_gates
    gov_config.STORE_VALIDATION_RESULTS = not args.skip_governance
    batch_size = args.batch_size
    if args.fast_local:
        batch_size = max(batch_size, 15000)

    neo = Neo4jWriter(args.neo4j_uri, args.neo4j_user, args.neo4j_pass)
    ui = MySQL(MySQLConnInfo(
        host=args.ui_mysql_host,
        port=args.ui_mysql_port,
        user=args.ui_mysql_user,
        password=args.ui_mysql_pass,
        db=args.ui_db,
    ))
    crm = None
    if not args.skip_crm:
        crm = MySQL(MySQLConnInfo(
            host=args.crm_mysql_host,
            port=args.crm_mysql_port,
            user=args.crm_mysql_user,
            password=args.crm_mysql_pass,
            db=args.crm_db,
        ))
    if args.limit:
        ui.limit = args.limit
        if crm:
            crm.limit = args.limit
    try:
        injector = InjectorV5(
            neo=neo,
            ui=ui,
            crm=crm,
            batch_size=batch_size,
            governance_config=gov_config,
        )
        injector.module_fields_xlsx = args.module_fields_xlsx
        injector._module_field_mapper = ModuleFieldMapper(args.module_fields_xlsx) if args.module_fields_xlsx else None
        runner = getattr(injector, 'run_all', None)
        if not callable(runner):
            raise AttributeError('Injector has no runnable entrypoint')
        runner(
            do_ui=not args.skip_ui,
            do_crm=not args.skip_crm,
            classify=args.classify,
            xlsx_entity=args.entity_xlsx,
            xlsx_relationships=args.relationships_xlsx,
            xlsx_taxonomy=args.taxonomy_xlsx,
            xlsx_conditional=args.conditional_xlsx,
            run_governance=not args.skip_governance,
        )
    finally:
        try:
            ui.close()
        except Exception:
            pass
        try:
            if crm:
                crm.close()
        except Exception:
            pass
        neo.close()


InjectorV5.upsert_hat_rbac_governance_metadata = upsert_hat_rbac_governance_metadata
InjectorV5.inject_hat_system = inject_hat_system_ext
InjectorV5.inject_rbac_abac = inject_rbac_abac_ext
InjectorV5.run_all = patched_run_all_full_gi



# ============================== V18 PROVENANCE + M-AC PATCHES ==============================

def extend_runtime_constraints_v18(self):
    extended_create_runtime_constraints(self)
    extra = [
        "CREATE CONSTRAINT lead_source_id IF NOT EXISTS FOR (n:LeadSource) REQUIRE n.sourceId IS UNIQUE",
        "CREATE CONSTRAINT external_source_id IF NOT EXISTS FOR (n:ExternalSource) REQUIRE n.externalSourceId IS UNIQUE",
        "CREATE CONSTRAINT data_source_id IF NOT EXISTS FOR (n:DataSource) REQUIRE n.dataSourceId IS UNIQUE",
        "CREATE CONSTRAINT ip_identity_id IF NOT EXISTS FOR (n:IPIdentity) REQUIRE n.ipId IS UNIQUE",
        "CREATE CONSTRAINT provenance_event_id IF NOT EXISTS FOR (n:ProvenanceEvent) REQUIRE n.eventId IS UNIQUE",
        "CREATE CONSTRAINT attribution_touch_id IF NOT EXISTS FOR (n:AttributionTouch) REQUIRE n.touchId IS UNIQUE",
        "CREATE CONSTRAINT signal_id IF NOT EXISTS FOR (n:Signal) REQUIRE n.signalId IS UNIQUE",
        "CREATE CONSTRAINT intent_id IF NOT EXISTS FOR (n:Intent) REQUIRE n.intentId IS UNIQUE",
        "CREATE CONSTRAINT anonymous_visitor_id IF NOT EXISTS FOR (n:AnonymousVisitor) REQUIRE n.visitorId IS UNIQUE",
        "CREATE CONSTRAINT ghost_profile_id IF NOT EXISTS FOR (n:GhostProfile) REQUIRE n.profileId IS UNIQUE",
        "CREATE CONSTRAINT fingerprint_id IF NOT EXISTS FOR (n:Fingerprint) REQUIRE n.fingerprintId IS UNIQUE",
        "CREATE CONSTRAINT truth_engine_id IF NOT EXISTS FOR (n:TruthEngine) REQUIRE n.truthId IS UNIQUE",
        "CREATE CONSTRAINT predictive_score_id IF NOT EXISTS FOR (n:PredictiveScore) REQUIRE n.predictionId IS UNIQUE",
        "CREATE CONSTRAINT data_waterfall_id IF NOT EXISTS FOR (n:DataWaterfall) REQUIRE n.waterfallId IS UNIQUE",
        "CREATE CONSTRAINT credit_escrow_id IF NOT EXISTS FOR (n:CreditEscrow) REQUIRE n.escrowId IS UNIQUE",
        "CREATE CONSTRAINT agentic_dispatcher_id IF NOT EXISTS FOR (n:AgenticDispatcher) REQUIRE n.dispatcherId IS UNIQUE",
        "CREATE CONSTRAINT intelligence_finding_id IF NOT EXISTS FOR (n:IntelligenceFinding) REQUIRE n.findingId IS UNIQUE",
        "CREATE CONSTRAINT competitor_intel_id IF NOT EXISTS FOR (n:CompetitorIntel) REQUIRE n.competitorIntelId IS UNIQUE",
        "CREATE CONSTRAINT buyer_committee_id IF NOT EXISTS FOR (n:BuyerCommittee) REQUIRE n.committeeId IS UNIQUE",
    ]
    for stmt in extra:
        try:
            self.run(stmt)
        except Exception as e:
            print(f"⚠️ Constraint skipped: {stmt[:60]}... :: {e}")

Neo4jWriter.create_runtime_constraints = extend_runtime_constraints_v18


def _extend_governance_id_map_v18():
    old = RuleExecutionEngine._get_id_field
    def _patched(self, entity_label: str) -> str:
        extra = {
            'LeadSource': 'sourceId',
            'ExternalSource': 'externalSourceId',
            'DataSource': 'dataSourceId',
            'IPIdentity': 'ipId',
            'ProvenanceEvent': 'eventId',
            'AttributionTouch': 'touchId',
            'Signal': 'signalId',
            'Intent': 'intentId',
            'AnonymousVisitor': 'visitorId',
            'GhostProfile': 'profileId',
            'Fingerprint': 'fingerprintId',
            'TruthEngine': 'truthId',
            'PredictiveScore': 'predictionId',
            'DataWaterfall': 'waterfallId',
            'CreditEscrow': 'escrowId',
            'AgenticDispatcher': 'dispatcherId',
            'IntelligenceFinding': 'findingId',
            'CompetitorIntel': 'competitorIntelId',
            'BuyerCommittee': 'committeeId',
        }
        if entity_label in extra:
            return extra[entity_label]
        return old(self, entity_label)
    RuleExecutionEngine._get_id_field = _patched

_extend_governance_id_map_v18()


def upsert_provenance_and_advanced_governance_metadata(self):
    taxonomy_rows = [
        {'label': 'LeadSource', 'identityLayer': 'L Provenance', 'identityConfidenceGate': '0.6', 'exampleProperties': 'sourceId, sourceType, channel', 'businessLogic': 'Canonical source record for lead provenance'},
        {'label': 'ExternalSource', 'identityLayer': 'L Provenance', 'identityConfidenceGate': '0.5', 'exampleProperties': 'externalSourceId, name, sourceKind', 'businessLogic': 'External or logical source system/channel'},
        {'label': 'DataSource', 'identityLayer': 'L Provenance', 'identityConfidenceGate': '0.5', 'exampleProperties': 'dataSourceId, dbName, tableName', 'businessLogic': 'Database/table origin of evidence'},
        {'label': 'IPIdentity', 'identityLayer': 'L Provenance', 'identityConfidenceGate': '0.5', 'exampleProperties': 'ipId, ipAddress', 'businessLogic': 'IP-level identity and provenance anchor'},
        {'label': 'ProvenanceEvent', 'identityLayer': 'L Provenance', 'identityConfidenceGate': '0.6', 'exampleProperties': 'eventId, eventType, eventTs', 'businessLogic': 'Normalized provenance touch or evidence event'},
        {'label': 'AttributionTouch', 'identityLayer': 'L Provenance', 'identityConfidenceGate': '0.6', 'exampleProperties': 'touchId, channel, score', 'businessLogic': 'Attribution touchpoint contributing to a lead'},
        {'label': 'Signal', 'identityLayer': 'M/N/O/P/Q', 'identityConfidenceGate': '0.6', 'exampleProperties': 'signalId, signalType, weight', 'businessLogic': 'Behavioral or commercial signal'},
        {'label': 'Intent', 'identityLayer': 'M/N/O/P/Q', 'identityConfidenceGate': '0.65', 'exampleProperties': 'intentId, intentType, score', 'businessLogic': 'Aggregated intent profile from signals'},
        {'label': 'AnonymousVisitor', 'identityLayer': 'N/O', 'identityConfidenceGate': '0.5', 'exampleProperties': 'visitorId, sessionId', 'businessLogic': 'Anonymous browsing entity'},
        {'label': 'GhostProfile', 'identityLayer': 'N/O', 'identityConfidenceGate': '0.55', 'exampleProperties': 'profileId, fingerprintId, ipId', 'businessLogic': 'Probabilistic anonymous identity graph'},
        {'label': 'Fingerprint', 'identityLayer': 'N/O', 'identityConfidenceGate': '0.55', 'exampleProperties': 'fingerprintId, userAgentHash, ipHash', 'businessLogic': 'Session/device fingerprint'},
        {'label': 'TruthEngine', 'identityLayer': 'W/X', 'identityConfidenceGate': '0.7', 'exampleProperties': 'truthId, confidence, evidenceCount', 'businessLogic': 'Evidence consolidation and truth scoring'},
        {'label': 'PredictiveScore', 'identityLayer': 'V/AA', 'identityConfidenceGate': '0.65', 'exampleProperties': 'predictionId, scoreType, score', 'businessLogic': 'Predictive opportunity or conversion scoring'},
        {'label': 'DataWaterfall', 'identityLayer': 'Z', 'identityConfidenceGate': '0.55', 'exampleProperties': 'waterfallId, sourceDepth, evidenceCount', 'businessLogic': 'Progressive data enrichment waterfall'},
        {'label': 'CreditEscrow', 'identityLayer': 'Y', 'identityConfidenceGate': '0.55', 'exampleProperties': 'escrowId, amount, status', 'businessLogic': 'Commercial/monetization state around deal confidence'},
        {'label': 'AgenticDispatcher', 'identityLayer': 'AB/AC', 'identityConfidenceGate': '0.55', 'exampleProperties': 'dispatcherId, nextBestAction, priority', 'businessLogic': 'Agentic routing and next-best-action coordinator'},
        {'label': 'IntelligenceFinding', 'identityLayer': 'R/S/T/U', 'identityConfidenceGate': '0.55', 'exampleProperties': 'findingId, findingType, score', 'businessLogic': 'Derived external/trade/competitor intelligence finding'},
        {'label': 'CompetitorIntel', 'identityLayer': 'U', 'identityConfidenceGate': '0.55', 'exampleProperties': 'competitorIntelId, competitorName, evidenceScore', 'businessLogic': 'Competitor-oriented intelligence node'},
        {'label': 'BuyerCommittee', 'identityLayer': 'P/Q', 'identityConfidenceGate': '0.55', 'exampleProperties': 'committeeId, size, confidence', 'businessLogic': 'Buying group / multi-person committee inference'},
    ]
    self.neo.run("""
        UNWIND $rows AS row
        MERGE (t:TaxonomyDef {label: row.label})
        SET t.identityLayer = coalesce(t.identityLayer, row.identityLayer),
            t.identityConfidenceGate = coalesce(t.identityConfidenceGate, row.identityConfidenceGate),
            t.exampleProperties = coalesce(t.exampleProperties, row.exampleProperties),
            t.businessLogic = coalesce(t.businessLogic, row.businessLogic)
    """, {'rows': taxonomy_rows})

    entity_rows = [
        {'entity_name':'LeadSource','database_table':'rfqs;crm_leads;page_visits;lead_threads','sourceTable':'derived','condition':'source touch exists','businessLogic':'Canonical source entity for leads/RFQs/threads/behavioral touches'},
        {'entity_name':'ExternalSource','database_table':'rfqs','sourceTable':'derived','condition':'source type or channel exists','businessLogic':'External or logical origin channel'},
        {'entity_name':'DataSource','database_table':'all','sourceTable':'derived','condition':'db/table known','businessLogic':'Tracks source database and table lineage'},
        {'entity_name':'IPIdentity','database_table':'sessions;page_visits','sourceTable':'derived','condition':'ip address exists','businessLogic':'Identity anchor built from IP'},
        {'entity_name':'ProvenanceEvent','database_table':'rfqs;crm_leads;page_visits;lead_threads;crm_meeting_informations','sourceTable':'derived','condition':'evidence event exists','businessLogic':'Normalized lineage/provenance event'},
        {'entity_name':'AttributionTouch','database_table':'page_visits;rfqs;crm_leads','sourceTable':'derived','condition':'touchpoint exists','businessLogic':'Attribution touch preceding or contributing to lead generation'},
        {'entity_name':'Signal','database_table':'page_visits;rfqs;lead_threads;crm_meeting_informations;crm_task_informations','sourceTable':'derived','condition':'activity exists','businessLogic':'Behavioral and commercial signals'},
        {'entity_name':'Intent','database_table':'derived_from_signals','sourceTable':'derived','condition':'signals aggregated','businessLogic':'Intent profile aggregated from signals'},
        {'entity_name':'AnonymousVisitor','database_table':'sessions;page_visits','sourceTable':'derived','condition':'session has no account owner','businessLogic':'Anonymous visitor graph node'},
        {'entity_name':'GhostProfile','database_table':'sessions;page_visits','sourceTable':'derived','condition':'ip or fingerprint exists','businessLogic':'Probabilistic anonymous identity graph'},
        {'entity_name':'Fingerprint','database_table':'sessions;page_visits','sourceTable':'derived','condition':'user agent or ip exists','businessLogic':'Stable device/session signature'},
        {'entity_name':'TruthEngine','database_table':'derived_from_evidence','sourceTable':'derived','condition':'lead/org/activity exists','businessLogic':'Truth/evidence engine for confidence and source fusion'},
        {'entity_name':'PredictiveScore','database_table':'derived_from_leads_deals_signals','sourceTable':'derived','condition':'lead/deal/account exists','businessLogic':'Predictive conversion and opportunity scoring'},
        {'entity_name':'DataWaterfall','database_table':'derived_from_provenance','sourceTable':'derived','condition':'lead/org exists','businessLogic':'Tracks enrichment depth and coverage'},
        {'entity_name':'CreditEscrow','database_table':'deals','sourceTable':'deals','condition':'deal exists','businessLogic':'Monetization/credit escrow abstraction for commercial state'},
        {'entity_name':'AgenticDispatcher','database_table':'derived_from_leads_threads_tasks','sourceTable':'derived','condition':'lead or thread exists','businessLogic':'Agentic routing and next best action'},
        {'entity_name':'IntelligenceFinding','database_table':'derived_from_products_orgs_pageviews','sourceTable':'derived','condition':'competitive or trade pattern exists','businessLogic':'Trade/external/competitor intelligence finding'},
        {'entity_name':'CompetitorIntel','database_table':'derived_from_products_brands_pageviews','sourceTable':'derived','condition':'competitor pattern exists','businessLogic':'Competitor intelligence node'},
        {'entity_name':'BuyerCommittee','database_table':'derived_from_threads_meetings_accounts','sourceTable':'derived','condition':'multi-actor buying pattern exists','businessLogic':'Buying committee inference'},
    ]
    self.neo.run("""
        UNWIND $rows AS row
        MERGE (e:EntityDef {entity_name: row.entity_name})
        SET e.database_table = row.database_table,
            e.sourceTable = row.sourceTable,
            e.condition = row.condition,
            e.businessLogic = row.businessLogic,
            e.layer = 'L2'
    """, {'rows': entity_rows})
    self.neo.run("""
        UNWIND $rows AS row
        MATCH (e:EntityDef {entity_name: row.entity_name})
        MATCH (t:TaxonomyDef {label: row.entity_name})
        MERGE (e)-[:BELONGS_TO_TAXONOMY]->(t)
    """, {'rows': entity_rows})

    rule_rows = [
        {'ruleType':'ProvenanceIntegrity','entity':'LeadSource','threshold':'0.60','notes':'LeadSource must link to at least one source/event'},
        {'ruleType':'IPIdentityIntegrity','entity':'IPIdentity','threshold':'0.50','notes':'IPIdentity must include a normalized IP string'},
        {'ruleType':'SignalQuality','entity':'Signal','threshold':'0.60','notes':'Signals require a type and evidence source'},
        {'ruleType':'IntentAggregation','entity':'Intent','threshold':'0.65','notes':'Intent should be backed by one or more signals'},
        {'ruleType':'TruthEvidence','entity':'TruthEngine','threshold':'0.70','notes':'TruthEngine must carry evidence count and confidence'},
        {'ruleType':'PredictiveIntegrity','entity':'PredictiveScore','threshold':'0.65','notes':'PredictiveScore must include scoreType and score'},
        {'ruleType':'DispatchReadiness','entity':'AgenticDispatcher','threshold':'0.55','notes':'Dispatcher must recommend a nextBestAction'},
        {'ruleType':'WaterfallCoverage','entity':'DataWaterfall','threshold':'0.55','notes':'Waterfall must include source depth/evidence count'},
        {'ruleType':'EscrowIntegrity','entity':'CreditEscrow','threshold':'0.55','notes':'Escrow must map to a deal or commercial amount'},
        {'ruleType':'IntelFindingQuality','entity':'IntelligenceFinding','threshold':'0.55','notes':'Finding should include type and confidence score'},
    ]
    self.neo.run("""
        UNWIND $rows AS row
        MERGE (r:RuleDef {ruleType: row.ruleType, entity: row.entity})
        SET r.autoMergeThreshold = row.threshold,
            r.hardEvidenceRequired = 'derived_or_backend',
            r.manualReviewTrigger = 'missing core evidence links',
            r.ttlDays = '365',
            r.decayModel = 'linear',
            r.notes = row.notes,
            r.systemCheckAutomatically = 'MATCH (n:' + row.entity + ') RETURN count(n) > 0 as ok',
            r.manualReviewCondition = 'Investigate low evidence coverage',
            r.layer = 'L3'
    """, {'rows': rule_rows})
    self.neo.run("""
        UNWIND $rows AS row
        MATCH (r:RuleDef {ruleType: row.ruleType, entity: row.entity})
        MATCH (e:EntityDef {entity_name: row.entity})
        MERGE (r)-[:VALIDATES_ENTITY]->(e)
    """, {'rows': rule_rows})

    rel_rows = [
        ('Lead','HAS_SOURCE','LeadSource'), ('RFQ','HAS_SOURCE','LeadSource'), ('Thread','HAS_SOURCE','LeadSource'),
        ('LeadSource','DERIVED_FROM','ExternalSource'), ('LeadSource','RECORDED_IN','DataSource'), ('LeadSource','HAS_EVENT','ProvenanceEvent'),
        ('ProvenanceEvent','OBSERVED_AT_IP','IPIdentity'), ('ProvenanceEvent','HAS_TOUCH','AttributionTouch'), ('AttributionTouch','INFLUENCES','Lead'),
        ('Signal','DERIVED_FROM','ProvenanceEvent'), ('Intent','BACKED_BY','Signal'), ('Lead','HAS_INTENT','Intent'), ('Organization','HAS_INTENT','Intent'),
        ('AnonymousVisitor','USES_FINGERPRINT','Fingerprint'), ('AnonymousVisitor','OBSERVED_AT_IP','IPIdentity'), ('GhostProfile','USES_FINGERPRINT','Fingerprint'),
        ('GhostProfile','OBSERVED_AT_IP','IPIdentity'), ('GhostProfile','SEEN_IN','AnonymousVisitor'), ('GhostProfile','MAY_RESOLVE_TO','Lead'),
        ('TruthEngine','EVALUATES','Lead'), ('TruthEngine','EVALUATES','Organization'), ('TruthEngine','USES_SOURCE','LeadSource'), ('TruthEngine','USES_SIGNAL','Signal'),
        ('PredictiveScore','SCORES','Lead'), ('PredictiveScore','SCORES','Deal'), ('PredictiveScore','SCORES','Account'),
        ('DataWaterfall','ENRICHES','Lead'), ('DataWaterfall','ENRICHES','Organization'), ('DataWaterfall','USES_SOURCE','LeadSource'),
        ('CreditEscrow','BACKS','Deal'), ('AgenticDispatcher','ROUTES','Lead'), ('AgenticDispatcher','ROUTES','Thread'), ('AgenticDispatcher','ASSIGNS_TO','Account'),
        ('IntelligenceFinding','TARGETS','Organization'), ('IntelligenceFinding','TARGETS','Product'), ('CompetitorIntel','ABOUT','Product'), ('CompetitorIntel','TARGETS','Organization'),
        ('BuyerCommittee','PART_OF','Organization'), ('BuyerCommittee','HAS_MEMBER','Account'), ('BuyerCommittee','INFLUENCES','Lead')
    ]
    rel_payload = [{'source':s,'type':t,'target':u,'logic':f'{s} {t} {u}'} for s,t,u in rel_rows]
    self.neo.run("""
        UNWIND $rows AS row
        MERGE (rt:RelationshipType {source: row.source, type: row.type, target: row.target})
        SET rt.businessLogic = row.logic
    """, {'rows': rel_payload})


def inject_lead_source_provenance_ext(self, run_governance: bool = False):
    print("→ Lead-source provenance")
    # Data source anchors
    ds_rows = [
        {'dataSourceId':'ds:baba_stagings:users','props':{'dbName':'baba_stagings','tableName':'users','source':'derived'}},
        {'dataSourceId':'ds:baba_stagings:sessions','props':{'dbName':'baba_stagings','tableName':'sessions','source':'derived'}},
        {'dataSourceId':'ds:crm:rfqs','props':{'dbName':'crm','tableName':'rfqs','source':'derived'}},
        {'dataSourceId':'ds:crm:crm_leads','props':{'dbName':'crm','tableName':'crm_leads','source':'derived'}},
        {'dataSourceId':'ds:crm:page_visits','props':{'dbName':'crm','tableName':'page_visits','source':'derived'}},
        {'dataSourceId':'ds:crm:lead_threads','props':{'dbName':'crm','tableName':'lead_threads','source':'derived'}},
        {'dataSourceId':'ds:crm:crm_meeting_informations','props':{'dbName':'crm','tableName':'crm_meeting_informations','source':'derived'}},
    ]
    self.neo.run("""
        UNWIND $rows AS row
        MERGE (d:DataSource {dataSourceId: row.dataSourceId})
        SET d += row.props
    """, {'rows': ds_rows})

    # IPIdentity from UI sessions and CRM page visits
    ips = {}
    if self.ui.table_exists('sessions'):
        try:
            rows = self.ui.q("SELECT id, user_id, ip_address, user_agent, last_activity FROM sessions WHERE ip_address IS NOT NULL AND ip_address <> ''")
            for r in rows:
                ip = str(r.get('ip_address')).strip()
                ip_id = stable_id('ip', ip)
                ips[ip_id] = {'ipId': ip_id, 'props': {'ipAddress': ip, 'source':'ui', 'lastSeenAt': safe_iso(r.get('last_activity'))}}
                if r.get('id') is not None:
                    self.neo.run("""
                        MERGE (i:IPIdentity {ipId:$ipId})
                        SET i += $props
                        WITH i
                        MATCH (s:Session {sessionId:$sessionId})
                        MERGE (s)-[:OBSERVED_AT_IP]->(i)
                    """, {'ipId': ip_id, 'props': ips[ip_id]['props'], 'sessionId': str(r.get('id'))})
        except Exception as e:
            print(f"⚠️ Provenance session IP scan failed: {e}")
    if self.crm and self.crm.table_exists('page_visits'):
        try:
            rows = self.crm.q("SELECT id, user_id, session_id, ip, page_url, page_name, page_event_ts, product_id, category_id FROM page_visits WHERE ip IS NOT NULL AND ip <> ''")
        except Exception:
            rows = []
        pv_source_rows, pv_event_rows, pv_touch_rows, pv_signal_rows, anon_rows, fp_rows = [], [], [], [], [], []
        for r in rows:
            ip = str(r.get('ip')).strip()
            ip_id = stable_id('ip', ip)
            ips[ip_id] = {'ipId': ip_id, 'props': {'ipAddress': ip, 'source':'crm', 'lastSeenAt': safe_iso(r.get('page_event_ts'))}}
            pvid = str(r.get('id'))
            sess_id = str(r.get('session_id')) if r.get('session_id') is not None else None
            ext_id = stable_id('external_source', 'page_visit', r.get('page_url') or r.get('page_name') or 'page_visit')
            source_id = stable_id('lead_source', 'page_visit', pvid)
            event_id = stable_id('provenance_event', 'page_visit', pvid)
            touch_id = stable_id('touch', 'page_visit', pvid)
            signal_id = stable_id('signal', 'page_view', pvid)
            ua = ''
            try:
                sess = self.neo.run("MATCH (s:Session {sessionId:$sid}) RETURN s.userAgent AS ua LIMIT 1", {'sid': sess_id}) if sess_id else []
                ua = (sess[0].get('ua') if sess else '') or ''
            except Exception:
                ua = ''
            fp_id = stable_id('fingerprint', ip, ua)
            fp_rows.append({'fingerprintId': fp_id, 'props': {'userAgent': ua, 'ipAddress': ip, 'source':'derived', 'confidence': 0.55}})
            pv_source_rows.append({'sourceId': source_id, 'props': {'sourceType':'page_visit', 'channel':'behavioral', 'sourceRef': pvid, 'source':'crm'}})
            pv_event_rows.append({'eventId': event_id, 'props': {'eventType':'page_view', 'eventTs': safe_iso(r.get('page_event_ts')), 'pageUrl': r.get('page_url'), 'pageName': r.get('page_name'), 'source':'crm'}})
            pv_touch_rows.append({'touchId': touch_id, 'props': {'channel':'web', 'touchType':'page_view', 'score': 1.0, 'source':'crm', 'eventTs': safe_iso(r.get('page_event_ts'))}})
            pv_signal_rows.append({'signalId': signal_id, 'props': {'signalType':'page_view', 'weight': 1.0, 'source':'crm', 'pageUrl': r.get('page_url'), 'eventTs': safe_iso(r.get('page_event_ts'))}})
            if sess_id:
                if not self.neo.run("MATCH (:Account)-[:HAS_SESSION]->(:Session {sessionId:$sid}) RETURN 1 LIMIT 1", {'sid': sess_id}):
                    visitor_id = stable_id('anon_visitor', sess_id)
                    anon_rows.append({'visitorId': visitor_id, 'sessionId': sess_id, 'ipId': ip_id, 'fingerprintId': fp_id, 'props': {'source':'derived', 'sessionId': sess_id, 'lastSeenAt': safe_iso(r.get('page_event_ts'))}})
        if ips:
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (i:IPIdentity {ipId: row.ipId})
                SET i += row.props
            """, {'rows': list(ips.values())})
        if fp_rows:
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (f:Fingerprint {fingerprintId: row.fingerprintId})
                SET f += row.props
            """, {'rows': fp_rows})
        if anon_rows:
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (v:AnonymousVisitor {visitorId: row.visitorId})
                SET v += row.props
                WITH row, v
                MATCH (s:Session {sessionId: row.sessionId})
                MATCH (i:IPIdentity {ipId: row.ipId})
                MATCH (f:Fingerprint {fingerprintId: row.fingerprintId})
                MERGE (v)-[:SEEN_IN]->(s)
                MERGE (v)-[:OBSERVED_AT_IP]->(i)
                MERGE (v)-[:USES_FINGERPRINT]->(f)
            """, {'rows': anon_rows})
            ghost_rows = []
            for row in anon_rows:
                ghost_rows.append({'profileId': stable_id('ghost_profile', row['visitorId']), 'visitorId': row['visitorId'], 'ipId': row['ipId'], 'fingerprintId': row['fingerprintId'], 'props': {'source':'derived', 'confidence':0.55}})
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (g:GhostProfile {profileId: row.profileId})
                SET g += row.props
                WITH row, g
                MATCH (v:AnonymousVisitor {visitorId: row.visitorId})
                MATCH (i:IPIdentity {ipId: row.ipId})
                MATCH (f:Fingerprint {fingerprintId: row.fingerprintId})
                MERGE (g)-[:SEEN_IN]->(v)
                MERGE (g)-[:OBSERVED_AT_IP]->(i)
                MERGE (g)-[:USES_FINGERPRINT]->(f)
            """, {'rows': ghost_rows})
        if pv_source_rows:
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (ls:LeadSource {sourceId: row.sourceId})
                SET ls += row.props
            """, {'rows': pv_source_rows})
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (es:ExternalSource {externalSourceId: 'external:page_visit'})
                SET es.name = 'page_visit', es.sourceKind = 'behavioral', es.source='derived'
                WITH row, es
                MATCH (ls:LeadSource {sourceId: row.sourceId})
                MATCH (ds:DataSource {dataSourceId: 'ds:crm:page_visits'})
                MERGE (ls)-[:DERIVED_FROM]->(es)
                MERGE (ls)-[:RECORDED_IN]->(ds)
            """, {'rows': pv_source_rows})
        if pv_event_rows:
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (e:ProvenanceEvent {eventId: row.eventId})
                SET e += row.props
            """, {'rows': pv_event_rows})
        if pv_touch_rows:
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (t:AttributionTouch {touchId: row.touchId})
                SET t += row.props
            """, {'rows': pv_touch_rows})
        if pv_signal_rows:
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (s:Signal {signalId: row.signalId})
                SET s += row.props
            """, {'rows': pv_signal_rows})
        join_rows = []
        for r in rows:
            pvid = str(r.get('id'))
            source_id = stable_id('lead_source', 'page_visit', pvid)
            event_id = stable_id('provenance_event', 'page_visit', pvid)
            touch_id = stable_id('touch', 'page_visit', pvid)
            signal_id = stable_id('signal', 'page_view', pvid)
            ip_id = stable_id('ip', str(r.get('ip')).strip())
            join_rows.append({'pageViewId': pvid, 'sourceId': source_id, 'eventId': event_id, 'touchId': touch_id, 'signalId': signal_id, 'ipId': ip_id})
        if join_rows:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (pv:PageView {pageViewId: row.pageViewId})
                MATCH (ls:LeadSource {sourceId: row.sourceId})
                MATCH (e:ProvenanceEvent {eventId: row.eventId})
                MATCH (t:AttributionTouch {touchId: row.touchId})
                MATCH (s:Signal {signalId: row.signalId})
                MATCH (ip:IPIdentity {ipId: row.ipId})
                MERGE (pv)-[:HAS_SOURCE]->(ls)
                MERGE (ls)-[:HAS_EVENT]->(e)
                MERGE (e)-[:HAS_TOUCH]->(t)
                MERGE (s)-[:DERIVED_FROM]->(e)
                MERGE (e)-[:OBSERVED_AT_IP]->(ip)
            """, {'rows': join_rows})

    # RFQ provenance
    if self.crm and self.crm.table_exists('rfqs'):
        try:
            rows = self.crm.q("SELECT id, user_id, source_type, title, created_at, category FROM rfqs")
        except Exception:
            rows = []
        src_rows, ext_rows, evt_rows, touch_rows, join_rows = [], {}, [], [], []
        for r in rows:
            rfq_id = str(r.get('id'))
            source_type = str(r.get('source_type') or 'rfq').strip().lower()
            source_id = stable_id('lead_source', 'rfq', rfq_id)
            ext_id = stable_id('external_source', 'rfq', source_type)
            evt_id = stable_id('provenance_event', 'rfq', rfq_id)
            touch_id = stable_id('touch', 'rfq', rfq_id)
            src_rows.append({'sourceId': source_id, 'props': {'sourceType':'rfq', 'channel': source_type, 'sourceRef': rfq_id, 'title': r.get('title'), 'source':'crm'}})
            ext_rows[ext_id] = {'externalSourceId': ext_id, 'props': {'name': source_type, 'sourceKind': 'rfq_channel', 'source':'derived'}}
            evt_rows.append({'eventId': evt_id, 'props': {'eventType':'rfq_created', 'eventTs': safe_iso(r.get('created_at')), 'title': r.get('title'), 'category': r.get('category'), 'source':'crm'}})
            touch_rows.append({'touchId': touch_id, 'props': {'channel': source_type, 'touchType':'rfq', 'score': 3.0, 'source':'crm', 'eventTs': safe_iso(r.get('created_at'))}})
            join_rows.append({'rfqId': rfq_id, 'sourceId': source_id, 'extId': ext_id, 'eventId': evt_id, 'touchId': touch_id, 'accountId': str(r.get('user_id')) if r.get('user_id') is not None else None})
        if src_rows:
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (ls:LeadSource {sourceId: row.sourceId})
                SET ls += row.props
            """, {'rows': src_rows})
        if ext_rows:
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (es:ExternalSource {externalSourceId: row.externalSourceId})
                SET es += row.props
            """, {'rows': list(ext_rows.values())})
        if evt_rows:
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (e:ProvenanceEvent {eventId: row.eventId})
                SET e += row.props
            """, {'rows': evt_rows})
        if touch_rows:
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (t:AttributionTouch {touchId: row.touchId})
                SET t += row.props
            """, {'rows': touch_rows})
        if join_rows:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (rfq:RFQ {rfqId: row.rfqId})
                MATCH (ls:LeadSource {sourceId: row.sourceId})
                MATCH (es:ExternalSource {externalSourceId: row.extId})
                MATCH (e:ProvenanceEvent {eventId: row.eventId})
                MATCH (t:AttributionTouch {touchId: row.touchId})
                MATCH (ds:DataSource {dataSourceId: 'ds:crm:rfqs'})
                MERGE (rfq)-[:HAS_SOURCE]->(ls)
                MERGE (ls)-[:DERIVED_FROM]->(es)
                MERGE (ls)-[:RECORDED_IN]->(ds)
                MERGE (ls)-[:HAS_EVENT]->(e)
                MERGE (e)-[:HAS_TOUCH]->(t)
                FOREACH (_ IN CASE WHEN row.accountId IS NULL THEN [] ELSE [1] END |
                    MERGE (touchAccountRef:AttributionTouch {touchId: row.touchId})
                )
            """, {'rows': join_rows})
            self.neo.run("""
                UNWIND $rows AS row
                OPTIONAL MATCH (a:Account {accountId: row.accountId})
                OPTIONAL MATCH (rfq:RFQ {rfqId: row.rfqId})
                OPTIONAL MATCH (t:AttributionTouch {touchId: row.touchId})
                WITH row, a, rfq, t
                WHERE row.accountId IS NOT NULL AND a IS NOT NULL AND rfq IS NOT NULL AND t IS NOT NULL
                MERGE (t)-[:INFLUENCES]->(rfq)
                MERGE (a)-[:GENERATED]->(t)
            """, {'rows': join_rows})

    # Lead provenance based on crm_leads + inferred RFQ joins
    if self.crm and self.crm.table_exists('crm_leads'):
        try:
            rows = self.crm.q("SELECT id, lead_id, buyer_user_id, seller_user_id, created_at, category_id, product_id, message FROM crm_leads")
        except Exception:
            rows = []
        src_rows, evt_rows, touch_rows, signal_rows, join_rows = [], [], [], [], []
        for r in rows:
            lead_id = str(r.get('id'))
            source_id = stable_id('lead_source', 'lead', lead_id)
            evt_id = stable_id('provenance_event', 'lead', lead_id)
            touch_id = stable_id('touch', 'lead', lead_id)
            signal_id = stable_id('signal', 'lead_created', lead_id)
            src_rows.append({'sourceId': source_id, 'props': {'sourceType':'crm_lead', 'channel':'crm', 'sourceRef': lead_id, 'source':'crm'}})
            evt_rows.append({'eventId': evt_id, 'props': {'eventType':'lead_created', 'eventTs': safe_iso(r.get('created_at')), 'message': r.get('message'), 'source':'crm'}})
            touch_rows.append({'touchId': touch_id, 'props': {'channel':'crm', 'touchType':'lead_created', 'score': 4.0, 'source':'crm', 'eventTs': safe_iso(r.get('created_at'))}})
            signal_rows.append({'signalId': signal_id, 'props': {'signalType':'lead_created', 'weight': 4.0, 'source':'crm', 'eventTs': safe_iso(r.get('created_at')), 'categoryId': r.get('category_id'), 'productId': r.get('product_id')}})
            join_rows.append({'leadId': lead_id, 'sourceId': source_id, 'eventId': evt_id, 'touchId': touch_id, 'signalId': signal_id, 'buyerId': str(r.get('buyer_user_id')) if r.get('buyer_user_id') is not None else None, 'sellerId': str(r.get('seller_user_id')) if r.get('seller_user_id') is not None else None, 'leadKey': str(r.get('lead_id')) if r.get('lead_id') is not None else None})
        if src_rows:
            self.neo.run("UNWIND $rows AS row MERGE (ls:LeadSource {sourceId: row.sourceId}) SET ls += row.props", {'rows': src_rows})
            self.neo.run("""
                UNWIND $rows AS row
                MERGE (es:ExternalSource {externalSourceId:'external:crm_lead'})
                SET es.name='crm_lead', es.sourceKind='crm', es.source='derived'
                WITH row, es
                MATCH (ls:LeadSource {sourceId: row.sourceId})
                MATCH (ds:DataSource {dataSourceId: 'ds:crm:crm_leads'})
                MERGE (ls)-[:DERIVED_FROM]->(es)
                MERGE (ls)-[:RECORDED_IN]->(ds)
            """, {'rows': src_rows})
        if evt_rows:
            self.neo.run("UNWIND $rows AS row MERGE (e:ProvenanceEvent {eventId: row.eventId}) SET e += row.props", {'rows': evt_rows})
        if touch_rows:
            self.neo.run("UNWIND $rows AS row MERGE (t:AttributionTouch {touchId: row.touchId}) SET t += row.props", {'rows': touch_rows})
        if signal_rows:
            self.neo.run("UNWIND $rows AS row MERGE (s:Signal {signalId: row.signalId}) SET s += row.props", {'rows': signal_rows})
        if join_rows:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (l:Lead {leadId: row.leadId})
                MATCH (ls:LeadSource {sourceId: row.sourceId})
                MATCH (e:ProvenanceEvent {eventId: row.eventId})
                MATCH (t:AttributionTouch {touchId: row.touchId})
                MATCH (s:Signal {signalId: row.signalId})
                MERGE (l)-[:HAS_SOURCE]->(ls)
                MERGE (ls)-[:HAS_EVENT]->(e)
                MERGE (e)-[:HAS_TOUCH]->(t)
                MERGE (s)-[:DERIVED_FROM]->(e)
                MERGE (t)-[:INFLUENCES]->(l)
            """, {'rows': join_rows})
            self.neo.run("""
                UNWIND $rows AS row
                OPTIONAL MATCH (a:Account {accountId: row.buyerId})
                OPTIONAL MATCH (l:Lead {leadId: row.leadId})
                OPTIONAL MATCH (t:AttributionTouch {touchId: row.touchId})
                WITH row, a, l, t
                WHERE row.buyerId IS NOT NULL AND a IS NOT NULL AND l IS NOT NULL AND t IS NOT NULL
                MERGE (a)-[:GENERATED]->(t)
                MERGE (intent:Intent {intentId: 'intent:buyer:' + row.leadId})
                ON CREATE SET intent.intentType = 'buyer_interest', intent.source = 'derived'
                MERGE (a)-[:HAS_INTENT]->(intent)
                MERGE (intent)-[:TARGETS]->(l)
            """, {'rows': join_rows})
            # connect inferred RFQ source when leadKey matches RFQ
            self.neo.run("""
                UNWIND $rows AS row
                OPTIONAL MATCH (l:Lead {leadId: row.leadId})
                OPTIONAL MATCH (r:RFQ {rfqId: row.leadKey})
                WITH row, l, r
                WHERE row.leadKey IS NOT NULL AND l IS NOT NULL AND r IS NOT NULL
                MERGE (r)-[:INFLUENCES]->(l)
            """, {'rows': join_rows})

    # Thread provenance
    if self.crm and self.crm.table_exists('lead_threads'):
        try:
            rows = self.crm.q("SELECT id, user_id, thread_name, created_at, updated_at FROM lead_threads")
        except Exception:
            rows = []
        src_rows, evt_rows, sig_rows, joins = [], [], [], []
        for r in rows:
            tid = str(r.get('id'))
            source_id = stable_id('lead_source', 'thread', tid)
            event_id = stable_id('provenance_event', 'thread', tid)
            signal_id = stable_id('signal', 'thread_created', tid)
            src_rows.append({'sourceId': source_id, 'props': {'sourceType':'thread', 'channel':'conversation', 'sourceRef': tid, 'source':'crm'}})
            evt_rows.append({'eventId': event_id, 'props': {'eventType':'thread_created', 'eventTs': safe_iso(r.get('created_at')), 'title': r.get('thread_name'), 'source':'crm'}})
            sig_rows.append({'signalId': signal_id, 'props': {'signalType':'thread_created', 'weight': 2.0, 'source':'crm', 'eventTs': safe_iso(r.get('created_at'))}})
            joins.append({'threadId': tid, 'sourceId': source_id, 'eventId': event_id, 'signalId': signal_id})
        if src_rows:
            self.neo.run("UNWIND $rows AS row MERGE (ls:LeadSource {sourceId: row.sourceId}) SET ls += row.props", {'rows': src_rows})
        if evt_rows:
            self.neo.run("UNWIND $rows AS row MERGE (e:ProvenanceEvent {eventId: row.eventId}) SET e += row.props", {'rows': evt_rows})
        if sig_rows:
            self.neo.run("UNWIND $rows AS row MERGE (s:Signal {signalId: row.signalId}) SET s += row.props", {'rows': sig_rows})
        if joins:
            self.neo.run("""
                UNWIND $rows AS row
                MATCH (t:Thread {threadId: row.threadId})
                MATCH (ls:LeadSource {sourceId: row.sourceId})
                MATCH (e:ProvenanceEvent {eventId: row.eventId})
                MATCH (s:Signal {signalId: row.signalId})
                MERGE (t)-[:HAS_SOURCE]->(ls)
                MERGE (ls)-[:HAS_EVENT]->(e)
                MERGE (s)-[:DERIVED_FROM]->(e)
            """, {'rows': joins})


def inject_advanced_intelligence_pipelines_ext(self, run_governance: bool = False):
    print("→ Advanced M→AC intelligence / truth / monetization / agentic / predictive")
    # Intent from account-level signals
    self.neo.run("""
        MATCH (a:Account)
        OPTIONAL MATCH (a)-[:GENERATED]->(pv:PageView)
        OPTIONAL MATCH (a)-[:CREATES]->(r:RFQ)
        OPTIONAL MATCH (a)-[:OWNS_DEAL]->(d:Deal)
        WITH a, count(DISTINCT pv) AS pageViews, count(DISTINCT r) AS rfqs, count(DISTINCT d) AS deals
        WHERE pageViews > 0 OR rfqs > 0 OR deals > 0
        MERGE (i:Intent {intentId: 'intent:account:' + a.accountId})
        SET i.intentType = CASE WHEN rfqs > 0 OR deals > 0 THEN 'commercial_buying' ELSE 'behavioral_interest' END,
            i.score = toFloat(pageViews) + (toFloat(rfqs) * 5.0) + (toFloat(deals) * 8.0),
            i.pageViews = pageViews,
            i.rfqCount = rfqs,
            i.dealCount = deals,
            i.source = 'derived'
        MERGE (a)-[:HAS_INTENT]->(i)
    """)
    self.neo.run("""
        MATCH (a:Account)-[:HAS_INTENT]->(i:Intent)
        OPTIONAL MATCH (a)-[:GENERATED]->(pv:PageView)
        OPTIONAL MATCH (a)-[:CREATES]->(r:RFQ)
        OPTIONAL MATCH (a)-[:OWNS_DEAL]->(d:Deal)
        FOREACH (_ IN CASE WHEN pv IS NULL THEN [] ELSE [1] END |
            MERGE (s:Signal {signalId: 'signal:pv-intent:' + pv.pageViewId})
            ON CREATE SET s.signalType='page_view', s.weight=1.0, s.source='derived'
            MERGE (i)-[:BACKED_BY]->(s)
        )
        FOREACH (_ IN CASE WHEN r IS NULL THEN [] ELSE [1] END |
            MERGE (s2:Signal {signalId: 'signal:rfq-intent:' + r.rfqId})
            ON CREATE SET s2.signalType='rfq_created', s2.weight=5.0, s2.source='derived'
            MERGE (i)-[:BACKED_BY]->(s2)
        )
        FOREACH (_ IN CASE WHEN d IS NULL THEN [] ELSE [1] END |
            MERGE (s3:Signal {signalId: 'signal:deal-intent:' + d.dealId})
            ON CREATE SET s3.signalType='deal_progress', s3.weight=8.0, s3.source='derived'
            MERGE (i)-[:BACKED_BY]->(s3)
        )
    """)

    # Lead-level predictive scores and truth engine
    self.neo.run("""
        MATCH (l:Lead)
        OPTIONAL MATCH (l)-[:HAS_RFQ]->(r:RFQ)
        OPTIONAL MATCH (l)-[:HAS_DEAL]->(d:Deal)
        OPTIONAL MATCH (l)-[:HAS_THREAD]->(th:Thread)
        OPTIONAL MATCH (l)-[:HAS_SOURCE]->(ls:LeadSource)
        OPTIONAL MATCH (l)<-[:INFLUENCES]-(touch:AttributionTouch)
        WITH l,
             count(DISTINCT r) AS rfqs,
             count(DISTINCT d) AS deals,
             count(DISTINCT th) AS threads,
             count(DISTINCT ls) AS sources,
             count(DISTINCT touch) AS touches
        MERGE (p:PredictiveScore {predictionId: 'prediction:lead:' + l.leadId})
        SET p.scoreType = 'lead_conversion_probability',
            p.score = (rfqs * 20.0) + (deals * 35.0) + (threads * 8.0) + (sources * 5.0) + (touches * 4.0),
            p.rfqCount = rfqs,
            p.dealCount = deals,
            p.threadCount = threads,
            p.sourceCount = sources,
            p.touchCount = touches,
            p.source = 'derived'
        MERGE (p)-[:SCORES]->(l)
        MERGE (t:TruthEngine {truthId: 'truth:lead:' + l.leadId})
        SET t.confidence = CASE WHEN (sources + rfqs + deals + touches) = 0 THEN 0.0 ELSE toFloat(sources + rfqs + deals + touches) / 10.0 END,
            t.evidenceCount = sources + rfqs + deals + touches,
            t.threadCount = threads,
            t.source = 'derived'
        MERGE (t)-[:EVALUATES]->(l)
    """)
    self.neo.run("""
        MATCH (l:Lead)<-[:EVALUATES]-(t:TruthEngine)
        OPTIONAL MATCH (l)-[:HAS_SOURCE]->(ls:LeadSource)
        OPTIONAL MATCH (l)<-[:INFLUENCES]-(touch:AttributionTouch)
        OPTIONAL MATCH (intent:Intent)<-[:HAS_INTENT]-(:Account)-[:BUYER_ACCOUNT|SELLER_ACCOUNT]-(l)
        FOREACH (_ IN CASE WHEN ls IS NULL THEN [] ELSE [1] END | MERGE (t)-[:USES_SOURCE]->(ls))
        FOREACH (_ IN CASE WHEN touch IS NULL THEN [] ELSE [1] END |
            MERGE (s:Signal {signalId:'signal:touch:' + touch.touchId})
            ON CREATE SET s.signalType='touch', s.weight=coalesce(touch.score,1.0), s.source='derived'
            MERGE (s)-[:DERIVED_FROM]->(:ProvenanceEvent {eventId:'evt:touch:' + touch.touchId})
            MERGE (t)-[:USES_SIGNAL]->(s)
        )
        FOREACH (_ IN CASE WHEN intent IS NULL THEN [] ELSE [1] END |
            MERGE (l)-[:HAS_INTENT]->(intent)
        )
    """)

    # Organization truth and competitor/trade intelligence
    self.neo.run("""
        MATCH (o:Organization)
        OPTIONAL MATCH (o)-[:SELLS]->(p:Product)
        OPTIONAL MATCH (o)-[:HAS_FACILITY]->(f:Facility)
        OPTIONAL MATCH (o)<-[:SUPPLIED_BY]-(prod:Product)
        OPTIONAL MATCH (o)<-[:TARGETS_SELLER]-(pv:PageView)
        WITH o, count(DISTINCT p) AS sells, count(DISTINCT f) AS facilities, count(DISTINCT prod) AS suppliedProducts, count(DISTINCT pv) AS targetPageViews
        MERGE (t:TruthEngine {truthId: 'truth:org:' + o.orgId})
        SET t.confidence = toFloat(sells + facilities + suppliedProducts + targetPageViews) / 10.0,
            t.evidenceCount = sells + facilities + suppliedProducts + targetPageViews,
            t.source = 'derived'
        MERGE (t)-[:EVALUATES]->(o)
        MERGE (w:DataWaterfall {waterfallId: 'waterfall:org:' + o.orgId})
        SET w.sourceDepth = sells + facilities + suppliedProducts,
            w.evidenceCount = sells + facilities + suppliedProducts + targetPageViews,
            w.source = 'derived'
        MERGE (w)-[:ENRICHES]->(o)
    """)
    self.neo.run("""
        MATCH (o:Organization)<-[:SELLS]-(ownerOrg:Organization)-[:OWNS]->(:Store)
        WITH DISTINCT ownerOrg
        OPTIONAL MATCH (ownerOrg)-[:SELLS]->(p:Product)-[:HAS_BRAND]->(b:Brand)
        WITH ownerOrg, collect(DISTINCT coalesce(b.name, b.slug, b.brand_name, b.brandId)) AS brandNames
        WITH ownerOrg, [x IN brandNames WHERE x IS NOT NULL] AS brandNames
        FOREACH (brandName IN CASE WHEN size(brandNames)=0 THEN [] ELSE brandNames END |
            MERGE (ci:CompetitorIntel {competitorIntelId: 'competitor:' + ownerOrg.orgId + ':' + replace(toLower(brandName),' ','_')})
            SET ci.competitorName = brandName, ci.evidenceScore = size(brandNames), ci.source='derived'
            MERGE (ci)-[:TARGETS]->(ownerOrg)
        )
    """)
    self.neo.run("""
        MATCH (pv:PageView)-[:TARGETS]->(p:Product)
        OPTIONAL MATCH (pv)-[:TARGETS_SELLER]->(o:Organization)
        WITH pv, p, o
        WHERE o IS NOT NULL
        MERGE (f:IntelligenceFinding {findingId: 'finding:pageview:' + pv.pageViewId})
        SET f.findingType = 'product_interest',
            f.score = coalesce(pv.timeSpent, 0) + 1,
            f.source='derived'
        MERGE (f)-[:TARGETS]->(p)
        MERGE (f)-[:TARGETS]->(o)
    """)

    # Buyer committee inference from threads + meetings + buyer accounts
    self.neo.run("""
        MATCH (l:Lead)-[:BUYER_ACCOUNT]->(a:Account)
        OPTIONAL MATCH (l)-[:HAS_THREAD]->(t:Thread)
        OPTIONAL MATCH (l)-[:HAS_MEETING]->(m:Meeting)
        WITH l, collect(DISTINCT a) AS buyers, count(DISTINCT t) AS threads, count(DISTINCT m) AS meetings
        WHERE size(buyers) > 0 AND (threads > 0 OR meetings > 0)
        MERGE (bc:BuyerCommittee {committeeId: 'committee:' + l.leadId})
        SET bc.size = size(buyers),
            bc.threadCount = threads,
            bc.meetingCount = meetings,
            bc.confidence = toFloat(size(buyers) + threads + meetings) / 10.0,
            bc.source='derived'
        MERGE (bc)-[:INFLUENCES]->(l)
        FOREACH (buyer IN buyers | MERGE (bc)-[:HAS_MEMBER]->(buyer))
    """)
    self.neo.run("""
        MATCH (bc:BuyerCommittee)-[:HAS_MEMBER]->(:Account)-[:MEMBER_OF|OWNS]->(o:Organization)
        WITH bc, head(collect(DISTINCT o)) AS org
        FOREACH (_ IN CASE WHEN org IS NULL THEN [] ELSE [1] END |
            MERGE (bc)-[:PART_OF]->(org)
        )
    """)

    # Data waterfall and predictive on organizations and deals
    self.neo.run("""
        MATCH (d:Deal)
        OPTIONAL MATCH (d)-[:IN_STAGE|AT_STAGE]->(s:PipelineStage)
        OPTIONAL MATCH (d)<-[:HAS_DEAL]-(l:Lead)
        OPTIONAL MATCH (d)-[:HAS_LEG]->(leg:DealLeg)
        WITH d, count(DISTINCT s) AS stages, count(DISTINCT l) AS leads, count(DISTINCT leg) AS legs, coalesce(toFloat(d.amount),0.0) AS amount
        MERGE (p:PredictiveScore {predictionId:'prediction:deal:' + d.dealId})
        SET p.scoreType='deal_win_probability',
            p.score=(stages * 10.0) + (leads * 20.0) + (legs * 4.0) + CASE WHEN amount > 0 THEN 10.0 ELSE 0.0 END,
            p.source='derived'
        MERGE (p)-[:SCORES]->(d)
        MERGE (e:CreditEscrow {escrowId:'escrow:' + d.dealId})
        SET e.amount = amount,
            e.status = coalesce(d.status, 'unknown'),
            e.source='derived'
        MERGE (e)-[:BACKS]->(d)
    """)
    self.neo.run("""
        MATCH (l:Lead)
        OPTIONAL MATCH (l)-[:HAS_SOURCE]->(ls:LeadSource)
        OPTIONAL MATCH (l)-[:HAS_RFQ]->(r:RFQ)
        OPTIONAL MATCH (l)-[:HAS_DEAL]->(d:Deal)
        OPTIONAL MATCH (l)-[:HAS_THREAD]->(th:Thread)
        WITH l, count(DISTINCT ls) AS sourceCount, count(DISTINCT r) AS rfqCount, count(DISTINCT d) AS dealCount, count(DISTINCT th) AS threadCount
        MERGE (w:DataWaterfall {waterfallId:'waterfall:lead:' + l.leadId})
        SET w.sourceDepth = sourceCount,
            w.evidenceCount = sourceCount + rfqCount + dealCount + threadCount,
            w.rfqCount = rfqCount,
            w.dealCount = dealCount,
            w.threadCount = threadCount,
            w.source='derived'
        MERGE (w)-[:ENRICHES]->(l)
    """)

    # Agentic dispatcher from leads and threads
    self.neo.run("""
        MATCH (w:DataWaterfall)-[:ENRICHES]->(l:Lead)
        MATCH (l)-[:HAS_SOURCE]->(ls:LeadSource)
        MERGE (w)-[:USES_SOURCE]->(ls)
    """)

    self.neo.run("""
        MATCH (l:Lead)
        OPTIONAL MATCH (l)-[:HAS_DEAL]->(d:Deal)
        OPTIONAL MATCH (l)-[:HAS_THREAD]->(t:Thread)
        OPTIONAL MATCH (l)-[:HAS_TASK]->(task:Task)
        OPTIONAL MATCH (l)-[:BUYER_ACCOUNT]->(buyer:Account)
        OPTIONAL MATCH (l)-[:SELLER_ACCOUNT]->(seller:Account)
        WITH l, d, t, count(DISTINCT task) AS tasks,
             head(collect(DISTINCT buyer)) AS buyer,
             head(collect(DISTINCT seller)) AS seller
        MERGE (ad:AgenticDispatcher {dispatcherId:'dispatch:lead:' + l.leadId})
        SET ad.priority = CASE WHEN d IS NOT NULL THEN 'high' WHEN t IS NOT NULL THEN 'medium' ELSE 'normal' END,
            ad.nextBestAction = CASE WHEN d IS NOT NULL AND tasks = 0 THEN 'create_followup_task'
                                     WHEN t IS NOT NULL THEN 'continue_thread_nurture'
                                     WHEN buyer IS NOT NULL THEN 'assign_to_seller'
                                     ELSE 'research_lead' END,
            ad.source='derived'
        MERGE (ad)-[:ROUTES]->(l)
        FOREACH (_ IN CASE WHEN seller IS NULL THEN [] ELSE [1] END |
            MERGE (ad)-[:ASSIGNS_TO]->(seller)
        )
    """)
    self.neo.run("""
        MATCH (t:Thread)
        OPTIONAL MATCH (t)<-[:HAS_THREAD]-(l:Lead)
        WITH t, head(collect(DISTINCT l)) AS lead
        MERGE (ad:AgenticDispatcher {dispatcherId:'dispatch:thread:' + t.threadId})
        SET ad.priority='medium', ad.nextBestAction=CASE WHEN lead IS NULL THEN 'triage_thread' ELSE 'link_thread_to_lead_workflow' END, ad.source='derived'
        MERGE (ad)-[:ROUTES]->(t)
    """)

    # Link GhostProfiles to possible leads via shared IP/session touch overlap
    self.neo.run("""
        MATCH (g:GhostProfile)-[:SEEN_IN]->(:AnonymousVisitor)-[:SEEN_IN]->(s:Session)
        MATCH (s)-[:HAS_PAGEVIEW]->(pv:PageView)
        MATCH (pv)-[:HAS_SOURCE]->(:LeadSource)<-[:HAS_SOURCE]-(l:Lead)
        MERGE (g)-[:MAY_RESOLVE_TO]->(l)
    """)


def patched_run_all_full_gi_mac(self, do_ui=True, do_crm=True, classify=False, xlsx_entity=None, xlsx_relationships=None, xlsx_taxonomy=None, xlsx_conditional=None, run_governance=True):
    print("\n" + "="*60)
    print("UI/CRM INJECTOR V18 - FULL GOVERNANCE + G/I + PROVENANCE + M→AC")
    print("="*60 + "\n")
    self.upsert_meta_from_xlsx(entity_xlsx=xlsx_entity, rel_xlsx=xlsx_relationships, taxonomy_xlsx=xlsx_taxonomy, conditional_xlsx=xlsx_conditional)
    self.upsert_hat_rbac_governance_metadata()
    self.upsert_provenance_and_advanced_governance_metadata()
    self.neo.create_runtime_constraints()
    if do_ui:
        print("\n---- Injecting UI Data ----")
        self.inject_users_persons_accounts_orgs(run_governance=False)
        self.inject_categories()
        self.inject_brands()
        self.inject_units(run_governance=False)
        self.inject_products(run_governance=False)
        self.inject_product_applications(run_governance=False)
        self.inject_use_cases_and_keywords(run_governance=False)
        self.inject_master_keywords()
        self.inject_facilities()
        self.inject_feature_packages()
        print("\n---- Injecting G/I Graph Layers ----")
        self.inject_hat_system(run_governance=False)
        self.inject_rbac_abac(run_governance=False)
    if do_crm:
        print("\n---- Injecting CRM Data ----")
        self.inject_crm_pipeline()
        self.inject_crm_core()
        self.inject_crm_activity()
    print("\n---- Injecting Provenance + Advanced Intelligence ----")
    self.inject_lead_source_provenance(run_governance=False)
    self.inject_advanced_intelligence_pipelines(run_governance=False)
    if run_governance and self.config.STORE_VALIDATION_RESULTS:
        print("\n---- Running Full Governance Validation ----")
        self.run_governance_validation_pass()
    if classify:
        print("\n---- Applying Taxonomy Classification ----")
        self.apply_taxonomy_classification()
    print("\n" + "="*60)
    print("INJECTION COMPLETE (V18 FULL GOVERNANCE + G/I + PROVENANCE + M→AC)")
    print("="*60)


InjectorV5.upsert_provenance_and_advanced_governance_metadata = upsert_provenance_and_advanced_governance_metadata
InjectorV5.inject_lead_source_provenance = inject_lead_source_provenance_ext
InjectorV5.inject_advanced_intelligence_pipelines = inject_advanced_intelligence_pipelines_ext
InjectorV5.run_all = patched_run_all_full_gi_mac


if __name__ == "__main__":
    main()
