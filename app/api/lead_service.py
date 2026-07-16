"""
LeadService: Enriches SwitchLead nodes from Neo4j with ZoomInfo decision-maker
contacts fetched directly from Postgres. Returns structured dicts ready for the API.
"""

import re
from datetime import datetime, timezone
from app.core.logger import info, warn

# ── HS code chapter descriptions ─────────────────────────────────────────────
HS_CHAPTERS = {
    '01': 'Live animals', '02': 'Meat & edible offal', '03': 'Fish & seafood',
    '04': 'Dairy & eggs', '05': 'Animal products', '06': 'Live plants & flowers',
    '07': 'Edible vegetables', '08': 'Edible fruits & nuts', '09': 'Coffee, tea & spices',
    '10': 'Cereals', '11': 'Milling products', '12': 'Oil seeds', '13': 'Lac & gums',
    '14': 'Vegetable plaiting materials', '15': 'Animal/vegetable fats & oils',
    '16': 'Preparations of meat/fish', '17': 'Sugars & confectionery',
    '18': 'Cocoa & preparations', '19': 'Cereal/flour/starch preparations',
    '20': 'Preparations of vegetables/fruits', '21': 'Miscellaneous food preparations',
    '22': 'Beverages, spirits & vinegar', '23': 'Residues & food industry waste',
    '24': 'Tobacco', '25': 'Salt, sulphur, earths & stone', '26': 'Ores & slag',
    '27': 'Mineral fuels & oils', '28': 'Inorganic chemicals',
    '29': 'Organic chemicals', '30': 'Pharmaceutical products',
    '31': 'Fertilisers', '32': 'Tanning & dyeing extracts', '33': 'Essential oils & cosmetics',
    '34': 'Soap & waxes', '35': 'Albuminoidal substances & enzymes',
    '36': 'Explosives & pyrotechnics', '37': 'Photographic goods',
    '38': 'Miscellaneous chemical products', '39': 'Plastics',
    '40': 'Rubber', '41': 'Raw hides & skins', '42': 'Leather articles',
    '43': 'Furskins', '44': 'Wood & wood products', '45': 'Cork',
    '46': 'Straw & basketware', '47': 'Pulp of wood', '48': 'Paper & paperboard',
    '49': 'Printed books & newspapers', '50': 'Silk', '51': 'Wool',
    '52': 'Cotton', '53': 'Other vegetable textile fibres', '54': 'Man-made filaments',
    '55': 'Man-made staple fibres', '56': 'Wadding & felt', '57': 'Carpets',
    '58': 'Special woven fabrics', '59': 'Impregnated textile fabrics',
    '60': 'Knitted fabrics', '61': 'Knitted apparel', '62': 'Woven apparel',
    '63': 'Other textile articles', '64': 'Footwear', '65': 'Headgear',
    '66': 'Umbrellas', '67': 'Feathers & artificial flowers',
    '68': 'Stone & cement articles', '69': 'Ceramic products', '70': 'Glass',
    '71': 'Precious stones & metals', '72': 'Iron & steel', '73': 'Iron/steel articles',
    '74': 'Copper', '75': 'Nickel', '76': 'Aluminium', '78': 'Lead',
    '79': 'Zinc', '80': 'Tin', '81': 'Other base metals', '82': 'Tools & cutlery',
    '83': 'Miscellaneous metal articles', '84': 'Machinery & mechanical appliances',
    '85': 'Electrical machinery & equipment', '86': 'Railway equipment',
    '87': 'Vehicles', '88': 'Aircraft', '89': 'Ships & boats',
    '90': 'Optical & medical instruments', '91': 'Clocks & watches',
    '92': 'Musical instruments', '93': 'Arms & ammunition',
    '94': 'Furniture & bedding', '95': 'Toys & games', '96': 'Miscellaneous manufactures',
    '97': 'Works of art',
}

# Decision-maker title keywords (procurement/supply chain focus)
DM_KEYWORDS = [
    'procurement', 'purchasing', 'supply chain', 'sourcing', 'supplier',
    'category manager', 'buyer', 'head of supply', 'vp supply', 'director of supply',
    'chief supply', 'chief procurement', 'cpo', 'director of procurement',
    'vp procurement', 'global procurement', 'strategic sourcing',
    'materials manager', 'vendor management', 'supply manager',
]


def _months_since(date_str: str) -> int | None:
    """Return whole months elapsed since date_str (UTC). None if unparseable."""
    if not date_str:
        return None
    try:
        s = str(date_str).strip()
        dt = None
        # Try datetime formats with fixed slice sizes (format string length ≠ output length)
        for fmt, size in (('%Y-%m-%dT%H:%M:%S', 19), ('%Y-%m-%d %H:%M:%S', 19), ('%Y-%m-%d', 10)):
            try:
                dt = datetime.strptime(s[:size], fmt)
                break
            except ValueError:
                continue
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0, (datetime.now(timezone.utc) - dt).days // 30)
    except Exception:
        return None


def _hs_description(hs_code: str) -> str:
    chapter = str(hs_code)[:2]
    return HS_CHAPTERS.get(chapter, f'HS {hs_code}')


def _urgency(switch_prob: float, health_score: float) -> str:
    """
    HIGH   — health < 40 (CHURNED/DORMANT) OR switch_prob >= 0.60
    MEDIUM — health 40-69 (STRESSED) OR switch_prob 0.35-0.59
    LOW    — healthy relationship, low switch probability
    """
    prob_pct = (switch_prob or 0) * 100
    if health_score is not None and health_score < 40:
        return 'HIGH'
    if prob_pct >= 60:
        return 'HIGH'
    if health_score is not None and health_score < 70:
        return 'MEDIUM'
    if prob_pct >= 35:
        return 'MEDIUM'
    return 'LOW'


def _is_decision_maker(title: str) -> bool:
    if not title:
        return False
    t = title.lower()
    return any(kw in t for kw in DM_KEYWORDS)


class LeadService:

    def __init__(self, neo, pg_conn):
        """
        neo     : Neo4jClient instance
        pg_conn : live psycopg2 connection to Postgres (develop db)
        """
        self.neo     = neo
        self.pg_conn = pg_conn

    # ── public API ────────────────────────────────────────────────────────────

    def get_leads(self, limit: int = 50, min_score: float = 0.0, deduplicate: bool = False) -> list:
        """Return enriched leads ordered by final_score desc.

        deduplicate=True: keep only the top-scoring candidate supplier per unique buyer.
        """
        fetch_limit = limit * 5 if deduplicate else limit * 2
        raw = self._fetch_leads_from_neo4j(fetch_limit)

        enriched = []
        seen_buyers: set = set()
        for lead in raw:
            if (lead.get('final_score') or 0) < min_score:
                continue
            buyer_key = lead.get('buyer_org_id') or lead.get('buyer_name', '')
            if deduplicate and buyer_key and buyer_key in seen_buyers:
                continue
            enriched.append(self._enrich(lead))
            if buyer_key:
                seen_buyers.add(buyer_key)
            if len(enriched) >= limit:
                break
        return enriched

    def get_lead(self, lead_id: str) -> dict | None:
        """Return a single enriched lead by lead_id."""
        raw = self._fetch_lead_by_id(lead_id)
        if not raw:
            return None
        return self._enrich(raw)

    def get_summary(self) -> dict:
        """High-level graph stats."""
        result = self.neo.run("""
            MATCH (sl:SwitchLead) WITH count(sl) AS total_leads
            MATCH (tr:TradeRelationship) WITH total_leads, count(tr) AS relationships
            MATCH (opp:SupplierSwitchOpportunity) WITH total_leads, relationships, count(opp) AS opportunities
            RETURN total_leads, relationships, opportunities
        """, {})
        row = result[0] if result else {}
        return {
            'total_switch_leads': row.get('total_leads', 0),
            'trade_relationships_monitored': row.get('relationships', 0),
            'stressed_relationships': row.get('opportunities', 0),
        }

    # ── Neo4j queries ─────────────────────────────────────────────────────────

    def _fetch_leads_from_neo4j(self, limit: int) -> list:
        cypher = """
            MATCH (sl:SwitchLead)
            OPTIONAL MATCH (opp:SupplierSwitchOpportunity {opportunity_id: sl.opportunity_id})
            OPTIONAL MATCH (tr:TradeRelationship)-[:TRIGGERED]->(opp)
            RETURN sl.lead_id                    AS lead_id,
                   sl.buyer_org_id               AS buyer_org_id,
                   sl.buyer_name                 AS buyer_name,
                   sl.candidate_supplier_org_id  AS recommended_supplier_org_id,
                   sl.candidate_supplier_name    AS recommended_supplier_name,
                   opp.existing_supplier_org_id  AS current_supplier_org_id,
                   opp.existing_supplier_name    AS current_supplier_name,
                   sl.hs_code                    AS hs_code,
                   sl.final_lead_score           AS final_score,
                   sl.switch_probability         AS switch_probability,
                   sl.match_score                AS match_score,
                   sl.lead_priority              AS lead_priority,
                   sl.buyer_monthly_volume       AS monthly_qty,
                   sl.stress_reason              AS stress_reason,
                   sl.recommended_action         AS recommended_action,
                   sl.contact_name               AS contact_name,
                   sl.contact_title              AS contact_title,
                   sl.contact_email              AS contact_email,
                   sl.buyer_country              AS buyer_country,
                   sl.buyer_industry             AS buyer_industry,
                   coalesce(tr.health_score,  opp.stress_score)  AS health_score,
                   coalesce(tr.health_status, opp.stress_reason) AS health_status,
                   tr.total_shipments                            AS total_shipments,
                   tr.last_shipment_date                         AS last_shipment,
                   tr.relationship_age_months                    AS relationship_age_months,
                   tr.baseline_avg_monthly_qty                   AS trade_monthly_volume
            ORDER BY sl.final_lead_score DESC
            LIMIT $limit
        """
        rows = self.neo.run(cypher, {'limit': limit})
        return [self._parse_lead_row(r) for r in rows]

    def _fetch_lead_by_id(self, lead_id: str) -> dict | None:
        cypher = """
            MATCH (sl:SwitchLead {lead_id: $lid})
            OPTIONAL MATCH (opp:SupplierSwitchOpportunity {opportunity_id: sl.opportunity_id})
            OPTIONAL MATCH (tr:TradeRelationship)-[:TRIGGERED]->(opp)
            RETURN sl.lead_id                    AS lead_id,
                   sl.buyer_org_id               AS buyer_org_id,
                   sl.buyer_name                 AS buyer_name,
                   sl.candidate_supplier_org_id  AS recommended_supplier_org_id,
                   sl.candidate_supplier_name    AS recommended_supplier_name,
                   opp.existing_supplier_org_id  AS current_supplier_org_id,
                   opp.existing_supplier_name    AS current_supplier_name,
                   sl.hs_code                    AS hs_code,
                   sl.final_lead_score           AS final_score,
                   sl.switch_probability         AS switch_probability,
                   sl.match_score                AS match_score,
                   sl.lead_priority              AS lead_priority,
                   sl.buyer_monthly_volume       AS monthly_qty,
                   sl.stress_reason              AS stress_reason,
                   sl.recommended_action         AS recommended_action,
                   sl.contact_name               AS contact_name,
                   sl.contact_title              AS contact_title,
                   sl.contact_email              AS contact_email,
                   sl.buyer_country              AS buyer_country,
                   sl.buyer_industry             AS buyer_industry,
                   coalesce(tr.health_score,  opp.stress_score)  AS health_score,
                   coalesce(tr.health_status, opp.stress_reason) AS health_status,
                   tr.total_shipments                            AS total_shipments,
                   tr.last_shipment_date                         AS last_shipment,
                   tr.relationship_age_months                    AS relationship_age_months,
                   tr.baseline_avg_monthly_qty                   AS trade_monthly_volume
        """
        rows = self.neo.run(cypher, {'lid': lead_id})
        if not rows:
            return None
        return self._parse_lead_row(rows[0])

    def _parse_lead_row(self, row: dict) -> dict:
        hs_raw = str(row.get('hs_code') or '')
        hs_code = re.sub(r'[\[\]\s]', '', hs_raw)   # strip brackets/spaces
        return {
            'lead_id':                    row.get('lead_id', ''),
            'buyer_org_id':               row.get('buyer_org_id', ''),
            'buyer_name':                 row.get('buyer_name', ''),
            # current supplier = the one the buyer is currently buying from (may switch away)
            'current_supplier_org_id':    row.get('current_supplier_org_id', ''),
            'current_supplier_name':      row.get('current_supplier_name', ''),
            # recommended supplier = the candidate we're pitching as an alternative
            'recommended_supplier_org_id': row.get('recommended_supplier_org_id', ''),
            'recommended_supplier_name':   row.get('recommended_supplier_name', ''),
            'hs_code':                    hs_code,
            'final_score':                float(row.get('final_score') or 0),
            'switch_probability':         float(row.get('switch_probability') or 0),
            'match_score':                float(row.get('match_score') or 0),
            'lead_priority':              row.get('lead_priority', ''),
            'health_score':               float(row.get('health_score')) if row.get('health_score') is not None else None,
            'health_status':              row.get('health_status', ''),
            'stress_reason':              row.get('stress_reason', ''),
            'recommended_action':         row.get('recommended_action', ''),
            # prefer trade volume from TradeRelationship; fall back to SwitchLead copy
            'monthly_qty':                float(row.get('trade_monthly_volume') or row.get('monthly_qty') or 0),
            'total_shipments':            int(row.get('total_shipments') or 0),
            'last_shipment':              row.get('last_shipment', ''),
            'relationship_age_months':    int(row.get('relationship_age_months') or 0),
            'contact_name':               row.get('contact_name', ''),
            'contact_title':              row.get('contact_title', ''),
            'contact_email':              row.get('contact_email', ''),
            'buyer_country':              row.get('buyer_country', ''),
            'buyer_industry':             row.get('buyer_industry', ''),
        }

    # ── enrichment ────────────────────────────────────────────────────────────

    def _enrich(self, lead: dict) -> dict:
        buyer_name            = lead.get('buyer_name', '')
        current_supplier_name = lead.get('current_supplier_name', '')
        recommended_sup_name  = lead.get('recommended_supplier_name', '')
        hs_code               = lead.get('hs_code', '')

        # Use contact already stored in Neo4j if available, else look up ZoomInfo
        known_contact = None
        if lead.get('contact_name'):
            known_contact = {
                'name':     lead['contact_name'],
                'title':    lead.get('contact_title', ''),
                'email':    lead.get('contact_email', ''),
                'phone':    '',
                'linkedin': '',
                'seniority': '',
            }
        contacts          = [known_contact] if known_contact else self._find_decision_makers(buyer_name)
        buyer_info        = self._find_company_info(buyer_name)
        current_sup_info  = self._find_company_info(current_supplier_name)
        recommended_sup_info = self._find_company_info(recommended_sup_name)

        return {
            'lead_id':     lead['lead_id'],
            'urgency':     _urgency(lead['switch_probability'], lead.get('health_score')),
            'final_score': round(lead['final_score'], 1),
            'buyer': {
                'org_id':   lead['buyer_org_id'],
                'name':     buyer_name,
                'industry': buyer_info.get('industry', ''),
                'country':  buyer_info.get('country', ''),
                'website':  buyer_info.get('website', ''),
            },
            # The supplier the buyer is currently buying from (relationship at risk)
            'current_supplier': {
                'org_id':   lead.get('current_supplier_org_id', ''),
                'name':     current_supplier_name,
                'industry': current_sup_info.get('industry', ''),
                'country':  current_sup_info.get('country', ''),
            },
            # The alternative supplier we are recommending (the sales pitch target)
            'recommended_supplier': {
                'org_id':   lead.get('recommended_supplier_org_id', ''),
                'name':     recommended_sup_name,
                'industry': recommended_sup_info.get('industry', ''),
                'country':  recommended_sup_info.get('country', ''),
                'match_score': round(lead.get('match_score', 0), 1),
            },
            'trade': {
                'hs_code':                  hs_code,
                'hs_description':           _hs_description(hs_code),
                'monthly_volume':           round(lead['monthly_qty'], 0),
                'total_shipments':          lead['total_shipments'],
                'active_span_months':       lead['relationship_age_months'],
                'months_since_last_shipment': _months_since(lead.get('last_shipment', '')),
                'last_shipment_date':       lead['last_shipment'],
            },
            'stress': {
                'health_status':          lead.get('health_status', ''),
                'health_score':           lead.get('health_score'),
                'switch_probability_pct': round(lead['switch_probability'] * 100, 1),
                'stress_reason':          lead.get('stress_reason', ''),
                'lead_priority':          lead.get('lead_priority', ''),
            },
            'decision_makers': contacts,
            'recommendation':  lead.get('recommended_action', ''),
            'action_steps':    self._action_steps(lead, contacts),
        }

    def _action_steps(self, lead: dict, contacts: list) -> list:
        steps = []
        prob  = lead['switch_probability'] * 100
        name  = lead.get('buyer_name', 'the buyer')

        if contacts:
            dm = contacts[0]
            steps.append(f"Contact {dm['name']} ({dm['title']}) at {name} — {dm.get('email') or dm.get('phone') or 'see LinkedIn'}")
        else:
            steps.append(f"Find procurement contact at {name} via ZoomInfo or LinkedIn")

        steps.append(
            f"Lead with HS {lead['hs_code']} ({_hs_description(lead['hs_code'])}) supply capability"
        )
        if prob >= 60:
            steps.append("Act within 2 weeks — high switch probability indicates active sourcing")
        steps.append(
            f"Reference {round(lead['monthly_qty'],0):,.0f} units/month opportunity to anchor the conversation"
        )
        return steps

    # ── Postgres ZoomInfo lookups ─────────────────────────────────────────────

    def _find_decision_makers(self, company_name: str) -> list:
        if not company_name:
            return []
        try:
            cur = self.pg_conn.cursor()
            # contact_search has company_name column; join contact_enrich for email/phone
            cur.execute("""
                SELECT cs.first_name, cs.last_name, cs.job_title,
                       cs.company_name, cs.company_id,
                       ce.email, ce.phone, ce.mobile_phone
                FROM zoominfo.contact_search cs
                LEFT JOIN zoominfo.contact_enrich ce ON ce.id = cs.contact_id
                WHERE LOWER(cs.company_name) LIKE LOWER(%s)
                LIMIT 50
            """, (f'%{company_name}%',))
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            cur.close()
            if rows:
                contacts = [dict(zip(cols, r)) for r in rows]
                dms = [c for c in contacts if _is_decision_maker(c.get('job_title') or '')]
                result = dms if dms else contacts[:3]
                return [self._format_contact(c) for c in result[:5]]
        except Exception as e:
            warn(f'LeadService._find_decision_makers: {e}')
        return []

    def _find_company_info(self, company_name: str) -> dict:
        if not company_name:
            return {}
        try:
            cur = self.pg_conn.cursor()
            # Try ZoomInfo company_search first
            cur.execute("""
                SELECT company_name, company_id
                FROM zoominfo.company_search
                WHERE LOWER(company_name) LIKE LOWER(%s)
                LIMIT 1
            """, (f'%{company_name}%',))
            row = cur.fetchone()
            cur.close()
            if row:
                return {'industry': '', 'country': '', 'website': ''}

            # Fall back to Trademo company master for country/address
            cur2 = self.pg_conn.cursor()
            cur2.execute("""
                SELECT company_name, country, city, address,
                       total_shipment_value_import, import_trading_partner_count
                FROM raw.trademo_company_master
                WHERE LOWER(company_name) LIKE LOWER(%s)
                LIMIT 1
            """, (f'%{company_name}%',))
            row  = cur2.fetchone()
            cols = [d[0] for d in cur2.description]   # read description before close
            cur2.close()
            if row:
                data = dict(zip(cols, row))
                return {
                    'industry': '',
                    'country':  data.get('country', ''),
                    'website':  '',
                    'city':     data.get('city', ''),
                    'address':  data.get('address', ''),
                }
        except Exception as e:
            warn(f'LeadService._find_company_info: {e}')
        return {}

    def _format_contact(self, c: dict) -> dict:
        name = (c.get('full_name') or c.get('name') or
                f"{c.get('first_name', '')} {c.get('last_name', '')}".strip())
        return {
            'name':     name,
            'title':    c.get('job_title') or c.get('title') or c.get('jobtitle', ''),
            'email':    c.get('email') or c.get('work_email', '') or '',
            'phone':    c.get('direct_phone') or c.get('phone') or c.get('mobile_phone', '') or '',
            'linkedin': c.get('linkedin_url') or c.get('linkedin', '') or '',
            'seniority': c.get('seniority') or c.get('management_level', '') or '',
        }
