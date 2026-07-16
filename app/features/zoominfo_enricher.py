"""
ZoomInfoEnricher: Enriches Organization nodes in Neo4j with ZoomInfo firmographic,
contact, intent and news trigger data from PostgreSQL.
"""

from collections import defaultdict

from app.core.ids import utc_now
from app.core.logger import info, ok, warn, banner


def _first_col(lower_map: dict, candidates: list):
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None


class ZoomInfoEnricher:

    def __init__(self, neo, pg, settings):
        self.neo = neo
        self.pg = pg
        self.settings = settings
        self._batch_size = int(settings.runtime.batch_size)

    def run(self) -> dict:
        banner('ZoomInfoEnricher: Enriching Organizations with ZoomInfo data')
        orgs     = self._enrich_organizations()
        contacts = self._add_contacts()
        intent   = self._add_intent_signals()
        self._add_news_triggers()
        ok(f'ZoomInfoEnricher complete — orgs={orgs}, contacts={contacts}, intent={intent}')
        return {'orgs_enriched': orgs, 'contacts_added': contacts, 'intent_signals': intent}

    # ── helpers ────────────────────────────────────────────────────────────────

    def _cols(self, schema: str, table: str) -> dict:
        try:
            return {c.lower(): c for c in self.pg.columns(schema, table)}
        except Exception as e:
            warn(f'ZoomInfoEnricher: cannot get columns for {schema}.{table}: {e}')
            return {}

    def _neo_batch(self, cypher: str, batch: list) -> int:
        if not batch:
            return 0
        result = self.neo.run(cypher, {'batch': batch})
        return int(result[0].get('c', 0)) if result else 0

    # ── org enrichment ─────────────────────────────────────────────────────────

    def _enrich_organizations(self) -> int:
        cl = self._cols('zoominfo', 'company_search')
        if not cl:
            return 0

        name_col     = _first_col(cl, ['company_name', 'name'])
        emp_col      = _first_col(cl, ['employee_count', 'employeecount', 'employees'])
        rev_col      = _first_col(cl, ['revenue', 'annual_revenue', 'revenue_range'])
        ind_col      = _first_col(cl, ['industry', 'primary_industry', 'sector'])
        domain_col   = _first_col(cl, ['domain', 'website'])
        linkedin_col = _first_col(cl, ['linkedin_url'])
        city_col     = _first_col(cl, ['city'])
        country_col  = _first_col(cl, ['country', 'country_name'])

        if not name_col:
            warn('ZoomInfoEnricher: no company name column in company_search')
            return 0

        parts = [f'"{name_col}" AS zi_name']
        if emp_col:      parts.append(f'"{emp_col}" AS employee_count')
        if rev_col:      parts.append(f'"{rev_col}" AS revenue')
        if ind_col:      parts.append(f'"{ind_col}" AS industry')
        if domain_col:   parts.append(f'"{domain_col}" AS domain')
        if linkedin_col: parts.append(f'"{linkedin_col}" AS linkedin_url')
        if city_col:     parts.append(f'"{city_col}" AS city')
        if country_col:  parts.append(f'"{country_col}" AS country')

        try:
            rows = self.pg.q(f'SELECT {", ".join(parts)} FROM zoominfo.company_search WHERE "{name_col}" IS NOT NULL')
        except Exception as e:
            warn(f'ZoomInfoEnricher._enrich_organizations: {e}')
            return 0

        _CYPHER = """
            UNWIND $batch AS row
            MATCH (o:Organization)
            WHERE toLower(o.orgName) CONTAINS toLower(row.name)
               OR (row.domain <> '' AND o.domain = row.domain)
            SET o.zi_employee_count = row.employee_count,
                o.zi_revenue_band   = row.revenue,
                o.zi_industry       = row.industry,
                o.zi_domain         = row.domain,
                o.zi_linkedin       = row.linkedin_url,
                o.zi_city           = row.city,
                o.zi_country        = row.country,
                o.zi_enriched_at    = row.enriched_at
            RETURN count(o) AS c
        """

        now = utc_now()
        total = 0
        batch = []
        for row in rows:
            name = str(row.get('zi_name') or '').strip()
            if not name:
                continue
            batch.append({
                'name':           name,
                'employee_count': row.get('employee_count'),
                'revenue':        str(row.get('revenue') or ''),
                'industry':       str(row.get('industry') or ''),
                'domain':         str(row.get('domain') or ''),
                'linkedin_url':   str(row.get('linkedin_url') or ''),
                'city':           str(row.get('city') or ''),
                'country':        str(row.get('country') or ''),
                'enriched_at':    now,
            })
            if len(batch) >= self._batch_size:
                total += self._neo_batch(_CYPHER, batch)
                batch = []
        total += self._neo_batch(_CYPHER, batch)
        info(f'ZoomInfoEnricher: enriched {total} Organization nodes from company_search')
        return total

    # ── contacts ───────────────────────────────────────────────────────────────

    def _add_contacts(self) -> int:
        cl = self._cols('zoominfo', 'contact_enrich')
        if not cl:
            return 0

        first_col   = _first_col(cl, ['first_name', 'firstname'])
        last_col    = _first_col(cl, ['last_name', 'lastname'])
        full_col    = _first_col(cl, ['full_name', 'fullname', 'name'])
        title_col   = _first_col(cl, ['title', 'job_title', 'jobtitle'])
        email_col   = _first_col(cl, ['email', 'email_address'])
        phone_col   = _first_col(cl, ['phone', 'phone_number'])
        company_col = _first_col(cl, ['company_name', 'company'])
        linkedin_col = _first_col(cl, ['linkedin_url'])
        seniority_col = _first_col(cl, ['seniority_level', 'seniority'])

        parts = []
        for alias, col in [('first_name', first_col), ('last_name', last_col), ('full_name', full_col),
                           ('title', title_col), ('email', email_col), ('phone', phone_col),
                           ('company_name', company_col), ('linkedin_url', linkedin_col),
                           ('seniority_level', seniority_col)]:
            if col:
                parts.append(f'"{col}" AS {alias}')

        if not parts:
            return 0

        try:
            rows = self.pg.q(f'SELECT {", ".join(parts)} FROM zoominfo.contact_enrich')
        except Exception as e:
            warn(f'ZoomInfoEnricher._add_contacts: {e}')
            return 0

        DM_KEYWORDS = ['procurement', 'supply chain', 'purchasing', 'cpo', 'vp supply',
                       'sourcing', 'operations', 'logistics']

        _CYPHER = """
            UNWIND $batch AS row
            MERGE (p:Person {email: CASE WHEN row.email <> '' THEN row.email
                                         ELSE 'zi:' + row.name END})
            SET p.name             = row.name,
                p.title            = row.title,
                p.phone            = row.phone,
                p.linkedin_url     = row.linkedin_url,
                p.seniority_level  = row.seniority_level,
                p.is_decision_maker = row.is_decision_maker,
                p.zi_enriched_at   = row.created_at
            WITH p, row
            MATCH (o:Organization)
            WHERE row.company_name <> ''
              AND toLower(o.orgName) CONTAINS toLower(row.company_name)
            MERGE (p)-[:CONTACT_AT]->(o)
            RETURN count(p) AS c
        """

        now = utc_now()
        total = 0
        batch = []
        for row in rows:
            first = str(row.get('first_name') or '').strip()
            last  = str(row.get('last_name')  or '').strip()
            full  = str(row.get('full_name')  or '').strip()
            name  = full or f'{first} {last}'.strip() or 'Unknown'
            title = str(row.get('title') or '')
            is_dm = any(kw in title.lower() for kw in DM_KEYWORDS)
            batch.append({
                'name': name, 'title': title,
                'email': str(row.get('email') or ''),
                'phone': str(row.get('phone') or ''),
                'company_name': str(row.get('company_name') or ''),
                'linkedin_url': str(row.get('linkedin_url') or ''),
                'seniority_level': str(row.get('seniority_level') or ''),
                'is_decision_maker': is_dm,
                'created_at': now,
            })
            if len(batch) >= self._batch_size:
                total += self._neo_batch(_CYPHER, batch)
                batch = []
        total += self._neo_batch(_CYPHER, batch)
        info(f'ZoomInfoEnricher: added/updated {total} contact Person nodes')
        return total

    # ── intent signals ─────────────────────────────────────────────────────────

    def _add_intent_signals(self) -> int:
        cl = self._cols('zoominfo', 'intent_search')
        if not cl:
            return 0

        company_col = _first_col(cl, ['company_name', 'company'])
        topic_col   = _first_col(cl, ['topic', 'intent_topic'])
        date_col    = _first_col(cl, ['date', 'signal_date'])

        if not company_col:
            return 0

        parts = [f'"{company_col}" AS company_name']
        if topic_col: parts.append(f'"{topic_col}" AS topic')
        if date_col:  parts.append(f'"{date_col}" AS signal_date')

        try:
            rows = self.pg.q(f'SELECT {", ".join(parts)} FROM zoominfo.intent_search WHERE "{company_col}" IS NOT NULL')
        except Exception as e:
            warn(f'ZoomInfoEnricher._add_intent_signals: {e}')
            return 0

        agg: dict = defaultdict(lambda: {'count': 0, 'topics': set(), 'last_date': ''})
        for row in rows:
            company = str(row.get('company_name') or '').strip()
            if not company:
                continue
            agg[company]['count'] += 1
            topic = str(row.get('topic') or '').strip()
            if topic:
                agg[company]['topics'].add(topic)
            d = str(row.get('signal_date') or '')
            if d and d > agg[company]['last_date']:
                agg[company]['last_date'] = d

        _CYPHER = """
            UNWIND $batch AS row
            MATCH (o:Organization)
            WHERE toLower(o.orgName) CONTAINS toLower(row.company_name)
            SET o.zi_intent_signal_count = row.intent_signal_count,
                o.zi_intent_topics       = row.intent_topics,
                o.zi_last_intent_date    = row.last_intent_date,
                o.zi_enriched_at         = row.enriched_at
            RETURN count(o) AS c
        """

        now = utc_now()
        total = 0
        batch = []
        for company, data in agg.items():
            batch.append({
                'company_name':        company,
                'intent_signal_count': data['count'],
                'intent_topics':       ', '.join(sorted(data['topics'])),
                'last_intent_date':    data['last_date'],
                'enriched_at':         now,
            })
            if len(batch) >= self._batch_size:
                total += self._neo_batch(_CYPHER, batch)
                batch = []
        total += self._neo_batch(_CYPHER, batch)
        return total

    # ── news triggers ──────────────────────────────────────────────────────────

    def _add_news_triggers(self) -> int:
        LEADERSHIP = ['ceo', 'cpo', 'vp', 'director', 'chief', 'president', 'appoint', 'resign', 'hire', 'named']
        FUNDING    = ['funding', 'investment', 'raise', 'series', 'capital', 'venture', 'ipo']
        MA         = ['merger', 'acquisition', 'acquire', 'merge', 'takeover', 'buyout']

        flags: dict = defaultdict(lambda: {
            'news_count': 0, 'last_news_date': '',
            'has_leadership_change': False,
            'has_funding_event': False,
            'has_ma_event': False,
        })

        def _process(schema, table):
            cl = self._cols(schema, table)
            if not cl:
                return
            company_col = _first_col(cl, ['company_name', 'company'])
            content_col = _first_col(cl, ['headline', 'title', 'summary', 'description', 'content', 'body'])
            date_col    = _first_col(cl, ['date', 'published_date', 'news_date', 'created_at'])
            if not company_col:
                return
            parts = [f'"{company_col}" AS company_name']
            if content_col: parts.append(f'"{content_col}" AS content')
            if date_col:    parts.append(f'"{date_col}" AS news_date')
            try:
                rows = self.pg.q(f'SELECT {", ".join(parts)} FROM {schema}.{table} WHERE "{company_col}" IS NOT NULL')
            except Exception as e:
                warn(f'ZoomInfoEnricher._add_news_triggers {schema}.{table}: {e}')
                return
            for row in rows:
                company = str(row.get('company_name') or '').strip()
                if not company:
                    continue
                content = str(row.get('content') or '').lower()
                d = str(row.get('news_date') or '')
                flags[company]['news_count'] += 1
                if d and d > flags[company]['last_news_date']:
                    flags[company]['last_news_date'] = d
                if any(kw in content for kw in LEADERSHIP): flags[company]['has_leadership_change'] = True
                if any(kw in content for kw in FUNDING):    flags[company]['has_funding_event']     = True
                if any(kw in content for kw in MA):         flags[company]['has_ma_event']           = True

        _process('zoominfo', 'news_search')
        _process('zoominfo', 'scoop_search')

        if not flags:
            return 0

        _CYPHER = """
            UNWIND $batch AS row
            MATCH (o:Organization)
            WHERE toLower(o.orgName) CONTAINS toLower(row.company_name)
            SET o.zi_news_count            = row.news_count,
                o.zi_last_news_date        = row.last_news_date,
                o.zi_has_leadership_change = row.has_leadership_change,
                o.zi_has_funding_event     = row.has_funding_event,
                o.zi_has_ma_event          = row.has_ma_event,
                o.zi_enriched_at           = row.enriched_at
            RETURN count(o) AS c
        """

        now = utc_now()
        total = 0
        batch = []
        for company, data in flags.items():
            batch.append({
                'company_name':          company,
                'news_count':            data['news_count'],
                'last_news_date':        data['last_news_date'],
                'has_leadership_change': data['has_leadership_change'],
                'has_funding_event':     data['has_funding_event'],
                'has_ma_event':          data['has_ma_event'],
                'enriched_at':           now,
            })
            if len(batch) >= self._batch_size:
                total += self._neo_batch(_CYPHER, batch)
                batch = []
        total += self._neo_batch(_CYPHER, batch)
        info(f'ZoomInfoEnricher: updated {total} Organization nodes with news triggers')
        return total
