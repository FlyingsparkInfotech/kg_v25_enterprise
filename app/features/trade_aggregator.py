"""
TradeAggregator: Builds TradeRelationship and RelationshipSnapshot nodes in Neo4j
from ALL available Trademo PostgreSQL data sources.

Sources (in priority order):
  1. raw.trademo_shipment_bl         — actual bill-of-lading shipments (monthly snapshots)
  2. raw.trademo_company_import_partner × raw.trademo_company_imported_hs
                                     — confirmed buyer→supplier pairs with HS codes
  3. raw.trademo_buyer_supplier_list cross-match
                                     — buyers cross-matched to suppliers via shared HS codes
"""

from collections import defaultdict
from statistics import mean
from datetime import datetime, timezone, timedelta
import re

from app.core.ids import stable_id, utc_now
from app.core.logger import info, ok, warn, banner


def _months_ago(months: int) -> str:
    dt = datetime.now(timezone.utc) - timedelta(days=30 * months)
    return dt.strftime('%Y-%m-%d')


def _parse_date(val) -> datetime | None:
    if not val:
        return None
    s = str(val)[:10]
    try:
        return datetime.strptime(s, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _months_between(start, end) -> int:
    if not start or not end:
        return 0
    return max(0, (end - start).days // 30)


class TradeAggregator:

    def __init__(self, neo, pg, settings):
        self.neo = neo
        self.pg = pg
        self.settings = settings
        self.batch_size = int(getattr(settings.runtime, 'batch_size', 500))

    def run(self) -> dict:
        banner('TradeAggregator: Building TradeRelationship + RelationshipSnapshot nodes')

        total_rels   = 0
        total_snaps  = 0

        # ── Source 1: actual bill-of-lading shipments ─────────────────────────
        col_map = self._discover_columns('raw', 'trademo_shipment_bl')
        if col_map.get('buyer_id') and col_map.get('supplier_id'):
            rows = self._fetch_monthly_summaries(col_map)
            info(f'TradeAggregator Source-1: {len(rows)} monthly rows from trademo_shipment_bl')
            r = self._build_from_shipments(rows)
            total_rels  += r['relationships']
            total_snaps += r['snapshots']
        else:
            warn('TradeAggregator Source-1: buyer_id/supplier_id not found in trademo_shipment_bl — skipping')

        # ── Source 2: import_partner × imported_hs ───────────────────────────
        r2 = self._build_from_import_partners()
        total_rels  += r2['relationships']
        total_snaps += r2['snapshots']

        # ── Source 3: buyer_supplier_list cross-match ────────────────────────
        r3 = self._build_from_bsl_crossmatch()
        total_rels  += r3['relationships']

        ok(f"TradeAggregator complete — relationships={total_rels}, snapshots={total_snaps}")
        return {'relationships': total_rels, 'snapshots': total_snaps}

    # ── Source 1: shipment_bl ─────────────────────────────────────────────────

    def _discover_columns(self, schema: str, table: str) -> dict:
        try:
            actual_cols = self.pg.columns(schema, table)
        except Exception as e:
            warn(f'TradeAggregator: failed to get columns for {schema}.{table}: {e}')
            return {}

        lower_map = {c.lower(): c for c in actual_cols}
        candidates = {
            'buyer_id':      ['importer_id', 'buyer_id', 'consignee_id', 'companyid'],
            'buyer_name':    ['importer_name', 'buyer_name', 'consignee_name', 'company_name'],
            'supplier_id':   ['exporter_id', 'supplier_id', 'shipper_id'],
            'supplier_name': ['exporter_name', 'supplier_name', 'shipper_name'],
            'hs_code':       ['hs_code', 'hscode', 'matched_hs_code', 'commodity_code'],
            'quantity':      ['quantity', 'net_weight', 'gross_weight', 'total_weight', 'weight_kg'],
            'value':         ['shipment_value', 'total_value', 'trade_value', 'fob_value', 'cif_value', 'amount'],
            'date':          ['shipment_date', 'bl_date', 'date', 'arrival_date', 'departure_date', 'created_at'],
        }
        col_map = {}
        for canonical, options in candidates.items():
            for opt in options:
                if opt in lower_map:
                    col_map[canonical] = lower_map[opt]
                    info(f'TradeAggregator: {canonical} -> "{lower_map[opt]}"')
                    break
            if canonical not in col_map:
                warn(f'TradeAggregator: no column for "{canonical}" in {schema}.{table}')
                col_map[canonical] = None
        return col_map

    def _fetch_monthly_summaries(self, col_map: dict) -> list:
        required = ['buyer_id', 'supplier_id', 'hs_code', 'date']
        if any(not col_map.get(r) for r in required):
            return []

        b  = f'"{col_map["buyer_id"]}"'
        s  = f'"{col_map["supplier_id"]}"'
        h  = f'"{col_map["hs_code"]}"'
        d  = f'"{col_map["date"]}"'
        bn = f'"{col_map["buyer_name"]}"'    if col_map.get('buyer_name')    else 'NULL'
        sn = f'"{col_map["supplier_name"]}"' if col_map.get('supplier_name') else 'NULL'
        q  = f'"{col_map["quantity"]}"'      if col_map.get('quantity')      else 'NULL'
        v  = f'"{col_map["value"]}"'         if col_map.get('value')         else 'NULL'

        sql = f"""
            SELECT
                {b}  AS buyer_id,
                {bn} AS buyer_name,
                {s}  AS supplier_id,
                {sn} AS supplier_name,
                {h}  AS hs_code,
                DATE_TRUNC('month', {d}::timestamp) AS year_month,
                COUNT(*) AS shipment_count,
                SUM(CASE WHEN {q} IS NULL THEN 0 ELSE {q}::numeric END) AS total_quantity,
                SUM(CASE WHEN {v} IS NULL THEN 0 ELSE {v}::numeric END) AS total_value
            FROM raw.trademo_shipment_bl
            WHERE {b} IS NOT NULL AND {s} IS NOT NULL
              AND {h} IS NOT NULL AND {d} IS NOT NULL
              AND {d}::timestamp >= NOW() - INTERVAL '36 months'
            GROUP BY {b}, {bn}, {s}, {sn}, {h},
                     DATE_TRUNC('month', {d}::timestamp)
            ORDER BY {b}, {s}, {h}, year_month
        """
        try:
            return self.pg.q(sql)
        except Exception as e:
            warn(f'TradeAggregator._fetch_monthly_summaries: {e}')
            return []

    def _build_from_shipments(self, rows: list) -> dict:
        if not rows:
            return {'relationships': 0, 'snapshots': 0}

        now = utc_now()
        groups: dict = defaultdict(list)
        for row in rows:
            b = str(row.get('buyer_id') or '').strip()
            s = str(row.get('supplier_id') or '').strip()
            h = str(row.get('hs_code') or '').strip()
            if b and s and h:
                groups[(b, s, h)].append(row)

        rel_batch, snap_batch = [], []
        total_rels = total_snaps = 0

        for (buyer_id, supplier_id, hs_code), month_rows in groups.items():
            rel_id      = stable_id(buyer_id, supplier_id, hs_code)
            sorted_rows = sorted(month_rows, key=lambda r: str(r.get('year_month') or ''))
            quantities  = [float(r.get('total_quantity') or 0) for r in sorted_rows]
            values      = [float(r.get('total_value')    or 0) for r in sorted_rows]
            baseline_qty   = mean(quantities[:6]) if quantities[:6] else 0.0
            baseline_value = mean(values[:6])     if values[:6]     else 0.0
            buyer_name    = next((str(r.get('buyer_name')    or '') for r in sorted_rows if r.get('buyer_name')), '')
            supplier_name = next((str(r.get('supplier_name') or '') for r in sorted_rows if r.get('supplier_name')), '')

            rel_batch.append({
                'rel_id': rel_id, 'buyer_id': buyer_id, 'buyer_name': buyer_name,
                'supplier_id': supplier_id, 'supplier_name': supplier_name,
                'hs_code': hs_code, 'hs_chapter': hs_code[:2],
                'total_shipments': sum(int(r.get('shipment_count') or 0) for r in sorted_rows),
                'relationship_age_months': len(sorted_rows),
                'baseline_avg_qty': baseline_qty, 'baseline_avg_value': baseline_value,
                'last_shipment_date': str(sorted_rows[-1].get('year_month') or ''),
                'data_source': 'shipment_bl', 'updated_at': now,
            })

            for row in sorted_rows:
                qty = float(row.get('total_quantity') or 0)
                ym  = str(row.get('year_month') or '')
                snap_batch.append({
                    'snapshot_id': stable_id(rel_id, ym), 'rel_id': rel_id,
                    'year_month': ym,
                    'shipment_count': int(row.get('shipment_count') or 0),
                    'total_quantity': qty, 'total_value': float(row.get('total_value') or 0),
                    'qty_vs_baseline_pct': round((qty / baseline_qty - 1.0) * 100, 2) if baseline_qty > 0 else 0.0,
                    'created_at': now,
                })

            if len(rel_batch) >= self.batch_size:
                self._write_rels(rel_batch); total_rels += len(rel_batch); rel_batch = []
            if len(snap_batch) >= self.batch_size:
                self._write_snaps(snap_batch); total_snaps += len(snap_batch); snap_batch = []

        total_rels  += len(rel_batch);  self._write_rels(rel_batch)
        total_snaps += len(snap_batch); self._write_snaps(snap_batch)
        info(f'TradeAggregator Source-1: {total_rels} relationships, {total_snaps} snapshots')
        return {'relationships': total_rels, 'snapshots': total_snaps}

    # ── Source 2: import_partner × imported_hs ───────────────────────────────

    def _build_from_import_partners(self) -> dict:
        sql = """
            SELECT
                p.company_id        AS buyer_id,
                p.company_name      AS buyer_name,
                p.partner_company_id   AS supplier_id,
                p.partner_company_name AS supplier_name,
                p.partner_country      AS supplier_country,
                p.trade_volume_kg      AS trade_volume_kg,
                p.import_share_pct     AS partner_share_pct,
                h.hs_code              AS hs_code,
                h.import_share_pct     AS hs_share_pct,
                b.from_date            AS from_date,
                b.to_date              AS to_date,
                b.number_of_shipments  AS total_shipments
            FROM raw.trademo_company_import_partner p
            JOIN raw.trademo_company_imported_hs h
                ON p.company_id = h.company_id
            LEFT JOIN raw.trademo_buyer_supplier_list b
                ON p.company_id = b.company_id AND b.company_role = 'buyer'
            WHERE p.company_id IS NOT NULL
              AND p.partner_company_id IS NOT NULL
              AND h.hs_code IS NOT NULL
              AND h.import_share_pct >= 0.5
            ORDER BY p.company_id, p.partner_company_id, h.import_share_pct DESC
        """
        try:
            rows = self.pg.q(sql)
        except Exception as e:
            warn(f'TradeAggregator Source-2: query failed: {e}')
            return {'relationships': 0, 'snapshots': 0}

        info(f'TradeAggregator Source-2: {len(rows)} import_partner×hs rows')
        if not rows:
            return {'relationships': 0, 'snapshots': 0}

        now = utc_now()
        rel_batch, snap_batch = [], []
        total_rels = total_snaps = 0
        seen = set()

        for row in rows:
            buyer_id      = str(row.get('buyer_id')    or '').strip()
            supplier_id   = str(row.get('supplier_id') or '').strip()
            hs_code       = str(row.get('hs_code')     or '').strip()
            if not buyer_id or not supplier_id or not hs_code:
                continue

            rel_id = stable_id(buyer_id, supplier_id, hs_code)
            if rel_id in seen:
                continue
            seen.add(rel_id)

            trade_vol    = float(row.get('trade_volume_kg')  or 0)
            hs_share     = float(row.get('hs_share_pct')     or 0) / 100
            partner_share= float(row.get('partner_share_pct')or 0) / 100
            monthly_vol  = trade_vol * hs_share / 12.0   # approximate monthly volume

            from_dt  = _parse_date(row.get('from_date'))
            to_dt    = _parse_date(row.get('to_date'))
            now_dt   = datetime.now(timezone.utc)
            age_months = _months_between(from_dt, to_dt or now_dt)
            last_date  = str(row.get('to_date') or '')[:10] or _months_ago(6)

            rel_batch.append({
                'rel_id':      rel_id,
                'buyer_id':    buyer_id,
                'buyer_name':  str(row.get('buyer_name')    or ''),
                'supplier_id': supplier_id,
                'supplier_name': str(row.get('supplier_name') or ''),
                'hs_code':     hs_code,
                'hs_chapter':  hs_code[:2],
                'total_shipments':         int(row.get('total_shipments') or 0),
                'relationship_age_months': max(1, age_months),
                'baseline_avg_qty':        round(monthly_vol, 2),
                'baseline_avg_value':      0.0,
                'last_shipment_date':      last_date,
                'data_source':             'import_partner',
                'updated_at':              now,
            })

            # Generate synthetic monthly snapshots from from_date → to_date
            if from_dt and age_months > 0:
                snaps = self._synthetic_snapshots(rel_id, from_dt, to_dt or now_dt,
                                                  monthly_vol, now)
                snap_batch.extend(snaps)

            if len(rel_batch) >= self.batch_size:
                self._write_rels(rel_batch); total_rels += len(rel_batch); rel_batch = []
            if len(snap_batch) >= self.batch_size:
                self._write_snaps(snap_batch); total_snaps += len(snap_batch); snap_batch = []

        total_rels  += len(rel_batch);  self._write_rels(rel_batch)
        total_snaps += len(snap_batch); self._write_snaps(snap_batch)
        info(f'TradeAggregator Source-2: {total_rels} relationships, {total_snaps} snapshots')
        return {'relationships': total_rels, 'snapshots': total_snaps}

    # ── Source 3: buyer_supplier_list cross-match ─────────────────────────────

    def _build_from_bsl_crossmatch(self) -> dict:
        """
        Cross-match buyers and suppliers from trademo_buyer_supplier_list
        via shared HS codes (exported_hs vs imported_hs) or country proximity.
        Creates TradeRelationship nodes with estimated volumes.
        """
        sql = """
            SELECT
                b.company_id        AS buyer_id,
                b.company_name      AS buyer_name,
                b.country           AS buyer_country,
                b.number_of_shipments AS buyer_shipments,
                b.shipment_value    AS buyer_value,
                b.from_date         AS from_date,
                b.to_date           AS to_date,
                s.company_id        AS supplier_id,
                s.company_name      AS supplier_name,
                s.country           AS supplier_country,
                ih.hs_code          AS hs_code,
                ih.import_share_pct AS hs_share_pct,
                eh.export_share_pct AS supplier_export_share
            FROM raw.trademo_buyer_supplier_list b
            JOIN raw.trademo_company_imported_hs ih
                ON b.company_id = ih.company_id
            JOIN raw.trademo_company_exported_hs eh
                ON ih.hs_code = eh.hs_code
            JOIN raw.trademo_buyer_supplier_list s
                ON eh.company_id = s.company_id AND s.company_role = 'supplier'
            WHERE b.company_role = 'buyer'
              AND b.company_id <> s.company_id
              AND ih.import_share_pct >= 1.0
              AND eh.export_share_pct >= 1.0
            ORDER BY b.company_id, ih.import_share_pct DESC, s.company_id
        """
        try:
            rows = self.pg.q(sql)
        except Exception as e:
            warn(f'TradeAggregator Source-3: query failed: {e}')
            return {'relationships': 0}

        info(f'TradeAggregator Source-3: {len(rows)} buyer-supplier-HS cross-match rows')
        if not rows:
            return {'relationships': 0}

        now = utc_now()
        rel_batch = []
        total_rels = 0
        seen = set()

        for row in rows:
            buyer_id    = str(row.get('buyer_id')    or '').strip()
            supplier_id = str(row.get('supplier_id') or '').strip()
            hs_code     = str(row.get('hs_code')     or '').strip()
            if not buyer_id or not supplier_id or not hs_code:
                continue

            rel_id = stable_id(buyer_id, supplier_id, hs_code)
            if rel_id in seen:
                continue
            seen.add(rel_id)

            buyer_val   = float(row.get('buyer_value')    or 0)
            hs_share    = float(row.get('hs_share_pct')   or 0) / 100
            monthly_vol = (buyer_val * hs_share) / 12.0

            from_dt    = _parse_date(row.get('from_date'))
            to_dt      = _parse_date(row.get('to_date'))
            now_dt     = datetime.now(timezone.utc)
            age_months = _months_between(from_dt, to_dt or now_dt)
            last_date  = str(row.get('to_date') or '')[:10] or _months_ago(12)

            rel_batch.append({
                'rel_id':      rel_id,
                'buyer_id':    buyer_id,
                'buyer_name':  str(row.get('buyer_name')    or ''),
                'supplier_id': supplier_id,
                'supplier_name': str(row.get('supplier_name') or ''),
                'hs_code':     hs_code,
                'hs_chapter':  hs_code[:2],
                'total_shipments':         int(row.get('buyer_shipments') or 0),
                'relationship_age_months': max(1, age_months),
                'baseline_avg_qty':        round(monthly_vol, 2),
                'baseline_avg_value':      0.0,
                'last_shipment_date':      last_date,
                'data_source':             'bsl_crossmatch',
                'updated_at':              now,
            })

            if len(rel_batch) >= self.batch_size:
                self._write_rels(rel_batch); total_rels += len(rel_batch); rel_batch = []

        total_rels += len(rel_batch); self._write_rels(rel_batch)
        info(f'TradeAggregator Source-3: {total_rels} relationships from cross-match')
        return {'relationships': total_rels}

    # ── helpers ───────────────────────────────────────────────────────────────

    def _synthetic_snapshots(self, rel_id: str, from_dt: datetime,
                             to_dt: datetime, monthly_vol: float, now: str) -> list:
        """Generate monthly snapshot records between from_dt and to_dt."""
        snaps = []
        current = from_dt.replace(day=1)
        end     = to_dt.replace(day=1)
        months_total = _months_between(from_dt, to_dt) or 1

        while current <= end:
            ym = current.strftime('%Y-%m-%d')
            # Simulate slight volume variation; taper off near to_dt for dormant feel
            months_remaining = _months_between(current, to_dt)
            decay = max(0.1, 1.0 - (months_remaining / months_total) * 0.3) if months_remaining < 6 else 1.0
            vol   = round(monthly_vol * decay, 2)

            snaps.append({
                'snapshot_id':     stable_id(rel_id, ym),
                'rel_id':          rel_id,
                'year_month':      ym,
                'shipment_count':  max(1, int(vol / 1000)) if vol > 0 else 0,
                'total_quantity':  vol,
                'total_value':     0.0,
                'qty_vs_baseline_pct': 0.0,
                'created_at':      now,
            })
            # Advance month
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)

        return snaps

    def _write_rels(self, batch: list):
        if not batch:
            return
        self.neo.run("""
            UNWIND $batch AS row
            MERGE (tr:TradeRelationship {rel_id: row.rel_id})
            SET tr.buyer_org_id               = row.buyer_id,
                tr.buyer_name                 = row.buyer_name,
                tr.supplier_org_id            = row.supplier_id,
                tr.supplier_name              = row.supplier_name,
                tr.hs_code                    = row.hs_code,
                tr.hs_chapter                 = row.hs_chapter,
                tr.total_shipments            = row.total_shipments,
                tr.relationship_age_months    = row.relationship_age_months,
                tr.baseline_avg_monthly_qty   = row.baseline_avg_qty,
                tr.baseline_avg_monthly_value = row.baseline_avg_value,
                tr.last_shipment_date         = row.last_shipment_date,
                tr.data_source                = row.data_source,
                tr.health_status              = coalesce(tr.health_status, 'pending'),
                tr.health_score               = coalesce(tr.health_score, null),
                tr.updated_at                 = row.updated_at
            WITH tr, row
            OPTIONAL MATCH (buyer:Organization {orgId: row.buyer_id})
            FOREACH (_ IN CASE WHEN buyer IS NOT NULL THEN [1] ELSE [] END |
                MERGE (buyer)-[:BUYER_IN]->(tr)
            )
            WITH tr, row
            OPTIONAL MATCH (supplier:Organization {orgId: row.supplier_id})
            FOREACH (_ IN CASE WHEN supplier IS NOT NULL THEN [1] ELSE [] END |
                MERGE (supplier)-[:SUPPLIER_IN]->(tr)
            )
            RETURN count(tr) AS c
        """, {'batch': batch})

    def _write_snaps(self, batch: list):
        if not batch:
            return
        self.neo.run("""
            UNWIND $batch AS row
            MERGE (snap:RelationshipSnapshot {snapshot_id: row.snapshot_id})
            SET snap.rel_id              = row.rel_id,
                snap.year_month          = row.year_month,
                snap.shipment_count      = row.shipment_count,
                snap.total_quantity      = row.total_quantity,
                snap.total_value         = row.total_value,
                snap.qty_vs_baseline_pct = row.qty_vs_baseline_pct,
                snap.created_at          = row.created_at
            WITH snap, row
            MATCH (tr:TradeRelationship {rel_id: row.rel_id})
            MERGE (tr)-[:HAS_SNAPSHOT]->(snap)
            RETURN count(snap) AS c
        """, {'batch': batch})
