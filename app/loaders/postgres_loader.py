import json
from app.core.ids import stable_id, utc_now
from app.core.logger import info, ok, warn
from app.mappings.postgres_signal_map import signal_mapping

_PAYLOAD_LIMIT   = 12000   # bytes — log a warning when a row exceeds this
_MAX_COLS        = 100     # max columns selected per table


def first(row, keys):
    lower = {str(k).lower(): k for k in row.keys()}
    for k in keys:
        if k in row and row[k] not in (None, ''):
            return row[k]
        lk = k.lower()
        if lk in lower and row[lower[lk]] not in (None, ''):
            return row[lower[lk]]
    return None


def to_float(v):
    if v in (None, ''):
        return None
    try:
        return float(str(v).replace(',', '').strip())
    except Exception:
        return None


class PostgresLoader:
    def __init__(self, neo, pg, settings):
        self.neo      = neo
        self.pg       = pg
        self.settings = settings

    def run(self):
        total = 0
        for t in self.pg.list_tables(self.settings.postgres.schemas):
            schema, table = t['schema'], t['table']
            mapped = signal_mapping(schema, table)
            if not mapped:
                continue
            st, sf = mapped
            key = f'{schema}.{table}'.lower()

            try:
                cols  = self.pg.columns(schema, table)
                order = next(
                    (c for c in ['updated_at', 'modified_on', 'created_at', 'created_on',
                                 'timestamp', 'event_time', 'shipment_date', 'date', 'id']
                     if c in cols),
                    None,
                )
                sel = ', '.join([f'"{c}"' for c in cols[:_MAX_COLS]])
                sql = f'SELECT {sel} FROM "{schema}"."{table}"'
                if order:
                    sql += f' ORDER BY "{order}" DESC'
                if self.settings.postgres.table_limit and self.settings.postgres.table_limit > 0:
                    sql += f' LIMIT {int(self.settings.postgres.table_limit)}'
                rows = self.pg.q(sql)
            except Exception as e:
                warn(f'skip {key}: {e}')
                continue

            if not rows:
                continue

            self.neo.run(
                "MERGE (sr:SourceRegistry {source_key:$k}) "
                "SET sr.source_group=$g, sr.source_name=$n, sr.is_active=true",
                {'k': key, 'g': sf, 'n': table},
            )

            prepared = []
            for r in rows:
                rid = first(r, [
                    'id', 'source_record_id', 'uuid', 'uid',
                    'company_id', 'companyid', 'contact_id', 'shipment_id',
                    'identifier', 'bl_id', 'bsl_key',
                ]) or stable_id(key, json.dumps(r, default=str, sort_keys=True)[:1000])
                rid = str(rid)

                raw_uid = stable_id('source_record', key, rid)
                sig_uid = stable_id('signal', key, rid, st)

                account  = first(r, ['account_id', 'company_id', 'companyid', 'organization_id',
                                     'org_id', 'importer_id', 'exporter_id', 'company_name',
                                     'companyname', 'consignee_name', 'shipper_name', 'domain'])
                person   = first(r, ['person_id', 'contact_id', 'user_id', 'email',
                                     'phone', 'mobilephone'])
                product  = first(r, ['product_id', 'sku', 'product_name', 'hs_code',
                                     'hscode', 'productdescription'])
                category = first(r, ['category_id', 'category', 'hs_code', 'hscode', 'industry'])

                # ── timestamp extraction ───────────────────────────────────────
                ts_raw = first(r, ['occurred_at', 'created_at', 'updated_at', 'timestamp',
                                   'event_time', 'shipment_date', 'date', 'received_at',
                                   'modified_on', 'created_on'])
                if ts_raw is None:
                    warn(f'{key}: row {rid} has no timestamp column — defaulting to utc_now(). '
                         'Signal timing for this record will be inaccurate.')
                # Always emit strict ISO-8601 so Neo4j datetime() can parse it.
                # Python datetime objects stringify as '2026-06-24 12:00:00' (no T);
                # we use .isoformat() to get '2026-06-24T12:00:00'.
                if ts_raw is None:
                    occurred_at = utc_now()
                elif hasattr(ts_raw, 'isoformat'):
                    occurred_at = ts_raw.isoformat()
                else:
                    # Replace space separator with T and strip any trailing fractional/tz noise
                    occurred_at = str(ts_raw).replace(' ', 'T')

                # ── payload size guard ─────────────────────────────────────────
                payload_full = json.dumps(r, default=str, ensure_ascii=False)
                if len(payload_full) > _PAYLOAD_LIMIT:
                    warn(f'{key}: row {rid} payload is {len(payload_full)} chars — '
                         f'truncating to {_PAYLOAD_LIMIT}. Last {len(payload_full)-_PAYLOAD_LIMIT} '
                         'chars of data will not be stored in Neo4j.')
                payload = payload_full[:_PAYLOAD_LIMIT]

                conf = (0.90 if sf == 'TradeShipmentSource'
                        else 0.85 if sf == 'EnrichmentSource'
                        else 0.70)

                prepared.append({
                    'source_key':          key,
                    'raw_uid':             raw_uid,
                    'rid':                 rid,
                    'payload':             payload,
                    'sig_uid':             sig_uid,
                    'st':                  st,
                    'sf':                  sf,
                    'occurred_at':         occurred_at,
                    'conf':                conf,
                    'account_hint':        str(account)  if account  is not None else None,
                    'person_hint':         str(person)   if person   is not None else None,
                    'product_hint':        str(product)  if product  is not None else None,
                    'category_hint':       str(category) if category is not None else None,
                    'hs_code':             str(first(r, ['hs_code', 'hscode', 'matched_hs_code']) or '') or None,
                    'shipment_value':      to_float(first(r, ['shipment_value', 'shipmentvalue',
                                                              'total_value', 'trade_value', 'amount'])),
                    'number_of_shipments': to_float(first(r, ['number_of_shipments', 'totalshipments',
                                                              'shipment_count', 'shipments'])),
                })

            self.neo.run('''
UNWIND $rows AS row
MERGE (raw:RawSource {raw_source_uid:row.raw_uid})
SET raw:SourceRecord,
    raw.source_record_uid=row.raw_uid,
    raw.source_key=row.source_key,
    raw.source_record_id=row.rid,
    raw.payload_json=row.payload,
    raw.payload_ref=row.payload,
    raw.source_table=row.source_key,
    raw.source_system='postgres',
    raw.occurred_at=row.occurred_at,
    raw.confidence_score=row.conf
MERGE (sig:Signal {signal_uid:row.sig_uid})
SET sig.signalId=row.sig_uid,
    sig.source_key=row.source_key,
    sig.signal_type=row.st,
    sig.signalType=row.st,
    sig.source_family=row.sf,
    sig.sourceFamily=row.sf,
    sig.occurred_at=row.occurred_at,
    sig.confidence_score=row.conf,
    sig.intensity_score=CASE WHEN row.st CONTAINS 'shipment' OR row.st CONTAINS 'rfq' THEN 1.0 ELSE 0.6 END,
    sig.account_hint=row.account_hint,
    sig.person_hint=row.person_hint,
    sig.product_hint=row.product_hint,
    sig.category_hint=row.category_hint,
    sig.hs_code=row.hs_code,
    sig.shipment_value=row.shipment_value,
    sig.number_of_shipments=row.number_of_shipments,
    sig.source='postgres',
    sig.sourceTable=row.source_key,
    sig.v25_stage=coalesce(sig.v25_stage,'signal_loaded')
MERGE (reg:SourceRegistry {source_key:row.source_key})
MERGE (sf:SourceFamily {name:row.sf})
MERGE (typ:SignalType {name:row.st})
MERGE (raw)-[:RECORDED_IN_SOURCE]->(reg)
MERGE (raw)-[:NORMALIZES_TO]->(sig)
MERGE (sig)-[:FROM_SOURCE]->(reg)
MERGE (sig)-[:BELONGS_TO]->(sf)
MERGE (sig)-[:HAS_TYPE]->(typ)
''', {'rows': prepared})

            total += len(prepared)
            info(f'{key}: {st}, signals={len(prepared)}')

        ok(f'Postgres loader complete, signals={total}')
