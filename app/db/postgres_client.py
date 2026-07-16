import psycopg2, psycopg2.extras
class PostgresClient:
    def __init__(self, host, port, user, password, database):
        self.conn=psycopg2.connect(host=host, port=port, user=user, password=password, dbname=database, connect_timeout=20, options='-c statement_timeout=180000')
        self.conn.autocommit=True
    def close(self): self.conn.close()
    def q(self, sql, params=None):
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or ()); return [dict(r) for r in cur.fetchall()]
    def columns(self,schema,table): return [r['column_name'] for r in self.q('SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position',(schema,table))]
    def list_tables(self,schemas):
        out=[]
        for schema in schemas: out += self.q("SELECT table_schema AS schema, table_name AS table FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema=%s ORDER BY table_name",(schema,))
        return out
