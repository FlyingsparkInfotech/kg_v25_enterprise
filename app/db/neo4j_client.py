import time
import logging
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, TransientError

log = logging.getLogger(__name__)

_MAX_RETRIES = 3
_BACKOFF = [1, 2, 4]          # seconds between retry attempts
_SLOW_QUERY_THRESHOLD = 5.0   # seconds — queries longer than this get a warning


class Neo4jClient:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(
            uri, auth=(user, password), max_connection_pool_size=30
        )

    def close(self):
        self.driver.close()

    def run(self, q, p=None):
        p = p or {}
        last_err = None
        for attempt in range(_MAX_RETRIES):
            try:
                t0 = time.time()
                with self.driver.session() as s:
                    result = [dict(r) for r in s.run(q, p)]
                elapsed = time.time() - t0
                if elapsed > _SLOW_QUERY_THRESHOLD:
                    log.warning("Slow Neo4j query (%.1fs): %.120s", elapsed, q)
                return result
            except (ServiceUnavailable, TransientError) as e:
                last_err = e
                if attempt < _MAX_RETRIES - 1:
                    wait = _BACKOFF[attempt]
                    log.warning(
                        "Neo4j transient error (attempt %d/%d), retry in %ds: %s",
                        attempt + 1, _MAX_RETRIES, wait, e,
                    )
                    time.sleep(wait)
                # else fall through to raise below
            except Exception:
                raise
        raise last_err  # all retries exhausted

    def run_file(self, path):
        txt = open(path, encoding="utf-8").read()
        for stmt in [x.strip() for x in txt.split(";") if x.strip()]:
            self.run(stmt)
