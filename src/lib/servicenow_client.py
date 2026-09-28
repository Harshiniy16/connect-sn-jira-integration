"""ServiceNow REST client: timeouts, retries with jittered backoff, circuit breaker,
idempotent create (lookup-before-create) keyed on u_connect_contact_id."""
import json, logging, random, time
import requests

log = logging.getLogger(__name__)
RETRYABLE = {429, 500, 502, 503, 504}


class ServiceNowError(Exception): ...
class TransientError(ServiceNowError): ...      # retry later (SQS will redeliver)
class PermanentError(ServiceNowError): ...      # 4xx: send to DLQ, retry can't help
class CircuitOpen(TransientError): ...


class CircuitBreaker:
    """In-memory per container; state shared via DynamoDB in prod (omitted for brevity)."""
    def __init__(self, threshold=5, window=60, cooldown=30):
        self.threshold, self.window, self.cooldown = threshold, window, cooldown
        self.failures, self.opened_at = [], None

    def allow(self):
        if self.opened_at and time.time() - self.opened_at < self.cooldown:
            return False
        return True                              # closed or half-open probe

    def record(self, ok):
        now = time.time()
        if ok:
            self.failures, self.opened_at = [], None
            return
        self.failures = [t for t in self.failures if now - t < self.window] + [now]
        if len(self.failures) >= self.threshold:
            self.opened_at = now
            log.error(json.dumps({"event": "circuit_open", "target": "servicenow"}))


class ServiceNowClient:
    def __init__(self, base_url, token_provider, table="interaction", timeout=(2, 8)):
        self.base, self.table, self.timeout = base_url.rstrip("/"), table, timeout
        self.token_provider = token_provider     # callable -> bearer token (from Secrets Manager)
        self.session = requests.Session()
        self.breaker = CircuitBreaker()

    def _request(self, method, path, max_attempts=4, **kw):
        if not self.breaker.allow():
            raise CircuitOpen("servicenow circuit open")
        for attempt in range(1, max_attempts + 1):
            retry_after = None
            try:
                r = self.session.request(
                    method, f"{self.base}{path}", timeout=self.timeout,
                    headers={"Authorization": f"Bearer {self.token_provider()}",
                             "Accept": "application/json"}, **kw)
            except (requests.Timeout, requests.ConnectionError) as e:
                self.breaker.record(False)
                err = TransientError(f"network: {e}")
            else:
                if r.status_code < 400:
                    self.breaker.record(True)
                    return r
                if r.status_code == 401:                     # token rotated? refresh once
                    self.token_provider(force=True)
                if r.status_code not in RETRYABLE:
                    raise PermanentError(f"{r.status_code}: {r.text[:200]}")
                self.breaker.record(False)
                err = TransientError(f"{r.status_code}")
                retry_after = r.headers.get("Retry-After")
            if attempt == max_attempts:
                raise err
            delay = min(8, 0.5 * 2 ** attempt) * random.uniform(0.5, 1.5)   # jitter
            if retry_after and retry_after.isdigit():
                delay = max(delay, int(retry_after))
            log.warning(json.dumps({"event": "sn_retry", "attempt": attempt,
                                    "delay": round(delay, 2), "error": str(err)}))
            time.sleep(delay)

    # idempotent operations
    def find_by_contact_id(self, key):
        r = self._request("GET", f"/api/now/table/{self.table}",
                          params={"sysparm_query": f"u_connect_contact_id={key}",
                                  "sysparm_limit": 1, "sysparm_fields": "sys_id,number"})
        rows = r.json()["result"]
        return rows[0] if rows else None

    def create_or_get(self, key, payload):
        """Safe to call repeatedly: returns (sys_id, created?)."""
        existing = self.find_by_contact_id(key)          # covers 'timeout after commit'
        if existing:
            return existing["sys_id"], False
        body = {**payload, "u_connect_contact_id": key}
        try:
            r = self._request("POST", f"/api/now/table/{self.table}", json=body,
                              max_attempts=1)            # ambiguous outcome: don't blind-retry POST
        except TransientError:
            existing = self.find_by_contact_id(key)      # did it land despite the error?
            if existing:
                return existing["sys_id"], False
            raise
        except PermanentError as e:
            if "unique" in str(e).lower():               # unique index on u_connect_contact_id fired
                return self.find_by_contact_id(key)["sys_id"], False
            raise
        return r.json()["result"]["sys_id"], True

    def append_segment(self, sys_id, segment_note, totals):
        self._request("PATCH", f"/api/now/table/{self.table}/{sys_id}",
                      json={"work_notes": segment_note, **totals})

    def lookup_customer_context(self, phone):
        """Phase 2 lookup (single fast call). Field names are illustrative."""
        r = self._request("GET", "/api/x_cc/customer_context",
                          params={"phone": phone}, max_attempts=1)
        d = r.json().get("result")
        if not d:
            return None
        return {"sla_tier": str(d["sla_tier"]), "open_ticket_count": str(d["open_tickets"]),
                "account_health": str(d["health"])}
