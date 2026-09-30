import threading


class InMemoryStore:
    """Same semantics as lib.idempotency.DynamoStore (atomic conditional writes via a lock)."""
    def __init__(self):
        self.items, self.lock = {}, threading.Lock()

    def put_if_absent(self, item):
        with self.lock:
            if item["interaction_key"] in self.items:
                return False
            self.items[item["interaction_key"]] = dict(item); return True

    def get(self, key):
        with self.lock:
            it = self.items.get(key)
            return {**it, "seen_contact_ids": set(it["seen_contact_ids"])} if it else None

    def add_segment(self, key, cid, lease_until):
        with self.lock:
            it = self.items.get(key)
            if not it or cid in it["seen_contact_ids"]:
                return False
            it["seen_contact_ids"].add(cid); it["status"] = "IN_PROGRESS"
            it["lease_expires_at"] = lease_until; return True

    def reclaim(self, key, old, new):
        with self.lock:
            it = self.items[key]
            if it["status"] == "IN_PROGRESS" and it.get("lease_expires_at") == old:
                it["lease_expires_at"] = new; return True
            return False

    def complete(self, key, sys_id):
        with self.lock:
            it = self.items[key]; it["status"] = "COMPLETED"; it["sn_sys_id"] = sys_id
            it.pop("lease_expires_at", None)


class FakeResp:
    def __init__(self, status, body=None):
        self.status_code, self._b, self.text, self.headers = status, body or {}, "", {}
    def json(self): return self._b


class FakeServiceNow:
    """requests.Session stand-in: a table with a UNIQUE u_connect_contact_id constraint.
    fail_after_commit=N makes the first N POSTs commit and then raise a timeout."""
    def __init__(self, timeout_exc, fail_after_commit=0):
        self.rows, self.lock, self.timeout_exc = [], threading.Lock(), timeout_exc
        self.fail_after_commit, self.posts, self.patches = fail_after_commit, 0, []

    def request(self, method, url, **kw):
        with self.lock:
            if method == "GET":
                q = kw["params"]["sysparm_query"].split("=", 1)[1]
                return FakeResp(200, {"result": [r for r in self.rows if r["u_connect_contact_id"] == q][:1]})
            if method == "POST":
                self.posts += 1
                body = kw["json"]
                if any(r["u_connect_contact_id"] == body["u_connect_contact_id"] for r in self.rows):
                    resp = FakeResp(400); resp.text = "unique constraint violated"; return resp
                self.rows.append({**body, "sys_id": f"sys{len(self.rows)+1}"})
                if self.fail_after_commit > 0:
                    self.fail_after_commit -= 1
                    raise self.timeout_exc("committed, then response lost")
                return FakeResp(200, {"result": {"sys_id": self.rows[-1]["sys_id"]}})
            if method == "PATCH":
                self.patches.append(kw["json"]); return FakeResp(200, {})
