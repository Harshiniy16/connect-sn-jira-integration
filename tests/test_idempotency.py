"""Run:  python -m unittest discover -s tests -v     (stdlib only; no AWS/ServiceNow needed)"""
import threading, unittest
import _stubs  # noqa: F401  (must come first)
import requests
from fakes import FakeServiceNow, InMemoryStore
from lib.idempotency import InFlight, claim
from lib.servicenow_client import ServiceNowClient


def make_client(sn):
    c = ServiceNowClient("https://sn.example", lambda force=False: "tok")
    c.session = sn
    return c


class ClaimStateMachine(unittest.TestCase):
    def test_same_ctr_delivered_five_times(self):
        s, results = InMemoryStore(), []
        results.append(claim(s, "K", "C1", now=0))            # first delivery creates
        for _ in range(4):                                      # redeliveries while owner working
            with self.assertRaises(InFlight):
                claim(s, "K", "C1", now=1)
        s.complete("K", "sys1")
        self.assertEqual(claim(s, "K", "C1", now=2), "DUPLICATE")   # after completion: dropped
        self.assertEqual(results, ["NEW"])

    def test_transfer_segment_is_an_update_not_a_create(self):
        s = InMemoryStore()
        self.assertEqual(claim(s, "K", "C1", now=0), "NEW")
        s.complete("K", "sys1")
        self.assertEqual(claim(s, "K", "C2", now=5), "SEGMENT")     # same InitialContactId
        self.assertEqual(s.get("K")["status"], "IN_PROGRESS")       # segment write now tracked
        s.complete("K", "sys1")
        self.assertEqual(claim(s, "K", "C2", now=6), "DUPLICATE")

    def test_expired_lease_is_reclaimed_after_crash(self):
        s = InMemoryStore()
        claim(s, "K", "C1", now=0, lease_s=120)                 # worker claims, then "crashes"
        with self.assertRaises(InFlight):                       # lease still valid -> wait
            claim(s, "K", "C1", now=119, lease_s=120)
        self.assertEqual(claim(s, "K", "C1", now=121, lease_s=120), "RECLAIMED")
        with self.assertRaises(InFlight):                       # new owner holds a fresh lease
            claim(s, "K", "C1", now=122, lease_s=120)

    def test_only_one_of_many_concurrent_workers_reclaims(self):
        s = InMemoryStore(); claim(s, "K", "C1", now=0)
        out, lock = [], threading.Lock()
        def worker():
            try: r = claim(s, "K", "C1", now=500)
            except InFlight: r = "IN_FLIGHT"
            with lock: out.append(r)
        ts = [threading.Thread(target=worker) for _ in range(8)]
        [t.start() for t in ts]; [t.join() for t in ts]
        self.assertEqual(out.count("RECLAIMED"), 1)


class ServiceNowIdempotency(unittest.TestCase):
    def test_concurrent_creates_produce_one_record(self):
        sn = FakeServiceNow(requests.Timeout); c = make_client(sn)
        res = []
        ts = [threading.Thread(target=lambda: res.append(c.create_or_get("K", {"short_description": "x"})))
              for _ in range(5)]
        [t.start() for t in ts]; [t.join() for t in ts]
        self.assertEqual(len(sn.rows), 1)                        # unique key held
        self.assertEqual(len({sid for sid, _ in res}), 1)        # all callers converge on same sys_id

    def test_timeout_after_commit_does_not_duplicate(self):
        sn = FakeServiceNow(requests.Timeout, fail_after_commit=1); c = make_client(sn)
        sys_id, created = c.create_or_get("K", {"short_description": "x"})
        self.assertEqual(len(sn.rows), 1)                        # committed once, adopted on recheck
        self.assertEqual(sys_id, sn.rows[0]["sys_id"])
        self.assertEqual(sn.posts, 1)                            # no blind second POST

    def test_second_segment_finds_existing_record(self):
        sn = FakeServiceNow(requests.Timeout); c = make_client(sn)
        _, created1 = c.create_or_get("K", {"short_description": "x"})
        _, created2 = c.create_or_get("K", {"short_description": "x"})
        self.assertEqual((created1, created2), (True, False))    # writer then PATCHes, not POSTs
        self.assertEqual(len(sn.rows), 1)


class JiraLoopGuards(unittest.TestCase):
    def setUp(self):
        import lambdas.jira_sync as js
        from unittest import mock
        self.js, self.mock = js, mock

    def evt(self, actor="human", fields=None):
        return {"actor": actor, "source": "jira", "link_id": "L", "fields": fields or {"eng_status": "Done"}}

    def test_own_writes_are_ignored(self):
        self.assertEqual(self.js.handle(self.evt(actor="svc-cc-integration"), None, None), "skip:self")

    def test_echo_of_last_synced_state_is_dropped(self):
        subset = {"eng_status": "Done"}
        h = self.js.state_hash(subset)
        links = self.mock.MagicMock()
        links.get_item.return_value = {"Item": {"link_id": "L", "version": 1, "sys_id": "s",
                                                "jira_key": "J-1", "last_hash": {"jira": h}}}
        with self.mock.patch.object(self.js, "links", links):
            self.assertEqual(self.js.handle(self.evt(), self.mock.MagicMock(), None), "skip:echo")


if __name__ == "__main__":
    unittest.main()
