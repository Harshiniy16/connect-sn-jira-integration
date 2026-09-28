"""Claim logic for the idempotency table, kept separate from DynamoDB so it's easy to test.

One record per conversation, keyed on InitialContactId. It holds a status (IN_PROGRESS or
COMPLETED), a lease expiry, and the set of CTR segments we've accepted.

claim() returns:
  NEW        first CTR for this conversation, create the ticket
  SEGMENT    new CTR for a known conversation (a transfer), update the ticket
  RECLAIMED  we've seen this CTR but the earlier worker's lease ran out before it finished
             (probably crashed), so re-enqueue it. The writer is idempotent, so that's safe.
  DUPLICATE  seen before and already COMPLETED, drop it
and raises InFlight if the lease is still valid. The caller reports the record as failed so
Kinesis redelivers it later, by which point it's either completed or reclaimable.
"""
import time

LEASE_S = 120


class InFlight(Exception):
    pass


def claim(store, key, contact_id, now=None, lease_s=LEASE_S):
    now = int(now if now is not None else time.time())
    for _ in range(2):                       # 2nd pass covers a TTL-delete race
        if store.put_if_absent({
                "interaction_key": key, "status": "IN_PROGRESS",
                "lease_expires_at": now + lease_s,
                "seen_contact_ids": {contact_id}, "expires_at": now + 30 * 86400}):
            return "NEW"
        item = store.get(key)
        if item is None:
            continue
        if contact_id not in item.get("seen_contact_ids", set()):
            if store.add_segment(key, contact_id, now + lease_s):
                return "SEGMENT"
            raise InFlight(key)              # lost a race with another worker; retry later
        if item["status"] == "COMPLETED":
            return "DUPLICATE"
        if item["lease_expires_at"] < now:   # lease expired: take over (optimistic, one winner)
            if store.reclaim(key, item["lease_expires_at"], now + lease_s):
                return "RECLAIMED"
        raise InFlight(key)
    raise InFlight(key)


class DynamoStore:
    """Thin DynamoDB adapter. The claim logic above is what the unit tests cover, using
    tests/fakes.InMemoryStore, which follows the same rules. This class itself hasn't been run
    against real DynamoDB; testing it with moto or localstack would be the next step."""

    def __init__(self, table):
        self.t = table

    @staticmethod
    def _cond_failed(e):
        return e.response["Error"]["Code"] == "ConditionalCheckFailedException"

    def put_if_absent(self, item):
        from botocore.exceptions import ClientError
        try:
            self.t.put_item(Item=item, ConditionExpression="attribute_not_exists(interaction_key)")
            return True
        except ClientError as e:
            if self._cond_failed(e):
                return False
            raise

    def get(self, key):
        return self.t.get_item(Key={"interaction_key": key}, ConsistentRead=True).get("Item")

    def add_segment(self, key, contact_id, lease_until):
        from botocore.exceptions import ClientError
        try:
            self.t.update_item(
                Key={"interaction_key": key},
                UpdateExpression="SET #s = :ip, lease_expires_at = :l ADD seen_contact_ids :c",
                ConditionExpression="attribute_exists(interaction_key) "
                                    "AND NOT contains(seen_contact_ids, :cid)",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":ip": "IN_PROGRESS", ":l": lease_until,
                                           ":c": {contact_id}, ":cid": contact_id})
            return True
        except ClientError as e:
            if self._cond_failed(e):
                return False
            raise

    def reclaim(self, key, old_lease, new_lease):
        from botocore.exceptions import ClientError
        try:
            self.t.update_item(
                Key={"interaction_key": key},
                UpdateExpression="SET lease_expires_at = :new",
                ConditionExpression="#s = :ip AND lease_expires_at = :old",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":ip": "IN_PROGRESS", ":old": old_lease,
                                           ":new": new_lease})
            return True
        except ClientError as e:
            if self._cond_failed(e):
                return False
            raise

    def complete(self, key, sys_id):
        self.t.update_item(
            Key={"interaction_key": key},
            UpdateExpression="SET #s = :c, sn_sys_id = :id REMOVE lease_expires_at",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":c": "COMPLETED", ":id": sys_id})
