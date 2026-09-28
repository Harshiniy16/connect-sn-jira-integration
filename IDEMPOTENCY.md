# Preventing duplicate tickets

## Where duplicates come from

- Kinesis or Lambda redelivers a batch after an error or timeout.
- SQS delivers a message twice, or a slow attempt outlives its visibility timeout.
- ServiceNow saves the record but the response never reaches us, so we retry and create a second one. This is the nasty one.
- A transferred call produces several CTRs for one conversation.
- Someone replays the stream or redrives the DLQ.

## Three layers

I don't want to rely on any single check, so there are three.

**1. A DynamoDB record per conversation.** The key is `InitialContactId` (or `ContactId` if there isn't one). It holds a status (`IN_PROGRESS` or `COMPLETED`), a lease expiry, the set of segment IDs we've accepted, and the ServiceNow sys_id once we have it. Records expire after 30 days.

**2. A unique field in ServiceNow.** `u_connect_contact_id` has a unique index on the interaction table. Even if layer 1 is lost or bypassed, a second insert is rejected and we treat that as "already exists, go fetch it".

**3. Look before creating.** Before any create, and again after any ambiguous failure, the writer queries ServiceNow for that key. If the record is there, we adopt its sys_id.

## The claim logic

This lives in `src/lib/idempotency.py` and has tests. When a CTR arrives:

- If no record exists, create one (atomic conditional put). Outcome: **NEW**, go create the ticket.
- If the record exists but this `ContactId` is new, it's a transferred segment. Add it, put the status back to `IN_PROGRESS` with a fresh lease. Outcome: **SEGMENT**, go update the ticket.
- If we've seen this exact `ContactId` and the record is `COMPLETED`, it's a repeat. Outcome: **DUPLICATE**, drop it.
- If we've seen it, the record is `IN_PROGRESS` and the lease has expired, the previous worker probably died. One worker takes over using a conditional update on the old lease value, so only one can win. Outcome: **RECLAIMED**, re-queue it.
- If we've seen it and the lease is still valid, someone may be mid-write. We raise `InFlight`, the Lambda reports that record as failed, and Kinesis redelivers it later. By then it's either completed (dropped) or the lease has lapsed (reclaimed).

The lease exists because the claim happens before the queue send and the ServiceNow write. Without it, a crash in between would turn the redelivered record into a "duplicate" and the call would never get a ticket. Re-queuing after a takeover is safe because the writer is idempotent on its own.

The cost is that a redelivered record can hold up a Kinesis shard for up to the lease length, 120 seconds by default. At a couple thousand calls a day that doesn't matter. At much higher volume I'd shorten the lease or move the claim into the SQS consumer.

```python
def claim(store, key, contact_id, now=None, lease_s=120):
    if store.put_if_absent({...}):
        return "NEW"
    item = store.get(key)
    if contact_id not in item["seen_contact_ids"]:
        if store.add_segment(key, contact_id, now + lease_s):
            return "SEGMENT"
        raise InFlight(key)
    if item["status"] == "COMPLETED":
        return "DUPLICATE"
    if item["lease_expires_at"] < now and store.reclaim(key, item["lease_expires_at"], now + lease_s):
        return "RECLAIMED"
    raise InFlight(key)
```

## Transfers

Same instance: one interaction, updated per segment. Each segment adds a work note with the queue, agent and duration, and the assignee follows the latest agent.

Across Connect instances: the new instance generates a new initial ID, so I create a new interaction and link it to the earlier one when the transfer flow passed along an `origin_interaction_key` attribute (or `PreviousContactId` is set). I relate them rather than merge, because the teams have different SLAs and owners.

Out of order: if segment 2 arrives before segment 1, the update path has no sys_id to update. The message is delayed and retried a few times. If the first segment never shows up, the later one creates the record itself. Because the key is unique, the late one then just becomes an update.

New call from the same customer: a fresh initial ID means a new interaction. Linking history for the agent is Phase 2's job.

Updates are written so that order and repeats don't matter: work notes are keyed by `ContactId` and totals are recomputed from the segments we've seen instead of incremented.

## Timeouts and retries

- A timeout doesn't mean failure. After one, the client doesn't send another POST. It checks whether the record landed first.
- Only 429, 5xx and network errors are retried, with exponential backoff and jitter. Other 4xx errors can't be fixed by retrying, so they go to the DLQ.
- The SQS visibility timeout is at least six times the Lambda timeout so a slow attempt isn't picked up by a second worker.

## Other edge cases

- Unknown customer: still create the interaction and flag it `needs_account_match`.
- Very short or abandoned calls: still log them and let ops filter, but never drop silently.
- DLQ redrive and stream replay are safe because of the three layers. Replaying completed items does nothing.

## Tests

Run with `python -m unittest discover -s tests -v`. They cover the same CTR arriving five times, transfer segments, lease expiry with only one of eight concurrent workers taking over, five concurrent creates producing one ServiceNow record, ServiceNow committing and then timing out, and the Jira loop guards.

The tests run the claim logic against an in-memory store that behaves like the DynamoDB adapter. They don't touch real DynamoDB. In CI I'd add moto or localstack to test the adapter, plus segment-out-of-order and full-replay cases.
