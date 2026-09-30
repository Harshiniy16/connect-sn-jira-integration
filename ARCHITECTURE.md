# Architecture

## Phase 1: turning call-end events into tickets

Each Connect instance streams its contact trace records (CTRs) to a Kinesis stream. A Lambda (`ctr_ingest`) reads them, records the call in DynamoDB so I can spot repeats, and drops a job on an SQS queue. A second Lambda (`sn_writer`) picks jobs up and talks to ServiceNow.

I went with CTRs over Connect's real-time contact events because the CTR is the finished record: agent, queue, disposition, transfer chain. It shows up a little after the call ends, which is fine for logging. If someone later wants a screen pop for the agent, that's the point to add the event stream. The bigger reason for Kinesis is replay. If ServiceNow or my own code is broken for six hours, I rewind and reprocess instead of losing calls.

The queue in the middle matters because Kinesis retries a whole batch and stalls the shard when one record fails. SQS retries one message at a time and has a DLQ. I also cap `sn_writer` concurrency at around 5 so a burst can't hammer ServiceNow.

**Transfers.** One customer conversation gets one interaction. `InitialContactId` is the same across every segment of a transferred call, so I use it as the key. The first CTR creates the record and later ones add a work note and update the current queue and agent. If a call moves to a different Connect instance the initial ID changes, so I create a new interaction and relate it to the earlier one rather than merging, since the two teams have different SLAs and owners. The details are in IDEMPOTENCY.md.

**ServiceNow down when a call ends.** Nothing needs to happen in real time, so messages just wait in the queue and retry with growing delays for a few hours before landing in the DLQ. The DLQ has an alarm. Since the source is Kinesis, anything under a week old can also be replayed.

```python
for record in event["Records"]:
    ctr = json.loads(base64.b64decode(record["kinesis"]["data"]))
    try:
        decision = claim(store, key, ctr["ContactId"])  # NEW, SEGMENT, RECLAIMED or DUPLICATE
    except InFlight:
        failures.append(record)   # someone holds the lease, let Kinesis redeliver later
        continue
    if decision != "DUPLICATE":
        sqs.send_message(QueueUrl=Q, MessageBody=json.dumps(normalise(ctr)))
```

## Phase 2: routing with customer context

The contact flow calls a Lambda (`customer_context`) synchronously. I give it about 2 seconds in total, though it should normally answer in tens of milliseconds because it reads from a cache.

The order of attempts:

1. Fresh cache entry (under 10 minutes old): return it.
2. Stale entry (up to 24 hours old): return it right away and kick off an async refresh.
3. No entry: one ServiceNow call with a 1.2 second timeout. If it works, cache and return it.
4. Anything fails: return a default (`STANDARD` tier, `context_available=false`) and refresh in the background.

The flow branches on those attributes and has a sensible default path when context is missing. Its error branch also falls through to default routing, so even if my Lambda were down, calls still route.

For caching I key on the normalized phone number and store the routing fields with a short freshness window and a longer hard TTL. I'd also have a ServiceNow business rule push changes (SLA tier, account health) to a webhook so the cache is mostly warm before anyone calls. At this volume DynamoDB is fast enough that I wouldn't add DAX or ElastiCache.

If ServiceNow is slow or down, a circuit breaker stops the live lookups after a handful of failures and the Lambda serves stale data or defaults until a probe succeeds. There's also a flag that makes the Lambda return defaults immediately, which is my kill switch.

I only put coarse values in contact attributes (tier, ticket count, a health band). They end up in the CTR, so I don't want ticket text or extra personal data in there.

## Phase 3: Jira and ServiceNow sync

Webhooks from both systems go through API Gateway (signature checked) into a FIFO SQS queue, grouped by the ticket link so events for one ticket are handled one at a time. The `jira_sync` Lambda keeps a link table in DynamoDB: the ServiceNow sys_id, the Jira key, a hash of the last synced state, and a version number.

**ServiceNow to Jira.** When a ticket is marked technical, the Lambda creates the Jira issue, stores the ServiceNow number on it, and writes the Jira key back to ServiceNow. To survive a crash between "Jira issue created" and "link saved", it searches Jira for the ServiceNow number before creating anything.

**Avoiding loops.** I use three checks, any one of which would probably be enough:
- Both systems get writes from a dedicated service account, and I drop any event whose actor is that account.
- I hash the fields I sync. If an incoming event matches the last hash I stored, it's an echo of my own write and I drop it.
- Writes carry an origin tag so they never trigger another outbound sync.

On top of that there's an alarm if any single link syncs more than a few times in five minutes.

**Conflicts.** The simplest way I know to handle conflicts is to not have them, so each field has one owner. Jira owns engineering status, resolution and fix version. ServiceNow owns priority, SLA and the customer-facing state. Comments are append-only on both sides and deduped by ID. For anything genuinely shared I use last-writer-wins on the source system's timestamp and note the losing value in a work note so nothing disappears quietly. When Jira resolves an issue I move the ServiceNow ticket to "resolved, pending customer" rather than closing it, since closing is the service desk's call.

**Failures.** Handlers are safe to run twice. A failed event goes back on the queue with backoff, and after five tries it lands in the FIFO DLQ with an alarm. A stuck message only blocks its own ticket. Every 15 minutes a reconciliation job compares recently changed linked tickets on both sides against the link table and re-queues anything that drifted, which covers webhooks that never arrived.

## Trade-offs I'd expect to talk through

- Only Phase 2 is synchronous, and only because the flow needs an answer. Everything else is async for resilience.
- A 10 minute cache is fine for routing. Agents still see live data in ServiceNow itself.
- Kinesis buys replay and ordering but adds a little latency compared to EventBridge.
- FIFO SQS costs throughput I don't need in return for per-ticket ordering I do need.
- Webhooks give low latency and reconciliation gives correctness. I wouldn't trust either alone.
- If volume grew a hundredfold I'd add shards and switch from per-record REST calls to ServiceNow's Import Set API.
