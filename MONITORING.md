# Monitoring

What I mean by "working": every finished call has exactly one ServiceNow interaction within about five minutes, the context lookup answers within its budget, and linked Jira and ServiceNow tickets agree.

## Metrics and alarms

**Phase 1**
- Coverage: CTRs ingested versus interactions created, per instance. Page if it drops below 99.5% over 15 minutes.
- Lag from CTR timestamp to ServiceNow record (p95). Warn over 5 minutes, page over 15.
- Kinesis iterator age (warn over 5 minutes) and age of the oldest SQS message (over 15 minutes).
- DLQ depth. Ticket for anything above 0, page above 10.
- Duplicate detector: any contact ID with more than one record pages someone.
- Lambda errors and throttles above about 2%.

**Phase 2**
- `customer_context` latency, p99 above 1.5 seconds.
- Cache hit ratio (warn under 70%).
- Fallback-to-default rate, above 5% for 10 minutes.
- Circuit breaker open: warn, and page if it stays open past 10 minutes.

**Phase 3**
- Sync lag between the systems (p95 over 5 minutes).
- Loop guard: more than 5 syncs on one link in 5 minutes pages and auto-disables sync.
- FIFO DLQ depth above 0.
- Reconciliation drift above 0 for two cycles in a row.
- Conflict count, watched as a trend.

**Downstream and general**
- ServiceNow and Jira 4xx/5xx/429 rates and latency (5xx above 5%).
- Bursts of 401/403 or webhook signature failures.
- Lambda invocations and API calls versus baseline, to catch runaway cost.

## Logging

Logs are structured JSON with a correlation ID (the `InitialContactId` or link ID), the instance, the phase, the attempt number, what was decided (`create`, `update`, `skip:duplicate`, `skip:echo`, `fallback`), latency, and the sys_id or Jira key. I'd use Lambda Powertools for logging and custom metrics, and X-Ray to trace a call through Lambda, SQS and ServiceNow. Secrets, full phone numbers (last four only) and ticket text stay out of logs. Thirty days of hot retention, a year of audit records in S3.

## Synthetic checks

Error-rate alarms miss the case where nothing is happening, so I'd run a canary call to a test number every five minutes and check that exactly one interaction appears with the expected attributes. A daily report compares Connect CTR counts to ServiceNow interactions per team, and Jira/ServiceNow link parity.

## Dashboards and alert routing

I'd build three: an overview for managers (calls today, share auto-logged, estimated minutes saved at 2 to 3 per call, open DLQ items), an operations view (lag, queue depths, errors, breaker state, current flags), and one per team for coverage and fallback rate.

Anything that means lost data or customer impact pages on-call: coverage, DLQ over 10, the loop guard, fallback spikes. Lag and warning-level alarms go to Slack or a ticket. Each alarm links to the matching section in ROLLBACK.md. The canary alarms are also attached to Lambda alias deployments and AppConfig rollouts so a bad release rolls itself back.
