# Rollback and recovery

My general rule: flip a flag first (seconds), revert code second (minutes), repair data last. Flags live in AppConfig per Connect instance and per phase.

## What can go wrong

**ServiceNow is down or slow at call end.** I'd see it in the ServiceNow error rate, the age of the oldest queued message, and the DLQ. There's nothing to do while it recovers because the queue keeps retrying. If messages sit long enough to hit the DLQ, I redrive them once ServiceNow is back, which is safe because the writer is idempotent.

**Duplicate tickets show up.** A daily check counts ServiceNow records per `u_connect_contact_id`. If any key has more than one, I turn off writes, find the cause, and merge or close the extras with a script (keeping the earliest).

**A mapping bug puts wrong fields or the wrong assignment group on tickets.** I'd catch this through validation errors, agent reports, or the shadow-mode diff. Turn off writes, fix the mapping, then run a repair script over the affected time window.

**Jira and ServiceNow start looping.** The per-link sync-count alarm and a spike in API calls would tell me. I'd set `jira_sync_enabled=false`, pause the queue's event source mapping, look at what's in the DLQ before purging the FIFO queue, then reconcile.

**Half a sync goes through (ServiceNow updated, Jira not).** The reconciliation drift metric shows it. The reconciler re-queues anything where the stored hash doesn't match, so it usually heals on its own.

**The context Lambda is slow or failing.** Flow error rate, p99 latency and the fallback rate would show it. Set `context_enrichment_enabled=false` so it returns defaults, or repoint the flow to its previous version.

**Stale cache routes a high-tier customer wrongly.** Spot checks of cached tier against ServiceNow would show it. Flush that part of the cache, shorten the freshness window if needed, and let it re-warm.

**A bad deploy.** The canary alarm triggers an automatic Lambda alias rollback. For infrastructure I re-apply the previous tagged plan.

**Credentials expire or get rotated wrong.** A jump in 401s and 403s. Restore the previous secret version.

## Rollback steps by phase

**Phase 1**
1. Set `servicenow_write_enabled=false` for the affected instance. Ingest keeps recording CTRs, so nothing is lost.
2. Decide whether that team needs to log manually during the gap or can wait for the backfill.
3. After the fix, replay from the point before the incident (reset the Kinesis position or redrive the DLQ). Completed items are skipped.
4. For records created wrongly, query by the unique field, the service account and the time window, then fix or delete with a script that does a dry run first.

**Phase 2**
1. Set `context_enrichment_enabled=false` and the flow routes on defaults immediately.
2. If needed, repoint the number to the previous flow version (I'd keep old versions for at least 30 days).
3. Repair or flush the cache once the cause is known.

**Phase 3**
1. Set `jira_sync_enabled=false`, disable the ServiceNow business rule via its property, and disable the Jira webhook.
2. Pause queue processing without purging until I've looked at what's in it.
3. Run the reconciler in report-only mode to list what diverged, fix it according to the field ownership rules, then re-enable for one link, then one project.
4. Links created during the incident stay, since they're correct. Only bad field values get repaired.

## Partially synced data

I prefer rolling forward over deleting. Anything in flight is in a queue (safe), in a DLQ (visible), or in the link table with a stale hash (the reconciler picks it up). Repair scripts default to dry-run, log every change, and can be run twice safely. I'd also keep an audit stream of every write (who, what, before, after), because targeted repair needs it.

For infrastructure, applies come from a saved plan on a tagged commit, so reverting means re-applying the previous tag. Data stores have `prevent_destroy` and point-in-time recovery, and destructive changes need explicit approval.
