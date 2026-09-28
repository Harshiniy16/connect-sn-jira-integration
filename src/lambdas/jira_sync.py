"""Loop-safe, conflict-aware SN <-> Jira sync (SQS FIFO consumer, group = link_id)."""
import hashlib, json, logging, os
import boto3
from botocore.exceptions import ClientError

log = logging.getLogger(); log.setLevel(logging.INFO)
links = boto3.resource("dynamodb").Table(os.environ["LINK_TABLE"])
SERVICE_ACCOUNTS = {"svc-cc-integration"}           # same account name in SN and Jira
SN_OWNED = {"priority", "customer_state", "sla"}
JIRA_OWNED = {"eng_status", "resolution", "fix_version", "assignee"}


def state_hash(fields):
    return hashlib.sha256(json.dumps(fields, sort_keys=True).encode()).hexdigest()


def synced_subset(evt):
    owned = SN_OWNED if evt["source"] == "servicenow" else JIRA_OWNED
    return {k: v for k, v in evt["fields"].items() if k in owned}


def handle(evt, sn, jira):
    if evt["actor"] in SERVICE_ACCOUNTS:             # loop guard 1: our own write
        return "skip:self"
    link = links.get_item(Key={"link_id": evt["link_id"]}).get("Item")
    subset = synced_subset(evt)
    h = state_hash(subset)
    if link and link.get("last_hash", {}).get(evt["source"]) == h:   # loop guard 2: echo
        return "skip:echo"

    if evt["source"] == "servicenow":
        if not link:
            return create_jira_and_link(evt, sn, jira)               # idempotent (see below)
        jira.update(link["jira_key"], subset, origin="cc-sync")      # loop guard 3: tag
    else:
        if not link:
            return "skip:unlinked"
        sn_updates = {"work_notes": f"Jira {link['jira_key']}: {subset}",
                      **{f"u_jira_{k}": v for k, v in subset.items()}}
        if subset.get("eng_status") == "Resolved":
            sn_updates["state"] = "resolved_pending_customer"        # don't close: SN owns that
        sn.update(link["sys_id"], sn_updates, origin="cc-sync")

    persist(evt, link, h)
    return "synced"


def create_jira_and_link(evt, sn, jira):
    # Crash-safe: search first so a crash between 'Jira created' and 'link saved' can't duplicate.
    existing = jira.search(f'"SN Number" = "{evt["sn_number"]}"')
    key = existing[0]["key"] if existing else jira.create(
        {"summary": evt["fields"]["short_description"], "SN Number": evt["sn_number"]},
        origin="cc-sync")["key"]
    try:
        links.put_item(Item={"link_id": evt["link_id"], "sys_id": evt["sn_sys_id"],
                             "jira_key": key, "version": 1, "last_hash": {}},
                       ConditionExpression="attribute_not_exists(link_id)")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
    sn.update(evt["sn_sys_id"], {"u_jira_key": key}, origin="cc-sync")
    return "created"


def persist(evt, link, h):
    """Optimistic locking so concurrent workers can't clobber each other's hashes."""
    try:
        links.update_item(
            Key={"link_id": evt["link_id"]},
            UpdateExpression="SET last_hash.#s = :h, version = version + :one",
            ConditionExpression="version = :v",
            ExpressionAttributeNames={"#s": evt["source"]},
            ExpressionAttributeValues={":h": h, ":one": 1, ":v": link["version"]})
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            raise RuntimeError("version conflict -> requeue")    # SQS retries with fresh state
        raise
