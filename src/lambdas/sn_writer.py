"""SQS -> ServiceNow. Idempotent create/update with retries; permanent errors to DLQ."""
import json, logging, os
import boto3
from lib.servicenow_client import ServiceNowClient, TransientError, PermanentError
from lib.secrets import get_token
from lib.flags import enabled
from lib.idempotency import DynamoStore

log = logging.getLogger(); log.setLevel(logging.INFO)
ddb = boto3.resource("dynamodb").Table(os.environ["IDEMPOTENCY_TABLE"])
store = DynamoStore(ddb)
sqs = boto3.client("sqs")
client = ServiceNowClient(os.environ["SN_BASE_URL"], get_token)


def to_payload(m):
    return {"short_description": f"Call via {m['queue'] or 'Connect'}",
            "type": "phone", "opened_for_phone": m["customer_endpoint"],
            "work_notes": f"[{m['contact_id']}] queue={m['queue']} agent={m['agent']}",
            "assignment_group": m["attributes"].get("assignment_group")}


def process(m):
    key = m["interaction_key"]
    if not enabled("servicenow_write_enabled", instance=m["instance_arn"]):
        log.info(json.dumps({"event": "shadow", "would_write": to_payload(m)}))
        return                                            # dark launch: log only
    sys_id, created = client.create_or_get(key, to_payload(m))
    if not created:                                       # transfer segment or replay
        client.append_segment(sys_id,
            f"[{m['contact_id']}] segment: queue={m['queue']} agent={m['agent']}", {})
    store.complete(key, sys_id)
    log.info(json.dumps({"event": "sn_write", "created": created, "sys_id": sys_id,
                         "correlation_id": key}))


def handler(event, context):
    failures = []
    for rec in event["Records"]:
        try:
            process(json.loads(rec["body"]))
        except PermanentError:
            log.exception("permanent error -> redrive to DLQ")
            failures.append({"itemIdentifier": rec["messageId"]})
        except TransientError:
            # Exponential backoff via visibility timeout; SQS redelivers.
            receives = int(rec["attributes"]["ApproximateReceiveCount"])
            sqs.change_message_visibility(
                QueueUrl=os.environ["SN_WRITE_QUEUE_URL"],
                ReceiptHandle=rec["receiptHandle"],
                VisibilityTimeout=min(3600, 30 * 2 ** receives))
            failures.append({"itemIdentifier": rec["messageId"]})
    return {"batchItemFailures": failures}
