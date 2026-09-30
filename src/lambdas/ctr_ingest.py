"""Kinesis -> claim idempotency -> SQS. Phase 1 entry point."""
import base64, json, logging, os
import boto3
from lib.idempotency import DynamoStore, InFlight, claim

log = logging.getLogger(); log.setLevel(logging.INFO)
ddb = boto3.resource("dynamodb").Table(os.environ["IDEMPOTENCY_TABLE"])
store = DynamoStore(ddb)
sqs = boto3.client("sqs")
QUEUE = os.environ["SN_WRITE_QUEUE_URL"]


def normalise(ctr):
    return {
        "interaction_key": ctr.get("InitialContactId") or ctr["ContactId"],
        "contact_id": ctr["ContactId"],
        "previous_contact_id": ctr.get("PreviousContactId"),
        "instance_arn": ctr.get("InstanceARN"),
        "channel": ctr.get("Channel"),
        "initiation": ctr.get("InitiationMethod"),
        "queue": (ctr.get("Queue") or {}).get("Name"),
        "agent": (ctr.get("Agent") or {}).get("Username"),
        "customer_endpoint": (ctr.get("CustomerEndpoint") or {}).get("Address"),
        "disconnect": ctr.get("DisconnectTimestamp"),
        "attributes": ctr.get("Attributes") or {},
    }


def handler(event, context):
    failures = []
    for rec in event["Records"]:
        seq = rec["kinesis"]["sequenceNumber"]
        try:
            ctr = json.loads(base64.b64decode(rec["kinesis"]["data"]))
            msg = normalise(ctr)
            decision = claim(store, msg["interaction_key"], msg["contact_id"])
            log.info(json.dumps({"event": "ctr", "decision": decision,
                                 "correlation_id": msg["interaction_key"],
                                 "contact_id": msg["contact_id"]}))
            if decision == "DUPLICATE":
                continue
            sqs.send_message(QueueUrl=QUEUE, MessageBody=json.dumps(msg))
        except InFlight:
            # Lease still valid: report failure so Kinesis redelivers after it is
            # COMPLETED (-> dropped) or expired (-> reclaimed and re-enqueued).
            log.info(json.dumps({"event": "ctr", "decision": "IN_FLIGHT_RETRY"}))
            failures.append({"itemIdentifier": seq})
        except Exception:
            log.exception("ingest failed")
            failures.append({"itemIdentifier": seq})   # partial batch response: retry only these
    return {"batchItemFailures": failures}
