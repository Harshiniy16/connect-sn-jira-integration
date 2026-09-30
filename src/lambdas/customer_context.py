"""Invoked synchronously from the contact flow. Hard budget; always returns something."""
import json, logging, os, time
import boto3
from lib.servicenow_client import ServiceNowClient
from lib.secrets import get_token
from lib.flags import enabled

log = logging.getLogger(); log.setLevel(logging.INFO)
cache = boto3.resource("dynamodb").Table(os.environ["CACHE_TABLE"])
lam = boto3.client("lambda")
sn = ServiceNowClient(os.environ["SN_BASE_URL"], get_token, timeout=(0.5, 1.2))

DEFAULT = {"sla_tier": "STANDARD", "open_ticket_count": "0",
           "account_health": "UNKNOWN", "context_available": "false"}
FRESH_S = 600


def refresh_async(phone):
    lam.invoke(FunctionName=os.environ["REFRESHER_FN"], InvocationType="Event",
               Payload=json.dumps({"phone": phone}))


def handler(event, context):
    started = time.time()
    phone = event["Details"]["ContactData"]["CustomerEndpoint"]["Address"]
    if not enabled("context_enrichment_enabled"):
        return DEFAULT
    try:
        item = cache.get_item(Key={"phone": phone}).get("Item")
        if item and item["fresh_until"] > time.time():
            return {**item["context"], "context_available": "true", "source": "cache"}
        if item:                                             # stale-while-revalidate
            refresh_async(phone)
            return {**item["context"], "context_available": "true", "source": "stale"}
        ctx = sn.lookup_customer_context(phone)              # single call, 1.2s cap, breaker
        if ctx:
            cache.put_item(Item={"phone": phone, "context": ctx,
                                 "fresh_until": int(time.time()) + FRESH_S,
                                 "expires_at": int(time.time()) + 86400})
            return {**ctx, "context_available": "true", "source": "live"}
    except Exception as e:                                   # ANY failure -> default routing
        log.warning(json.dumps({"event": "context_fallback", "error": str(e)[:120]}))
        try:
            refresh_async(phone)
        except Exception:
            pass
    finally:
        log.info(json.dumps({"event": "context", "ms": int((time.time() - started) * 1000)}))
    return DEFAULT          # Connect attributes must be flat string values
