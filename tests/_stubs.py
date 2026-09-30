"""Lets the unit tests run offline with only the standard library: if boto3/botocore/requests
aren't installed, install minimal stand-ins. If the real packages are present they're used and
these stubs are skipped."""
import os, sys, types
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")  # real boto3 needs a region to build clients
for k, v in {"IDEMPOTENCY_TABLE": "t", "SN_WRITE_QUEUE_URL": "q", "LINK_TABLE": "l",
             "SN_BASE_URL": "https://sn.example", "SN_SECRET_ID": "s"}.items():
    os.environ.setdefault(k, v)

try:
    import boto3, botocore.exceptions, requests  # noqa: F401
except ImportError:
    class ClientError(Exception):
        def __init__(self, response, op=""):
            self.response = response
    b = types.ModuleType("boto3"); b.resource = mock.MagicMock(); b.client = mock.MagicMock()
    bc = types.ModuleType("botocore"); be = types.ModuleType("botocore.exceptions")
    be.ClientError = ClientError; bc.exceptions = be
    r = types.ModuleType("requests")
    class Timeout(Exception): ...
    class ConnectionError(Exception): ...
    r.Timeout, r.ConnectionError, r.Session = Timeout, ConnectionError, mock.MagicMock
    sys.modules.update({"boto3": b, "botocore": bc, "botocore.exceptions": be, "requests": r})
