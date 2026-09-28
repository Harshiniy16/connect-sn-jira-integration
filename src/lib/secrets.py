import json, os, time, boto3
_sm = boto3.client("secretsmanager"); _cache = {}
def get_token(force=False):
    n = os.environ["SN_SECRET_ID"]
    if force or n not in _cache or _cache[n][1] < time.time():
        v = json.loads(_sm.get_secret_value(SecretId=n)["SecretString"])["token"]
        _cache[n] = (v, time.time() + 300)
    return _cache[n][0]
