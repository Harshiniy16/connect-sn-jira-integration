"""Feature flags from AppConfig (Lambda extension endpoint). Fails CLOSED: no flag = feature off."""
import json, os, urllib.request
def enabled(name, instance=None):
    try:
        url = (f"http://localhost:2772/applications/{os.environ['APPCONFIG_APP']}/environments/"
               f"{os.environ['APPCONFIG_ENV']}/configurations/flags")
        cfg = json.loads(urllib.request.urlopen(url, timeout=0.2).read())
        flag = cfg.get(name, {})
        return flag.get("instances", {}).get(instance, flag.get("enabled", False))
    except Exception:
        return False
