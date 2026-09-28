"""Entry-time exit rules: historical positions retain their original contract."""
import json
ANOMALY_VERSION = 'anomaly-stop7-protect8-trail1-v1'
def new_policy(kind, stop, cost):
    anomaly = 'аномаль' in kind and 'лидер' in kind
    return dict(version=ANOMALY_VERSION if anomaly else 'rocket-legacy-v1',
                stop_percent=7.0 if anomaly else stop,
                protect_percent=8.0 if anomaly else 1.0, trail_pp=1.0,
                cost_percent=cost)
def policy_for(row, default_stop=1.0):
    data=dict(row)
    return json.loads(data['exit_policy_json']) if data.get('exit_policy_json') else dict(
        version='legacy-unversioned',stop_percent=default_stop,protect_percent=1.0,trail_pp=1.0)
