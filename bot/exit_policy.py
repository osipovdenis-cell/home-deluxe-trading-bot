"""Versioned exit rules; closed positions retain their historical contract."""
import json
LEADER_STEPS_VERSION = 'leader-lock1-lock2-trail1-v1'
ANOMALY_LEGACY_VERSION = 'anomaly-stop7-protect8-trail1-v1'
ANOMALY_VERSION = 'anomaly-stop7-activate8-trail1-v2'
ANOMALY_VERSIONS = (ANOMALY_LEGACY_VERSION, ANOMALY_VERSION)
def new_policy(kind, stop, cost):
    anomaly = 'аномаль' in kind and 'лидер' in kind
    leader = 'лидер' in kind and not anomaly
    return dict(version=ANOMALY_VERSION if anomaly else LEADER_STEPS_VERSION if leader else 'rocket-legacy-v1',
                protect_steps=[1.0, 2.0] if leader else [],
                stop_percent=7.0 if anomaly else stop,
                protect_percent=8.0 if anomaly else 1.0, trail_pp=1.0,
                cost_percent=cost)
def policy_for(row, default_stop=1.0):
    data=dict(row)
    return json.loads(data['exit_policy_json']) if data.get('exit_policy_json') else dict(
        version='legacy-unversioned',stop_percent=default_stop,protect_percent=1.0,trail_pp=1.0)


def protective_floor(policy, peak_percent):
    # In v2 +8% arms the trailing exit; it is not a locked profit floor.
    if policy['version'] == ANOMALY_VERSION:
        return None
    reached = [level for level in policy.get('protect_steps', [])
               if peak_percent + 1e-9 >= level]
    return max([policy['protect_percent']] + reached)


def upgrade_open_anomaly_policies(db, now):
    """Apply the authorized v2 rule to open v1 positions, once and atomically.

    Keep the old policy and transition time for reporting. Do not reset the
    peak, activation flag, quantities, costs, fills, or any closed position.
    """
    with db:
        rows = db.execute(
            "SELECT id, exit_policy_json FROM paper_positions "
            "WHERE status='OPEN' AND exit_policy_json IS NOT NULL"
        ).fetchall()
        for position_id, original in rows:
            policy = json.loads(original)
            if policy.get('version') != ANOMALY_LEGACY_VERSION:
                continue
            upgraded = dict(policy, version=ANOMALY_VERSION,
                            previous_policy=policy, policy_changed_at=now)
            db.execute(
                "UPDATE paper_positions SET exit_policy_json=? "
                "WHERE id=? AND status='OPEN' AND exit_policy_json=?",
                (json.dumps(upgraded), position_id, original),
            )
