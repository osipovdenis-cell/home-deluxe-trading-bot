# Post-close rocket analysis (shadow only)

The worker reads closed leader/anomaly positions and the existing bid path after
exit + 60 minutes, archives immutable entry features, submits at most three
structured proposals to an independent AI client, and validates them locally.
It cannot place orders or change live policies. Existing entry AI is unchanged.

`<audit_db_path>.rocket_ideas.sqlite3` is the durable journal: reviews, ideas,
all source identities, archived events, reserved request costs and weekly runs.
It is separate from the trading database. Reports include `rocket_idea_analysis`
and a human-readable status. Every Monday UTC, one Telegram summary is produced
for the preceding completed weeks. Delivery failures retry; a process crash
between successful delivery and its receipt commit can duplicate that summary.

## Configuration

| Environment variable | Default | Meaning |
|---|---:|---|
| ROCKET_IDEAS_ENABLED | true | Collect and evaluate in background |
| ROCKET_IDEAS_DAILY_REQUESTS | 24 | Maximum attempted requests per UTC day |
| ROCKET_IDEAS_DAILY_USD | 1 | Daily reserved USD ceiling |
| ROCKET_IDEAS_INPUT_USD_PER_MILLION | 0 | Explicit input token price upper bound |
| ROCKET_IDEAS_OUTPUT_USD_PER_MILLION | 0 | Explicit output token price upper bound |

AI calls require the existing API key **and both positive price bounds**. Set
bounds at least as high as the configured model's current rates before enabling
paid reviews. Zero means paused, not free. Collection and weekly reports continue
while paused. Do not guess prices from the model's name. Reservations cover input
UTF-8 bytes plus framing allowance and the full output-token limit. Failed calls
consume their reservation; retries are rate/budget limited and survive restarts.
The journal reports why AI is paused. Review payloads exceeding 120 KB are retained
and flagged, never silently truncated. Adjust this explicit bound in `Config`
only after checking the model's context and budget. HTTP errors stay in the worker.

## Fixed screening protocol

`bot.idea_evaluation.PROTOCOL` is frozen before collection: 30 affected events,
positive improvement in both last completed UTC weeks, positive after removing
the two best symbols, lower bound of a 95% symbol-cluster bootstrap above zero
(2,000 resamples, fixed seed). Rules with missing features or incomplete outcomes
are excluded explicitly. All sources of a merged idea are excluded, including
linked rejected decisions subsequently recovered into an actual trade.

Filter comparison is paired on the same events: a skipped event earns zero.
For exit and delay comparisons both baseline and counterfactual must close on the
same complete observed path. Bid OHLC is traversed in both high-first and low-first
orders and the worse outcome is used. Stops fill at the observed bid, not an
invented guaranteed threshold price. Entry uses the recorded ask; recorded costs
are subtracted once. No forced exit at the recording horizon. Percentages are
percentage points; the normalized USDT figure assumes 50 USDT per event and is
**not bank return**. Weekly and strategy/source breakdowns accompany the metrics.
A passed idea is only a candidate for a new forward shadow experiment. Screening
many AI-selected ideas is exploratory, not an out-of-sample profitability proof.

## Data limitations and interpretation

- Symbol is included for explanation and clustered statistics, never predicates.
  Missing inputs remain null; there is no inference from future outcomes or nearby
  confirmations. Snapshot time is the original decision/signal time; context
  snapshots were available before the actual entry. Recovered entries keep their
  original signal snapshot and are marked in the archived entry probe.
- New rejected episodes additionally retain bid minute extrema and timestamped
  ask samples (one per second), through their existing observed endpoint. This
  does not extend their current one-hour observation horizon or change outcomes.
  Old rejections without complete paths cannot support alternate exits.
- Delay rules mean fixed waiting after an initial predicate, not arbitrary new
  future conditions. They can be evaluated only when the delayed ask coincides
  with a recorded minute-bar boundary, within two seconds of the target, and both
  exits are observable. Other delays report insufficient data. Actual-trade bid
  archives lack future asks and therefore cannot establish delayed fills.
- Incomplete quotes, unclosed alternative exits and missing historical entry
  features remain separate exclusions. “No evidence” is never “passed”.
- Thresholds merge by predefined decimal bins in `FEATURES`, not by optimizing
  thresholds against results. The canonical rounded predicate is what is tested.
  A repeat counts unique source trades, not repeated retries of the same response.
- Strategy versions come from each position's saved `exit_policy_json`, not a
  wall-clock cutoff. Legacy unversioned positions remain separately labeled;
  unknown costs/exit parameters prevent replay. Ordinary leaders and anomalous
  rockets retain their distinct policies; this PR never edits either.
- No loss/pause in the analysis worker can block an entry or a position exit.
  Restart recovers archived trades and outstanding requests. Raw historical
  information that the old recorder never saved cannot be reconstructed here.

## Validation

`python -m unittest discover -s tests` covers forbidden inputs, normalized merging,
all-source exclusion, stop/protection/trailing paths, incomplete paired cohorts,
bootstrap gates, delayed-ask requirements, durable timeouts and budget enforcement,
weekly retry, plus the existing trading regression suite.
