# Scalping v1: prospective, independent, no orders

Ordinary trading remains disabled (`ordinary_max_open_positions=0`). Rocket
entry/exit policies, accounts and the rocket comparison are unchanged.

## Cohort and clock

Only new confirmation events with a known non-leader signal kind are included.
The existing universe/12-hour filter remains upstream. Legacy mixed history is
retained, but is NOT used for the new scalp learner or its AI history payload.
At most 20 symbols are observed, with one episode/symbol/15 minutes. Capacity
exclusions are counted. No backfill from summary averages.

Features are frozen at context capture. For processed signals, entry readiness
is moved to processing completion, including AI response/rejection. A delay
over 30 seconds makes the snapshot incomplete. The first later bookTicker ask
is the common reference entry. A spread above the existing scalp ceiling of
0.1% means no entry for any variant. Every subsequent exit uses observed bid.
Full ordered bid/ask events are retained in a bounded queue, not just extrema.
Gaps over 30 seconds or queue overflow exclude the episode from training and
final comparison. Reconnect/restart can therefore reduce coverage, not invent
profitable paths. Quotes received before decision readiness cannot open a leg.

## Entry hypotheses (frozen v1; not tuned on reported winners)

* `current`: ordinary confirmation, execution, quality and AI checks pass;
  legacy mixed-history adjustments are replaced with a fresh neutral profile.
* `continuation`: 5/10/20/60-second changes positive; pullback no more than
  0.15%, retaining the existing near-high boundary.
* `recovery`: 5/10/60-second changes positive with pullback greater than 0.15%.
  This is only a coarse recovery hypothesis, NOT a proven higher-low detector.

The last two variants do not require AI BUY. They are shadow experiments, not
permission to bypass trading safety. All events with an admissible reference
entry contribute to the classifier, even if no entry variant selects them.
AI decision and latest rejection are stored separately from the label.

## Paired exits and accounting

Each selected entry gets two independent 50-USDT virtual legs: all at +0.7%,
or 50% at +0.7% and 50% at +1%. Both use -0.5% stop and 15 minutes from entry.
Partial sales and barrier overshoot use observed bid, not the exact threshold.
At the horizon, remaining quantity is sold at the next bid within the allowed
quote gap. A post-deadline price cannot create a pre-deadline target label.
Configured round-trip cost is subtracted once from weighted gross return;
spread is already embedded in ask/bid and is not subtracted again.

Reported USDT is the sum of independent 50-USDT experiments, NOT a 200-USDT
portfolio simulation. Concurrent slots, bid/ask size, market impact and
slippage are not simulated. Drawdown is of cumulative closed experimental
results, not marked-to-market bank equity. Incomplete cases are explicit.

## Learning and validation

The coin profile and new probability model use the SAME reference label:
+0.7% before -0.5% within 15 minutes, measured from the post-decision ask.
Neutrals are recorded separately and have binary target label zero. Their
PnL nevertheless uses their actual horizon bid, not zero price movement.

Only fully matured episodes available at prediction time may train the model.
The last fifth is validation; remove training events whose label-end time
overlaps the first validation observation. At least 250 training and 80
validation events are needed in addition to the 400-example minimum.
Production predictions use matured data only and are saved before the outcome.
The new model and profile DO NOT adjust trading thresholds yet.

Evaluate on subsequent periods: net PnL, PF, closed-result drawdown, missing-data
rate, distinct symbols, and consistency across periods. Do not enable scalping
because of a single breakeven period or a high fraction of targets. Any new
threshold/variant needs a new version and a new out-of-time comparison.
