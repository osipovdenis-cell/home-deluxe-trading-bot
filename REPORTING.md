# Report periods and model progress

Reports are sent every two hours, not reset every two hours. `/learning` also
sends the complete reports on demand; `/status` includes cumulative rocket
totals. Existing trading permissions, entry gates and exits are unchanged.

## Totals

Rocket virtual trades and independent scalp experiments each show trailing
2 hours, trailing 24 hours and all retained strategy history. These windows
overlap; do not sum them. “Since launch” means the earliest retained trade of
that strategy, or the earliest event of the current scalp version, not the
latest process restart. Dates are explicit UTC.

Closed rocket PnL is selected by close time, even when the position was opened
before the window. Entry counts use open time. Currently open positions are
valued once, separately; missing prices are counted rather than assumed flat.
The existing detailed rocket report still describes the entry cohort and now
states this distinction. Recorded costs are included in closed PnL.

Scalp comparisons include only completed, gap-free episodes of the current
version. A leg is attributed to its final exit time. A later observation can
complete the 15-minute episode and add the previously closed leg to a past
period. Reports are not limited by the classifier's 5000-example training cap.
Amounts are sums of independent 50-USDT experiments, not a capital-constrained
portfolio. Entry and exit variants are not additive. Partial legs remain
attributed to the final exit, not individual cash-flow timestamps.

## Existing blocks

* Observer decisions/errors, leader funnel: explicit observation window.
* “Accumulated impulse statistics” (renamed from “What the bot learned”): 30 days.
* Confirmation audit: 24 hours by default, custom lookback printed when used.
* Leader order flow and winner comparisons: 7 days by default.
* Leader paths: signal lookback is separate from each signal's 60-minute horizon.
* Rocket A/B: all retained history of its version, explicitly labelled.
* Post-stop report: stop-selection window and follow-up horizon are separate.

## Learning journal

`probability_versions` stores frozen model coefficients, normalization,
validation metrics, training-pool size, first registration time, the baseline
target rate known when fitted, and the previous version id. Stable hashes
avoid claiming a new version merely because the same fit repeats after restart.

At prediction time, the current and previous frozen versions predict the same
candidate. Their probabilities and IDs are persisted before the outcome:
`shadow_model_meta` for legacy confirmations, and `model_evaluation` in scalp
state. Legacy forecasts without this metadata are not backfilled or used to
claim prospective improvement.

Progress reports show the newest registered version and its held-out score,
then the latest version with matured prospective predictions. This avoids an
always-zero report when refits occur every 5 minutes but outcomes need 15.
Matched Brier scores compare both versions on the exact same future cases;
the baseline also uses the historical rate frozen before the outcomes. A score
difference is descriptive, not statistical proof or automatic permission to
trade. No version promotion, entry threshold or stop adjustment is performed.

The legacy mixed model still has no purged validation gap, and says so. The
separate scalp model uses purged temporal validation. LLM parameters are not
fine-tuned by sending the coin's history as context.
