# Report periods and model progress

## Timing recorder v2 and diagnostic order flow

The timing experiment writes to `<audit-path>.rocket_timing.sqlite3` in WAL
mode. It no longer competes for the trading/audit database writer lock. Commands,
model results and heartbeat commit together; drained commands and in-memory job
references are acknowledged only after commit. A failed iteration invalidates
affected paths rather than concealing a missed extremum. Error type and SQLite
code are reported without exception payloads. The old timing database/tables
remain untouched; their counts and error total are reported as an archive.

The diagnostic feed uses live subscribe/unsubscribe on one connection for
aggTrade, bookTicker, depth5 and depth updates. Membership/order changes do not
reconnect existing coins. Recently watched coins retain subscriptions for up
to 120 seconds within the 40-symbol cap; active timing episodes take priority.
Real depth5 quotes fill otherwise quiet quote windows without interpolation.
Update IDs prevent stale depth snapshots overwriting newer quotes; aggregate
trade IDs prevent duplicate volume. Disconnects are recorded and clear flow
anchors, so an incomplete 60-second trade window cannot become a known signal.
The snapshot is atomic and excludes events received after its timestamp.

Timing exits process bids before the first connection gap; an already completed
exit is retained, but any leg still waiting/open at the gap becomes incomplete.
Heartbeat staleness is reported without mutating recorded outcomes.

Low-volume shadow experiments now read this diagnostic feed instead of the
trading feed. They record feed_version=2 and explicit missing-data reasons:
no subscription, stale trades/quotes, missing price anchors, warm-up after a
disconnect or incomplete 10-second windows. Observed flow may be retained for
diagnosis even when stale, but it cannot make the entry eligible. The text shows
new-feed coverage separately. Old UNKNOWN examples are not reconstructed.
The hypothesis, two-second freshness limit, market checks and actual trading
probe/entry/exit rules are unchanged; this does not enable any additional orders.

## Low-volume rocket continuation, prospective shadow v1

For new confirmed leaders rejected at the existing finite 0 <= volume_ratio_5m < 1
gate, save numeric entry features with the exact rejection episode. Read the
already buffered order-flow probe; no REST/AI call, sleep, order or new signal.
The snapshot contains price changes over 5/10/20/60 seconds, executed buys and
sells over 5/60 seconds, prior 5-second flow, spread, CVD, trade acceleration,
efficiency and available 15m/1h/4h trend. Missing or stale inputs remain unknown.
Never backfill a predictor from prices after the decision.

Frozen hypothesis B: positive 5/20/60-second price changes AND buys greater than
sells in both the 5- and 60-second windows. Also retain the existing fresh leader
quality/fading-buy guards, positive 12h change, 0.25% spread and 0.1% tick limits.
Require fresh trades/quotes and complete quote windows. These are a predefined
test, not thresholds fitted to the three previously linked historical examples.
The decision snapshot is at most 2 seconds old when captured. B uses only the
first actual quote following the rejection (never a later favourable quote),
checks snapshot age <= 2s again, spread/tick, and absolute signal drift < stop.
A rejected first quote stays NO_ENTRY; an absent/stale quote stays UNKNOWN.

A = retain the actual volume refusal, hence no trade and 0 USDT. B and the
descriptive "buy all low-volume refusals" reference share the existing daily
ledger's first ask / chronological bid path: 50 USDT, frozen stop and round-trip
cost, full-position +1% protection / 1 percentage-point trail, 60-minute horizon.
Spread is in ask-to-bid returns, not deducted again. Interrupted paths never
become known wins/losses; horizon-marked positions are reported separately from
closed PnL. A gap after a completed close does not invalidate that close.

This tests market eligibility, NOT the whole trading algorithm: AI vetoes,
symbol history, capital/slot limits, execution depth and slippage are not
modelled. The existing trading volume rejection is unchanged. No automatic
promotion or stop adjustment. Prospective results must be reviewed on later
days, across coins, with costs and missing-path coverage; report PnL excluding
the best coin to reveal concentration. Independent episodes are not independent
portfolio returns and repeated signals for the same move remain correlated.

The separate /learning and two-hour text shows rolling 24h and 7d cohorts by
decision time (these windows overlap). Encrypted rocket_daily.volume_test also
contains symbol/day PnL, counts, PF, feature medians for profitable/unprofitable
low-volume refusals and rejection reasons. Raw 24h episodes retain their frozen
volume_experiment and volume_execution fields. Old episodes without a snapshot
are excluded. Reaching a profit target later is not proof of a profitable
rocket trade under the chronological stop/trail rules.

## Quote recorder v2 and AI availability

Rocket daily counterfactuals now use a dedicated combined WebSocket with live
SUBSCRIBE/UNSUBSCRIBE, rather than reconnecting all symbols whenever membership
changes. Real-time bookTicker events retain intrasecond barrier ordering;
actual Binance depth5 snapshots provide a periodic best bid/ask on quiet books.
Older update IDs cannot overwrite newer quotes. No synthetic ticks, forward fill
or REST reconstruction are used. Disconnects, invalid quotes, overflow and gaps
remain INCOMPLETE. A close observed before a later disconnection remains valid.
Transaction failures restore active in-memory state to committed state.

New episodes carry quote_version=2 and the daily text shows their counts
separately; existing incomplete observations are never relabelled as complete.
Health includes real book/depth quote counts, reconnections and connection state.
This improves the prospective daily ledger only; it does not reconstruct old
leader-path or timing A/B gaps and is not an execution-quality guarantee.

Signal and performance OpenAI calls share a cooldown after HTTP 429. Retry-After
is respected; otherwise exponential delay with jitter starts at 30 seconds.
Recognized credit/spend/usage-limit codes pause for at least an hour and are
reported separately from rate limits. Unknown 429 responses remain unknown.
Cooldowns return immediately without calling the API; they do not become AI
WAIT/SKIP decisions or repeated API error counts. Counters and an allow-listed
error code are exported and shown in the observer. Restoring exhausted credits
or account limits requires an account-side change; retries cannot fix that.
No API response message, credentials or account identifiers are exported.

Entry thresholds, stop, trailing exit, fallback eligibility, stake and paper-only
permissions are unchanged by this update.

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


## Stable spread experiment (stable-spread-v1)

This prospective shadow comparison starts after the 20-second, volume and execution gates,
before the leader-quality rejection. Both legs require the same remaining quality checks.
A requires spread contraction; B additionally accepts an exactly unchanged spread, with
the existing 25 bps ceiling. Widening or unknown spread does not qualify. Contracting and
stable cohorts are reported separately. No live entry rule changes or automatic promotion.
AI, final fresh-entry approval and bank limits are excluded equally from both legs: this
isolates one quality gate, not the entire executable trading strategy. Entry and exit quotes
are estimates from the market price and frozen signal spread; fees and stop are frozen at
creation. Each symbol contributes at most one episode/hour; incomplete pairs are excluded.
The model does not account for order depth or slippage.

`/learning` and the periodic report include this comparison and specific rocket rejection
reasons. Encrypted exports include up to 500 recent reason events, 100 spread pairs and their
legs. Ninety-second entry waits retain their last blocking reason on expiry.

The independent bid recorder retains drained events across transaction rollback, updating
its deduplication state only after commit. Quote commits precede recovery/report calculations.
Real overflow and missing timestamps still make paths incomplete; existing incomplete history
is not reclassified. `rocket_recorder_health` in encrypted exports exposes successful recording,
retry counts, overflow counts and the last exception type, without raw error text or credentials.

## Rocket entry timing (rocket-timing-v1)

Prospective paper-only A/B on candidate processing events (before volume/quality/AI
vetoes). Common vetoes are recorded as NO_ENTRY in both arms. Approved candidates
start both arms from the same decision time; A models the existing final guard and
90-second recovery, B additionally requires last-5s executed buys > preceding-5s
buys and sells, and bid > bid 5 seconds ago. This is not a replay of actual fills:
AI/common gates are shared and frozen, account capacity is not modelled. Approval
occurs before the synchronous fresh-entry REST quote; the independent shadow uses
observed streaming ask/bid and the same price drift and spread limits instead.

A separate websocket and worker keep ordered bid events while main blocks on
AI/REST. Existing trading/scalp streams, exits and permissions are unchanged.
Candidate-to-approval timing, pre-approval snapshots (last 60), one-second waiting
snapshots, entry probes and final rejection reasons are available through the
encrypted export; latest 100 pairs. /learning and scheduled observer include the
new summary with all-history paired PnL and exclusions. No automatic activation.

One episode per symbol/hour, max 20 active episodes; stream capacity 40 with active
episodes first and watched leaders for warmup. This covers processed candidate
signals, not every Binance coin or every rejected 20-second confirmation.
Unwarmed/missing windows, checks >2s, bid gaps >5s, buffer overflow, restart or
recorder failure mark affected pairs incomplete; excluded from both PnLs. Startup
may initially produce incomplete pairs while windows accumulate. NO_ENTRY is never
substituted for missing data. Health exposes last tick, errors and dropped commands.

Each leg uses 50 USDT, initial stop and round-trip fee frozen at approval; whole
position +1% activation/floor and 1pp trailing drawdown; common 60-minute horizon
from approval with open valuation separately reported. Gaps/overshoots use observed
bid, never ideal threshold fill. Depth, slippage and portfolio slot contention are
not simulated. This version does not fix or replace older shadow recorders.
