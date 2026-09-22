# Design decisions

The decisions that shape this codebase, each with the reasoning it can be
defended on. The README says what the pipeline does; this says why it does it
that way.

## Fetching and transforming are separate jobs

The fetch jobs (`backfill`, `update`) store what the sources served, byte for
byte, and never parse it. `build` derives every Parquet partition from those
files alone.

An earlier design wrote parsed rows straight into storage from three jobs, so
correctness lived in write order and cursors: newest-write-wins across two
storage tiers, a compaction whose step order was load-bearing, a reconcile
cursor per symbol and family, update cursors merged under a file lock. Every
one of those existed to patch a row after it was written. With partitions as
a pure function of immutable inputs there is nothing to patch:

- Reconciliation is the precedence rule. On a shared `(symbol, time)` the
  archive row wins; among REST rows the newest run wins. When Vision publishes
  a month, the next build takes its rows over the REST ones.
- Gap filling is the update window. `update` resumes from the newest stored
  row, so whatever a failed run left missing is asked for again, and the
  month's archive overwrites the rest.
- Cursors are read from the partitions. The state file holds only what
  discovery knows: status, listing date, delivery date.
- A parse bug is fixed by fixing the parser and running `build --force`. The
  raw bytes are still there.

The cost is that the month in progress is rebuilt every hour from the REST
tail. Measured at 570 symbols that is a few seconds per family.

## Partitions remember their inputs

Each partition stores a fingerprint of the raw files it was built from in its
Parquet metadata: archive names, sizes and mtimes, and REST file names. A
month whose fingerprint has not changed is skipped, so the hourly build
touches the current month and nothing else.

The fingerprint is stat-based rather than hash-based on purpose: a full
history is ~100k archive files and the scan must stay in the tenths of a
second. The `.CHECKSUM` sidecars next to the archives are for integrity, not
for change detection.

## One archive shape

Vision publishes klines and funding by month but open interest (`metrics`)
only by day. `backfill` packs a month of daily CSVs into one ZIP, each day one
member with its bytes untouched. The member list is the plan ("which days are
missing"), the file count drops thirtyfold, and `build` reads every family the
same way: parse each member, concatenate. A packed month may be partial, which
is why precedence is per row and not per month.

## Absent periods live in the raw store

A period Vision has not published fifteen days after it ended is recorded as
an archive with no members, or as an empty day member inside a packed month.
The plan stays a directory listing (the period is "there"), `build` reads
nothing from it, and it is asked for once instead of on every run. Rejected: a
separate negative-cache file, which would be state beside the data. The
delivery date from `exchangeInfo` handles the delisted case exactly, so no
period is ever requested after a symbol's last day.

## Verified or absent

A Vision download is returned only once it matches the published SHA-256 and
every ZIP member passes its CRC-32. A failed check is retried and then raised;
a network error raises too. Only a 404 means "not published". So the raw store
never holds unverified bytes, and a bad download is an error the next run
retries, never a silent gap. Daily metrics files skip the checksum request
(they are tiny and number in the hundreds of thousands); the CRC still guards.

## Rate limiting: a sliding-window log per pool

Binance meters an IP through independent pools, and the client models each:
request weight per minute for `/fapi/v1`, and plain request counts per five
minutes for `/futures/data/*` and for the funding endpoints, which carry no
weight and never appear in the used-weight header. Kline weight scales with
`limit` (1, 2, 5 or 10). The limits come from the official SDK's docstrings,
cross-checked against live response headers.

Each pool is a sliding-window log rather than a token bucket. A bucket admits
a full burst and then refills, so up to twice the limit can land inside one of
the server's windows; the log holds the budget inside any window. It is ~40
lines and replaced a dependency. Budgets sit below the server limits
(2000 / 900 / 450) for clock skew and other processes on the same IP.

The server has the last word: a used-weight header at budget blocks every
request until the next minute, a 429 blocks them for `Retry-After`, and a 418
opens a circuit so that queued requests fail without being sent, because
traffic during a ban extends it. Budget is acquired before the in-flight slot,
so requests waiting for budget do not occupy connections.

## One update process

`update` fetches every family in one run with one client, so a single
throttle meters everything sent from the host. Separate processes per family
each assumed the full budget and, fired from cron in the same second, overran
it together. Families draw from independent pools, so they run concurrently
and fail independently; kline requests are queued first so candles land first.

Each family is fetched the way the API serves it: klines per symbol from the
newest stored candle (inclusive, since it was in progress when stored), open
interest as "the latest N points" sized to the gap, funding as one
market-wide query paged forward from a watermark. A symbol with nothing stored
gets the 1,500 candles one request carries: 62 days, which reaches past any
month Vision has not published, so the order of the first `backfill` and the
first `update` does not matter.

## What is not collected, and why

- **Taker buy/sell ratios and volumes** from `/futures/data`: REST retains
  ~30 days and the Vision archive carries a 5-minute ratio that is not
  comparable to REST's hourly one. A column that can never be backfilled is
  NULL for everything older than a month. OHLCV klines already carry exact
  hourly taker buy volume.
- **Long/short ratio series**: they were three quarters of the metrics
  traffic and open interest was the series that mattered. With one endpoint
  per row there are no partial rows, so no column-wise merge is needed.

## Parsing with polars

Parsers read the Vision CSV or the REST rows straight into a polars frame and
cast by a declared schema; the REST and Vision parsers of a family are asserted
to produce identical frames. Both kline sources carry the same twelve fields,
so one parser pair serves OHLCV, mark-price and premium-index klines.
