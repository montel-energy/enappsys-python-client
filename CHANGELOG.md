# Changelog

The section matching the version in `src/enappsys/__init__.py` becomes the body
of the GitHub release. Without a matching section the release falls back to a
list of commit subjects, so anything worth explaining belongs here.

## 0.3.0

### Splitting wide requests

A wide date range can be split into several requests and stitched back
together, so a long history arrives as one result. It is off unless asked for:

```python
client.bulk.get(...)                    # one request
client.bulk.get(..., chunk_rows=True)   # split at CHUNK_ROWS (5,000)
client.bulk.get(..., chunk_rows=10_000) # split at a budget you choose
```

Splitting counts each entity separately, since requesting two entities returns
two series worth of data. On the asynchronous client the chunks are fetched
concurrently. The HTTP 413 fallback is unchanged: a request over the payload
ceiling is still split automatically, since the alternative there is failing
outright.

It is opt-in because several requests are not one request. If the data is
updated while the chunks are in flight, the later ones return the new values
and the earlier ones the old, and there is nothing in a stitched result that
shows it happened.

Two faults in the withdrawn 0.2.x splitting are fixed here: boundaries were
placed on the resolution asked for rather than the one the series is published
on, so the row at each boundary came back twice, and the decision to split
compared the two bounds, which raised `TypeError` when one carried a timezone
and the other did not.

### Bulk and chart requests can ask for a unit

`bulk.get` and `chart.get` take an optional `units` argument, on both the
synchronous and asynchronous client:

```python
client.bulk.get(..., units="EUR/MWh")
client.chart.get("csv", ..., units="EUR/MWh 55% Eff")
```

Without it the platform returns a series in its base unit, as before. Use
`to_df(unit_in_columns=True)` to see which unit came back.

The Bulk API rejects an unknown unit with HTTP 400, but a known unit the series
cannot be converted to comes back unconverted, labelled with the requested unit.

The Chart API silently ignores a unit it cannot apply and returns the base unit,
so `chart.get` raises `ValidationError` when the response is not in the
requested unit. It uses the same spelling as `bulk.get` and is CSV only: JSON
chart responses are relabelled with the requested unit but not converted.

### The secret is kept out of HTTP library logs

The platform authenticates with `user` and `pass` query parameters, so every
request URL carries them. The client already kept the secret out of its own log
records, but urllib3 logs the request line it sends, which would write it into
any log with DEBUG enabled — including task logs under an orchestrator.

Creating a client now attaches a filter to `urllib3.connectionpool` and
`aiohttp.client` that replaces the secret, leaving the rest of the URL intact:

```
https://app.enappsys.com:443 "GET /csvapi?type=ENTSOE_DAY_AHEAD_PRICES&res=qh&user=your-username&pass=<redacted> HTTP/1.1" 200 None
```

The username is kept, here and in the client's own records — it identifies the
account behind a request, which is what makes a log traceable. Previously the
client stripped it from its own debug and warning lines as well; it no longer
does.

If you have run this client with DEBUG logging enabled, the secret may already
be present in existing logs; this change does not remove it.

## 0.2.0 - 0.2.2 (withdrawn)

Yanked on PyPI. Use 0.3.0, which carries everything they introduced. Their
request splitting was on by default and could return duplicate rows.
