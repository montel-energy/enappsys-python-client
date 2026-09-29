# Changelog

The section matching the version in `src/enappsys/__init__.py` becomes the body
of the GitHub release. Without a matching section the release falls back to a
list of commit subjects, so anything worth explaining belongs here.

## 0.2.1

### Fixes duplicate rows at chunk boundaries

Requests split by 0.2.0 could return the row at each chunk boundary twice. A
three-year quarter-hourly fetch came back with 21 duplicates.

**Anyone using 0.2.0 should upgrade.** The duplicates are silent — the data is
correct apart from the repeats, so nothing raises unless something downstream
rejects a duplicated index.

Only affected requests large enough to be split whose window was not aligned to
the resolution grid, which is any caller passing `datetime.now()` or similar.
Aligned windows, and requests below `CHUNK_ROWS`, were never affected.

The platform rounds a request's bounds outward to the nearest grid point, so an
interior boundary between two points was rounded outward by both neighbouring
chunks. Boundaries are now snapped to the grid; the caller's own start and end
are left untouched, so a split result matches an unsplit one exactly.

To check data already fetched with 0.2.0:

```python
duplicates = int(df.index.duplicated().sum())    # should be 0
```

## 0.2.0

### Wide requests are split automatically

Requests covering a wide date range are now split into several smaller ones and
stitched back together, returning identical data. A long history arrives as a
single result without you having to loop over it yourself.

Nothing needs to change at the call site. `bulk.get()` takes an optional
`chunk_rows` to control it:

```python
data = client.bulk.get(
    "csv",
    data_type="ENTSOE_AGGREGATED_GENERATION_PER_TYPE",
    entities=["DE.GERMANY_SOLAR"],
    start_dt="2023-01-01T00:00",
    end_dt="2026-01-01T00:00",
    resolution="qh",
    time_zone="UTC",
    chunk_rows=10_000,   # optional; defaults to CHUNK_ROWS (5,000), 0 disables
)
```

Splitting applies above `CHUNK_ROWS` rows, counting each entity separately,
since requesting two entities is equivalent to making two single-entity
requests. Pass a larger `chunk_rows` for fewer, wider requests, or
`chunk_rows=0` to send a single request as before. On the asynchronous client
the chunks are fetched concurrently.

XML responses are never split, since they cannot be stitched back together.
The existing HTTP 413 fallback is unchanged.

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
