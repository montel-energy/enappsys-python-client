# Changelog

The section matching the version in `src/enappsys/__init__.py` becomes the body
of the GitHub release. Without a matching section the release falls back to a
list of commit subjects, so anything worth explaining belongs here.

## 0.2.0

### Wide requests are split automatically

The platform answers some series much more slowly as the requested range grows.
Requests are now split into several smaller ones and stitched back together,
which for those series is dramatically faster and returns identical data.

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
because requesting two entities costs the same as making two requests. Pass a
larger `chunk_rows` for fewer, wider requests, or `chunk_rows=0` to send a
single request as before.

`DE.GERMANY_SOLAR`, quarter-hourly, measured against the live platform:

| Range | Rows | One request | Split | Split, async | Speedup | Speedup, async |
| --- | --- | --- | --- | --- | --- | --- |
| 7 days | 672 | 0.2s | 0.1s | 0.1s | 1.1x | 1.2x |
| 30 days | 2,880 | 0.9s | 0.8s | 0.8s | 1.1x | 1.1x |
| 90 days | 8,640 | 5.7s | 3.4s | 2.3s | 1.7x | 2.4x |
| 180 days | 17,280 | 20.8s | 6.7s | 2.6s | 3.1x | 7.9x |
| 1 year | 35,040 | 102.5s | 14.4s | 2.7s | 7.1x | 38.6x |
| 2 years | 70,080 | 475.0s | 28.3s | 3.5s | 16.8x | 137.5x |
| 3 years | 105,120 | 1373.7s | 42.5s | 4.2s | **32.4x** | **326.6x** |

Below `CHUNK_ROWS` nothing is split, which is why the 30-day row is unchanged.
The gain grows with the range because the underlying cost grows faster than
linearly. On the asynchronous client the chunks are fetched concurrently, so the
saving compounds.

Series that are already fast at any range pay for the extra round trips
instead, but only on the synchronous client — up to 1.7x slower, 2.5s becoming
4.2s over three years. On the asynchronous client concurrency absorbs them
completely and those series are no slower at any range. `chunk_rows=0` opts out
either way.

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
