BACKOFF_FACTOR = 0.5
RATE_LIMIT_DELAY = 0.1
STATUS_FORCELIST = (429, 500, 502, 503, 504)

# Rows per request above which the platform's response time grows faster than
# linearly. This is a latency budget, not a payload limit -- APIBase.API_MAX_ROWS
# remains the payload ceiling that the HTTP 413 fallback respects.
#
# Measured against app.enappsys.com. Some series answer a 35,040-row (one year,
# qh) request in under a second; others take around 100s for the identical
# shape. The slow ones grow roughly with the square of the rows requested: three
# years as a single request took 23 minutes, against 43s split serially and 4s
# split concurrently, returning identical data.
#
# 5000 is a deliberate compromise rather than the measured optimum (~1750). The
# optimum assumes every series is slow; in a 50-series sample only 22 were, and
# for the fast ones extra requests are pure overhead. At 5000 the slow series
# improve by an order of magnitude while the fast ones lose a second or two.
#
# If the platform's slow query path is fixed, this should go back to something
# large -- it exists only to work around that.
CHUNK_ROWS = 5000
