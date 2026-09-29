BACKOFF_FACTOR = 0.5
RATE_LIMIT_DELAY = 0.1
STATUS_FORCELIST = (429, 500, 502, 503, 504)

# Rows per request that `chunk_rows=True` selects, when a caller opts in to
# splitting a wide date range without naming a budget. This is a request
# sizing budget, not a payload limit -- APIBase.API_MAX_ROWS remains the payload
# ceiling that the HTTP 413 fallback respects.
#
# The two costs pull against each other: a larger budget means fewer round
# trips, a smaller one means each request covers less data. 5000 sits between
# them. Worth measuring against the series you actually fetch before changing.
CHUNK_ROWS = 5000
