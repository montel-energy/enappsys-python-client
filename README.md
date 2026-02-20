# EnAppSys Python Client

The Python library for the [EnAppSys](https://app.enappsys.com) platform provides a light-weight, typed Python client to interact with EnAppSys' API services. Additionally, there is an asynchronous client for non-blocking operations.

## Project Status

This project is in active development (0.x release series).
While the core API structure is expected to remain stable,
minor breaking changes may occur between releases.

## Installation

Supports Python 3.10+

```bash
pip install enappsys[pandas,async]
```

The extras are optional:

- `pandas` required for converting API responses to DataFrames, e.g. via `client_response.to_df()`
- `async` required for using the `EnAppSysAsync` asynchronous client.

If you only need the synchronous client and raw responses, install without extras:

```bash
pip install enappsys
```

### Configuring credentials

Your EnAppSys username and secret are required to make API requests. You can obtain these as follows:

1. Go to any download page on EnAppSys and click **Copy API URL**.
2. In the copied URL:

    - The value after `user=` is your **username**.
    - The value after `pass=` is your **secret** (a long numeric string).


The client looks for credentials in the following order:

1. **Direct arguments** when creating the client:

    ```python
    from enappsys import EnAppSys

    client = EnAppSys(
        user="example_user",
        secret="123456789123456789123456789123456789"
    )
    ```
    
2. **Environment variables**:

    ```bash
    export ENAPPSYS_USER=example_user
    export ENAPPSYS_SECRET=123456789123456789123456789123456789
    ```

3. **Credentials file** at your home directory, the default location is: `~/.credentials/enappsys.json`:

    ```json
    {
        "user": "example_user",
        "secret": "123456789123456789123456789123456789"
    }
    ```

    You can also save and specify a custom path:

    ```python
    client = EnAppSys(credentials_file="path/to/credentials.json")
    ```

## Usage

The EnAppSys client provides several download interfaces, depending on your user permissions.

### Bulk API

The Bulk API is a subscription service that allows you to retrieve time series data.
A **data type** represents a group of related series, and each individual series within that group is referred to as an **entity**.

The web interface for browsing available data types and entities is available at:
[https://app.enappsys.com/#dataservicecsv](https://app.enappsys.com/#dataservicecsv)

Retrieve the `DA_PRICE` and `DA_VOLUME` entities belonging to `EPEX_HR_AUCTION_RESULTS_DE` and convert them to a pandas `DataFrame`. When converting to a `DataFrame`, you can also rename the columns:

```python
day_ahead = client.bulk.get(
    "csv",
    data_type="EPEX_HR_AUCTION_RESULTS_DE",
    entities=["DA_PRICE", "DA_VOLUME"],
    start_dt="2025-01-01T00:00",
    end_dt="2025-01-02T00:00",
    resolution="qh",
    time_zone="CET",
)

df = day_ahead.to_df(rename_columns=["price", "volume"])
```

To retrieve **all entities** for a given `data_type`, omit the `entities` argument or pass `None`:

```python
day_ahead_all = client.bulk.get(
    "csv",
    data_type="EPEX_HR_AUCTION_RESULTS_DE",
    start_dt="2025-01-01T00:00",
    end_dt="2025-01-02T00:00",
    resolution="qh",
    time_zone="CET",
)

df_all = day_ahead_all.to_df()
```

The Bulk API supports multiple response formats:

- `"csv"`
- `"json"`
- `"json_map"`
- `"xml"`

The JSON-based formats optionally include metadata fields:

- `timestamp`: Indicates when the data was first entered into the database or created as a forecast (UTC).
- `last_updated`: Indicates the last time the data was updated in the database (UTC).

These fields can be included when converting the response to a DataFrame:

```python
data = client.bulk.get(
    "json",
    data_type="EPEX_HR_AUCTION_RESULTS_DE",
    start_dt="2025-01-01T00:00",
    end_dt="2025-01-02T00:00",
    resolution="qh",
    time_zone="CET",
)

df = data.to_df(timestamp=True, last_updated=True)
```

### Chart API

The Chart API extracts data directly from charts available on the EnAppSys platform.

Each chart is identified by a **code**, which can be found in the page URL.
For example:

```
https://app.enappsys.com/#de/elec/pricing/daprices/chart
```

The chart code is the part between `#` and `/chart`, in this case:

```
de/elec/pricing/daprices
```

Example usage:

```python
day_ahead_chart = client.chart.get(
    "csv",
    code="de/elec/pricing/daprices",
    start_dt="2025-01-01T00:00",
    end_dt="2025-01-02T00:00",
    resolution="qh",
    time_zone="CET",
)

df_day_ahead_chart = day_ahead_chart.to_df()
```

> **Note**
> Some charts contain non-timeseries data and may have a different structure.
> Below chart types are supported. If you encounter a chart that is not yet supported, please open an issue and include a link to the chart.

### Price Volume Curves

The Price Volume Curve API retrieves auction price-volume curve data for a given timestamp.

```python
hu_price_volume_curve = client.price_volume_curve.get(
    "csv",
    code="hu/elec/ancillary/capacity/afrr/daily/up",
    dt="2025-01-01T00:00",
    time_zone="CET",
    currency="EUR",
)

df_curve = hu_price_volume_curve.to_df()
```

The `dt` parameter represents the auction timestamp for which the curve should be retrieved.

## Asynchronous

An asynchronous client (`EnAppSysAsync`) is available for non-blocking and concurrent request execution.

The asynchronous interface is currently under active development.
Usage examples and extended documentation will be added in a future release.

## License

This project is licensed under the terms of the MIT license.
