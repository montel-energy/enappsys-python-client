from enappsys import EnAppSys

client = EnAppSys()

day_ahead_price_vol = client.bulk.get(
    'csv',
    data_type='EPEX_HR_AUCTION_RESULTS_DE',
    entities=['DA_PRICE', 'DA_VOLUME'],
    start_dt='2025-01-01T00:00',
    end_dt='2025-01-02T00:00',
    resolution='qh',
    time_zone='CET',
)
df_day_ahead_price_vol = day_ahead_price_vol.to_df()

day_ahead_price = client.bulk.get(
    'csv',
    data_type='EPEX_HR_AUCTION_RESULTS_DE',
    start_dt='2025-01-01T00:00',
    end_dt='2025-01-02T00:00',
    resolution='qh',
    time_zone='CET',
)
df_day_ahead_price = day_ahead_price.to_df()
day_ahead_all = client.bulk.get(
    'csv',
    data_type='EPEX_HR_AUCTION_RESULTS_DE',
    entities=None,
    start_dt='2025-01-01T00:00',
    end_dt='2025-01-02T00:00',
    resolution='qh',
    time_zone='CET',
)
df_day_ahead_all = day_ahead_all.to_df()


day_ahead_chart = client.chart.get(
    'csv',
    code="de/elec/pricing/daprices",
    start_dt='2025-01-01T00:00',
    end_dt='2025-01-02T00:00',
    resolution='qh',
    time_zone='CET',
)
df_day_ahead_chart = day_ahead_chart.to_df()

hu_price_volume_curve = client.price_volume_curve.get(
    'csv',
    code='hu/elec/ancillary/capacity/afrr/daily/up',
    dt='2025-01-01T00:00',
    time_zone='CET',
    currency='EUR',
)

df_hu_price_volume_curve = hu_price_volume_curve.to_df()
