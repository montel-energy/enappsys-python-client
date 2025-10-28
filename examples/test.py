from enappsys import EnAppSys

client = EnAppSys()

nl_solar = client.bulk.get(
    'csv',
    data_type='NL_SOLAR_NOWCAST',
    entities=['NL.DRENTHE', 'NL.ZEELAND'],
    start_dt='2025-01-01T00:00',
    end_dt='2025-01-02T00:00',
    resolution='qh',
    time_zone='CET',
)
df_nl_solar = nl_solar.to_df()
print(df_nl_solar)

hu_price_volume_curve = client.price_volume_curve.get(
    'csv',
    code='hu/elec/ancillary/capacity/afrr/daily/up',
    dt='2025-01-01T00:00',
    time_zone='CET',
    currency='EUR',
).to_df()

print(hu_price_volume_curve)