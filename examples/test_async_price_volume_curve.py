import asyncio

from enappsys import EnAppSysAsync

async def main():
    async with EnAppSysAsync() as client:
        df = await client.price_volume_curve.get_multiple(
            'csv',
            code='hu/elec/hupx/curves/da/hr',
            start_dt='2025-01-01T00:00',
            end_dt='2025-01-03T00:00',
            product="1h",
            time_zone='CET',
            currency='EUR',
        )
    df.to_csv('data/price_volume_curve.csv')

if __name__ == "__main__":
    asyncio.run(main())
