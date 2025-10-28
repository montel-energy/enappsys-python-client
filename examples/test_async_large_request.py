import asyncio
import pandas as pd
import logging
from enappsys import EnAppSysAsync

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


async def main(start_dt, end_dt):
    async with EnAppSysAsync() as client:
        freq = await client.bulk.get(
            'json',
            data_type="GB_SYSTEM_FREQUENCY_1SECOND",
            entities=["FREQUENCY"],
            start_dt=start_dt,
            end_dt=end_dt,
            resolution="1s",
            time_zone="CET",
        )
        df_freq = freq.to_df(rename_columns=["freq"], unit_in_columns=False)
        df_freq.to_csv("freq.csv")

        tz = df_freq.index.tz
        start = pd.Timestamp(start_dt).tz_localize(tz)
        end = pd.Timestamp(end_dt).tz_localize(tz)

        expected = pd.date_range(start, end - pd.Timedelta(seconds=1), freq="s")
        missing_seconds = expected.difference(df_freq.index)
        print(f"Missing seconds: {list(missing_seconds)}")


if __name__ == "__main__":
    start_dt = "2023-10-01T00:00"
    end_dt = "2023-11-01T00:00"
    asyncio.run(main(start_dt, end_dt))
