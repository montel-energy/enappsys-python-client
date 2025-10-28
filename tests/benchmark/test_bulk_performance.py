import asyncio
from datetime import datetime

from enappsys import EnAppSys, EnAppSysAsync

params = {
    "start_dt": datetime(year=2022, month=1, day=1),
    "end_dt": datetime(year=2023, month=1, day=1),
    "resolution": "hourly",
    "time_zone": "CET",
}


def main():
    client = EnAppSys()
    client.bulk.get(
        "csv",
        data_type="ENTSOE_AGGREGATED_GENERATION_PER_TYPE",
        entities=["BE.BELGIUM_SOLAR", "NL.NETHERLANDS_SOLAR"],
        **params,
    ).to_df(rename_columns=["be_solar", "nl_solar"])

    client.bulk.get(
        "csv",
        data_type="ENTSOE_DAY_AHEAD_PRICES",
        entities=["BE.BELGIUM", "NL.NETHERLANDS"],
        **params,
    ).to_df(rename_columns=["be_dap", "nl_dap"])

    client.bulk.get(
        "csv",
        data_type="ENTSOE_NET_CROSS_BORDER_PHYSICAL_FLOWS",
        entities=["NL.NETHERLANDS_BELGIUM", "BE.BELGIUM_NETHERLANDS"],
        **params,
    ).to_df(rename_columns=["be_to_nl", "nl_to_be"])

    # optional explicit close
    client._session.session.close()


async def get(client: EnAppSysAsync, data_type, entities, params, rename_columns):
    out = await client.bulk.get("csv", data_type=data_type, entities=entities, **params)
    return out.to_df(rename_columns=rename_columns)


async def main_async():
    client = EnAppSysAsync()
    await asyncio.gather(
        get(
            client,
            "ENTSOE_AGGREGATED_GENERATION_PER_TYPE",
            ["BE.BELGIUM_SOLAR", "NL.NETHERLANDS_SOLAR"],
            params,
            ["be_solar", "nl_solar"],
        ),
        get(
            client,
            "ENTSOE_DAY_AHEAD_PRICES",
            ["BE.BELGIUM", "NL.NETHERLANDS"],
            params,
            ["be_dap", "nl_dap"],
        ),
        get(
            client,
            "ENTSOE_NET_CROSS_BORDER_PHYSICAL_FLOWS",
            ["NL.NETHERLANDS_BELGIUM", "BE.BELGIUM_NETHERLANDS"],
            params,
            ["be_to_nl", "nl_to_be"],
        ),
    )
    await client._session.close()


def test_sync(benchmark):
    def run_sync():
        main()

    benchmark(run_sync)


def test_async(benchmark):
    def run_async():
        asyncio.run(main_async())

    benchmark(run_async)
