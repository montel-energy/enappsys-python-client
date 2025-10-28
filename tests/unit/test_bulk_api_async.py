from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Literal, Union

import pytest

if TYPE_CHECKING:
    from enappsys import EnAppSysAsync


def df_to_list(df):
    header = ["datetime"] + list(df.columns)
    rows = [[idx, *values] for idx, *values in df.itertuples(index=True, name=None)]
    return [header, *rows]


@pytest.mark.asyncio
async def test_csv_to_list(client_async: "EnAppSysAsync", api_params, expected_data):
    out = await client_async.bulk.get(
        "csv",
        **api_params,
    )
    df = out.to_df(
        data_type_in_columns=True,
        rename_columns=["be_dap", "nl_dap"],
    )
    actual = df_to_list(df)
    assert actual == expected_data


@pytest.mark.asyncio
async def test_csv_to_df(client_async: "EnAppSysAsync", api_params, expected_df):
    out = await client_async.bulk.get(
        "csv",
        **api_params,
    )
    df = out.to_df(
        data_type_in_columns=True,
        rename_columns=["be_dap", "nl_dap"],
    )
    assert df.equals(expected_df)


@pytest.mark.asyncio
async def test_json_to_df(client_async: "EnAppSysAsync", api_params, expected_df):
    out = await client_async.bulk.get(
        "json",
        **api_params,
    )
    df = out.to_df(
        data_type_in_columns=True,
        rename_columns=["be_dap", "nl_dap"],
    )
    assert df.equals(expected_df)


@pytest.mark.asyncio
async def test_json_map_to_df(client_async: "EnAppSysAsync", api_params, expected_df):
    out = await client_async.bulk.get(
        "json_map",
        **api_params,
    )
    df = out.to_df(
        data_type_in_columns=True,
        rename_columns=["be_dap", "nl_dap"],
    )
    assert df.equals(expected_df)
