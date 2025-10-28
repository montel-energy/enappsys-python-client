from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from enappsys import EnAppSys


def test_csv_to_list(client: "EnAppSys", api_params, expected_data):
    out = client.bulk.get("csv", **api_params)
    df = out.to_df(
        data_type_in_columns=True,
        rename_columns=["be_dap", "nl_dap"],
    )
    header = ["datetime"] + list(df.columns)
    rows = [[idx, *values] for idx, *values in df.itertuples(index=True, name=None)]
    actual = [header, *rows]
    assert actual == expected_data


def test_csv_to_df(client: "EnAppSys", api_params, expected_df):
    out = client.bulk.get("csv", **api_params)
    df = out.to_df(
        data_type_in_columns=True,
        rename_columns=["be_dap", "nl_dap"],
    )
    assert df.equals(expected_df)


def test_json_to_df(client: "EnAppSys", api_params, expected_df):
    out = client.bulk.get("json", **api_params)
    df = out.to_df(
        data_type_in_columns=True,
        rename_columns=["be_dap", "nl_dap"],
    )
    assert df.equals(expected_df)


def test_json_map_to_df(client: "EnAppSys", api_params, expected_df):
    out = client.bulk.get("json_map", **api_params)
    df = out.to_df(
        data_type_in_columns=True,
        rename_columns=["be_dap", "nl_dap"],
    )
    assert df.equals(expected_df)
