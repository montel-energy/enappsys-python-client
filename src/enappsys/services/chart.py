from __future__ import annotations

import io

from datetime import datetime
from typing import Literal, overload, TYPE_CHECKING

from enappsys.enum import (
    CurrencyEnum,
    DelimiterEnum,
    ResponseFormatEnum,
    ResolutionEnum,
    TimeZoneEnum,
)
from enappsys.exceptions import ContentTooLarge, ValidationError
from enappsys.services.base import APIBase, JSONBase, JSONMapBase, _warn_empty_response
from enappsys.utils import validate_rename_columns_length, require_pandas

if TYPE_CHECKING:
    import pandas as pd


class ChartBase:
    def __init__(
        self,
        response,
        url,
        params,
        response_format,
        code,
        start_dt,
        end_dt,
        resolution,
        time_zone,
        currency,
        min_avg_max,
        enable_settlement_period=False,
        time_display=None,
    ):
        self.response = response
        self.url = url
        self.params = params
        self.response_format = response_format
        self.code = code
        self.start_dt = start_dt
        self.end_dt = end_dt
        self.resolution = resolution
        self.time_zone = time_zone
        self.currency = currency
        self.min_avg_max = min_avg_max
        self.enable_settlement_period = enable_settlement_period
        self.time_display = time_display


class ChartCSV(ChartBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def to_df(
        self,
        tz_localize: bool = True,
        rename_columns: list | dict | None = None,
        unit_in_columns: bool = False,
    ) -> pd.DataFrame:
        """Process the CSV API data format into a ``pandas.DataFrame``.

        Parameters
        ----------
        tz_localize: bool, optional
            If True, localize tz-naive index. Default is True.
        rename_columns: list, dict, optional
            Rename chart data columns only. Optional metadata columns, such as
            settlement period, keep their API-provided names and are not
            included in list length validation. If a list, provide new names
            for all chart entities. If a dict, specify original entity names
            as keys and new names as values. Default is None.
        unit_in_columns : bool, optional
            If True, includes units: "<column_name> (<unit>)". Default is False.

        Returns
        -------
        pandas.DataFrame
            Processed API response formatted as a `pandas.DataFrame`.
        """
        pd = require_pandas()

        # TODO: Determine to include seconds manually
        df = pd.read_csv(
            io.StringIO(self.response),
            header=[0, 1],
            index_col=0,
            parse_dates=True,
            date_format="[%d/%m/%Y %H:%M]",
        )

        df.index.name = "dateTime"
        if df.empty:
            _warn_empty_response("csv", url=self.url, params=self.params)
            df.index = pd.DatetimeIndex([], name="dateTime")
        if tz_localize:
            df.index = df.index.tz_localize(self.time_zone, ambiguous="infer")

        step_size = 1
        if self.min_avg_max:
            step_size = 3

        columns = df.columns.get_level_values(0).to_list()
        units = df.columns.get_level_values(1).to_list()
        settlement_column = None
        if self.enable_settlement_period and columns:
            maybe_settlement_column = str(columns[-1]).strip().lower().replace(" ", "_")
            if maybe_settlement_column == "settlement_period":
                settlement_column = columns.pop()
                units.pop()

        if rename_columns or unit_in_columns:
            if isinstance(rename_columns, list):
                validate_rename_columns_length(rename_columns, columns, step_size)

            for idx in range(0, len(columns), step_size):
                column = columns[idx]
                column_name = column.rsplit(" (MIN)")[0]

                if isinstance(rename_columns, list):
                    column_name = rename_columns[(idx) // step_size]
                elif isinstance(rename_columns, dict) and column_name in rename_columns:
                    column_name = rename_columns[column_name]
                if unit_in_columns:
                    column_name = f"{column_name} ({units[idx // step_size]})"

                if self.min_avg_max:
                    columns[idx] = f"{column_name} (MIN)"
                    columns[idx + 1] = f"{column_name} (AV)"
                    columns[idx + 2] = f"{column_name} (MAX)"
                else:
                    columns[idx] = column_name

        if settlement_column is not None:
            columns.append(settlement_column)

        df.columns = columns

        return df


class ChartJSON(ChartBase, JSONBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class ChartJSONMap(ChartBase, JSONMapBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class ChartXML(ChartBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class ChartAPI(APIBase):
    """Service providing access to the Chart API."""

    _RESPONSE_FORMAT_MAP = {
        ResponseFormatEnum.CSV: ChartCSV,
        ResponseFormatEnum.JSON: ChartJSON,
        ResponseFormatEnum.JSON_MAP: ChartJSONMap,
        ResponseFormatEnum.XML: ChartXML,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @overload
    def get(
        self,
        response_format: Literal["csv"] | ResponseFormatEnum.CSV,
        code: str,
        start_dt: str | datetime,
        end_dt: str | datetime,
        resolution: str | ResolutionEnum,
        time_zone: str | TimeZoneEnum,
        currency: str | CurrencyEnum,
        min_avg_max: bool,
        delimiter: str | DelimiterEnum = "comma",
    ) -> ChartCSV: ...

    @overload
    def get(
        self,
        response_format: Literal["json"] | ResponseFormatEnum.JSON,
        code: str,
        start_dt: str | datetime,
        end_dt: str | datetime,
        resolution: str | ResolutionEnum,
        time_zone: str | TimeZoneEnum,
        currency: str | CurrencyEnum,
        min_avg_max: bool,
    ) -> ChartJSON: ...

    @overload
    def get(
        self,
        response_format: Literal["json_map"] | ResponseFormatEnum.JSON_MAP,
        code: str,
        start_dt: str | datetime,
        end_dt: str | datetime,
        resolution: str | ResolutionEnum,
        time_zone: str | TimeZoneEnum,
        currency: str | CurrencyEnum,
        min_avg_max: bool,
    ) -> ChartJSONMap: ...

    @overload
    def get(
        self,
        response_format: Literal["xml"] | ResponseFormatEnum.XML,
        code: str,
        start_dt: str | datetime,
        end_dt: str | datetime,
        resolution: str | ResolutionEnum,
        time_zone: str | TimeZoneEnum,
        currency: str | CurrencyEnum,
        min_avg_max: bool,
    ) -> ChartXML: ...

    def get(
        self,
        response_format: Literal["csv", "json", "json_map", "xml"] | ResponseFormatEnum,
        code: str,
        resolution: str | ResolutionEnum,
        start_dt: str | datetime | None = None,
        end_dt: str | datetime | None = None,
        time_zone: str | TimeZoneEnum = "UTC",
        currency: str | CurrencyEnum = "EUR",
        min_avg_max: bool = False,
        delimiter: str | DelimiterEnum = "comma",
        enable_settlement_period: bool = False,
        time_display: dict | None = None,
    ) -> ChartCSV | ChartJSON | ChartJSONMap | ChartXML:
        """Fetch chart data.

        By default, provide ``start_dt`` and ``end_dt`` for the requested date
        range. For rolling chart windows, omit ``start_dt``/``end_dt`` and pass
        ``time_display`` as a dictionary using the API parameter names.

        Rolling windows require both backward and forward windows::

            {
                "mode": "rolling",
                "periodback": "daily",
                "amountback": 4,
                "periodfor": "min",
                "amountfor": 4,
            }

        Rolling-period windows require only the forward window::

            {
                "mode": "rolling_period",
                "periodfor": "yearly",
                "amountfor": 4,
            }

        ``rolling_period`` is the Python-facing spelling and is sent to the API
        as ``timedisplay=rolling-period``. ``enable_settlement_period`` is only
        supported for CSV responses.
        """
        response_format_enum = self._get_response_format(response_format)
        params = {}
        self._add_code(params, code)
        if time_display is None:
            if start_dt is None or end_dt is None:
                raise ValidationError(
                    reason="Provide both 'start_dt' and 'end_dt' when time_display is None.",
                    parameter="start_dt",
                )
            self._add_dt(params, start_dt, "start", "start_dt")
            self._add_dt(params, end_dt, "end", "end_dt")
        else:
            if start_dt is not None or end_dt is not None:
                raise ValidationError(
                    reason="Do not provide 'start_dt'/'end_dt' when using time_display.",
                    parameter="time_display",
                )
            self._add_time_display_params(params, time_display)
        self._add_resolution(params, resolution)
        self._add_time_zone(params, time_zone)
        self._add_currency(params, currency)
        self._add_min_avg_max(params, min_avg_max)
        if enable_settlement_period and response_format_enum != ResponseFormatEnum.CSV:
            raise ValidationError(
                reason="'enable_settlement_period' is only supported for CSV responses.",
                parameter="enable_settlement_period",
            )
        self._add_settlement(params, enable_settlement_period)
        self._add_delimiter(params, delimiter, response_format_enum)
        params["tag"] = response_format_enum.chart_tag

        url = "datadownload"

        try:
            response = self._session.get(url, params)
        except ContentTooLarge:
            if time_display is not None:
                raise
            chunks = self._get_in_chunks(url, params, start_dt, end_dt, resolution)
            response = self._assemble_chunks(chunks, response_format_enum.platform)

        chart_class = self._RESPONSE_FORMAT_MAP.get(response_format_enum)

        return chart_class(
            response,
            self._session.build_url(url),
            params,
            response_format,
            code,
            start_dt,
            end_dt,
            resolution,
            time_zone,
            currency,
            min_avg_max,
            enable_settlement_period,
            time_display,
        )
