from __future__ import annotations

import io
import logging

from datetime import datetime
from typing import Any, Dict, Literal, overload, TYPE_CHECKING, Union

from .base import APIBase, JSONBase, JSONMapBase
from ..enum import (
    CurrencyEnum,
    DelimiterEnum,
    ResponseFormatEnum,
    ResolutionEnum,
    TimeZoneEnum,
)
from ..exceptions import ContentTooLarge
from ..utils import validate_rename_columns_length

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)


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
            If a list, provide new names for all entities.
            If a dict, specify original entity names as keys and new names as values.
            Default is None.
        unit_in_columns : bool, optional
            If True, includes units: "<column_name> (<unit>)". Default is False.

        Returns
        -------
        pandas.DataFrame
            Processed API response formatted as a `pandas.DataFrame`.
        """
        import pandas as pd

        # TODO: Determine to include seconds manually
        df = pd.read_csv(
            io.StringIO(self.response),
            header=[0, 1],
            index_col=0,
            parse_dates=True,
            date_format="[%d/%m/%Y %H:%M]",
        )
        df.index.name = "dateTime"
        if tz_localize:
            df.index = df.index.tz_localize(self.time_zone, ambiguous="infer")

        step_size = 1
        if self.min_avg_max:
            step_size = 3

        columns = df.columns.get_level_values(0).to_list()
        units = df.columns.get_level_values(1).to_list()
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
        response_format: Literal["csv", ResponseFormatEnum.CSV],
        code: str,
        start_dt: Union[datetime, str],
        end_dt: Union[datetime, str],
        resolution: ResolutionEnum,
        time_zone: TimeZoneEnum,
        currency: CurrencyEnum,
        min_avg_max: bool,
        delimiter: DelimiterEnum = "comma",
    ) -> ChartCSV: ...

    @overload
    def get(
        self,
        response_format: Literal["json", ResponseFormatEnum.JSON],
        code: str,
        start_dt: Union[datetime, str],
        end_dt: Union[datetime, str],
        resolution: ResolutionEnum,
        time_zone: TimeZoneEnum,
        currency: CurrencyEnum,
        min_avg_max: bool,
    ) -> ChartJSON: ...

    @overload
    def get(
        self,
        response_format: Literal["json_map", ResponseFormatEnum.JSON_MAP],
        code: str,
        start_dt: Union[datetime, str],
        end_dt: Union[datetime, str],
        resolution: ResolutionEnum,
        time_zone: TimeZoneEnum,
        currency: CurrencyEnum,
        min_avg_max: bool,
    ) -> ChartJSONMap: ...

    @overload
    def get(
        self,
        response_format: Literal["xml", ResponseFormatEnum.XML],
        code: str,
        start_dt: Union[datetime, str],
        end_dt: Union[datetime, str],
        resolution: ResolutionEnum,
        time_zone: TimeZoneEnum,
        currency: CurrencyEnum,
        min_avg_max: bool,
    ) -> ChartXML: ...

    def get(
        self,
        response_format: ResponseFormatEnum,
        code: str,
        start_dt: Union[datetime, str],
        end_dt: Union[datetime, str],
        resolution: ResolutionEnum,
        time_zone: TimeZoneEnum = "UTC",
        currency: CurrencyEnum = "EUR",
        min_avg_max: bool = False,
        delimiter: DelimiterEnum = "comma",
    ) -> Union[ChartCSV, ChartJSON, ChartJSONMap, ChartXML]:
        response_format_enum = self._get_response_format(response_format)
        params: Dict[str, Any] = {}
        self._add_code(params, code)
        self._add_dt(params, start_dt, "start", "start_dt")
        self._add_dt(params, end_dt, "end", "end_dt")
        self._add_resolution(params, resolution)
        self._add_time_zone(params, time_zone)
        self._add_currency(params, currency)
        self._add_min_avg_max(params, min_avg_max)
        self._add_delimiter(params, delimiter, response_format_enum)
        params["tag"] = response_format_enum.chart_tag

        url = "datadownload"

        try:
            response = self._session.get(url, params)
        except ContentTooLarge as e:
            chunks = self._get_in_chunks(url, params, start_dt, end_dt, resolution)
            pass

        chart_class = self._RESPONSE_FORMAT_MAP.get(response_format_enum)

        return chart_class(
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
        )
