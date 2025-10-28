from __future__ import annotations

import io
import logging

from datetime import datetime
from typing import Any, Dict, Literal, overload, TYPE_CHECKING, Union

from .base import APIBase
from ..enum import (
    CurrencyEnum,
    DelimiterEnum,
    ResponseFormatEnum,
    TimeZoneEnum,
)
from ..utils import validate_rename_columns_length

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)


class PriceVolumeCurveBase:
    def __init__(
        self,
        response,
        url,
        params,
        response_format,
        code,
        dt,
        time_zone,
        currency,
    ):
        self.response = response
        self.url = url
        self.params = params
        self.response_format = response_format
        self.code = code
        self.dt = dt
        self.time_zone = time_zone
        self.currency = currency


class PriceVolumeCurveCSV(PriceVolumeCurveBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def to_df(
        self,
        dt_index: bool = True,
        rename_columns: list | dict | None = None,
        unit_in_columns: bool = False,
    ) -> pd.DataFrame:
        """Process the CSV API data format into a ``pandas.DataFrame``.

        Parameters
        ----------
        dt_index : bool, optional
            If True, sets passed datetime as the DataFrame index. Default is True.
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

        df = pd.read_csv(
            io.StringIO(self.response),
            header=[0, 1],
        )

        if dt_index:
            df.index = pd.DatetimeIndex([self.dt] * len(df))

        step_size = 1
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
                
                columns[idx] = column_name

        df.columns = columns

        return df
    

class PriceVolumeCurveXML(PriceVolumeCurveBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class PriceVolumeCurveAPI(APIBase):
    _RESPONSE_FORMAT_MAP = {
        ResponseFormatEnum.CSV: PriceVolumeCurveCSV,
        ResponseFormatEnum.XML: PriceVolumeCurveXML,
    }

    @overload
    def get(
        self,
        response_format: Literal["csv", ResponseFormatEnum.CSV],
        code: str,
        dt: Union[datetime, str],
        time_zone: TimeZoneEnum,
        currency: CurrencyEnum,
        delimiter: DelimiterEnum = "comma",
    ) -> PriceVolumeCurveCSV: ...
    
    @overload
    def get(
        self,
        response_format: Literal["xml", ResponseFormatEnum.XML],
        code: str,
        dt: Union[datetime, str],
        time_zone: TimeZoneEnum,
        currency: CurrencyEnum,
    ) -> PriceVolumeCurveXML: ...

    def get(
        self,
        response_format: Union[str, ResponseFormatEnum],
        code: str,
        dt: Union[datetime, str],
        time_zone: TimeZoneEnum,
        currency: CurrencyEnum,
        delimiter: DelimiterEnum = "comma",
    ) -> Union[PriceVolumeCurveCSV, PriceVolumeCurveXML]:
        response_format = self._get_response_format(response_format)
        params: Dict[str, Any] = {}
        self._add_code(params, code)
        dt = self._add_dt(params, dt, "minperiod", "dt")
        self._add_time_zone(params, time_zone)
        self._add_currency(params, currency)
        self._add_delimiter(params, delimiter, response_format)
        params["tag"] = response_format.chart_tag

        url = "datadownload"

        response = self._session.get(url, params)

        price_volume_curve_class = self._RESPONSE_FORMAT_MAP[response_format]
        return price_volume_curve_class(
            response,
            url,
            params,
            response_format,
            code,
            dt,
            time_zone,
            currency
        )
