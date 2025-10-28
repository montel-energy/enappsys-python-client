from __future__ import annotations

import asyncio

from datetime import datetime
from typing import Any, Dict, Literal, overload, TYPE_CHECKING, Union

from .base import APIBaseAsync
from ..services.price_volume_curve import PriceVolumeCurveCSV, PriceVolumeCurveXML
from ..enum import (
    CurrencyEnum,
    DelimiterEnum,
    ResponseFormatEnum,
    TimeZoneEnum,
)

if TYPE_CHECKING:
    import pandas as pd


class AsyncPriceVolumeCurveAPI(APIBaseAsync):
    _RESPONSE_FORMAT_MAP = {
        ResponseFormatEnum.CSV: PriceVolumeCurveCSV,
        ResponseFormatEnum.XML: PriceVolumeCurveXML,
    }

    @overload
    async def get(
        self,
        response_format: Literal["csv", ResponseFormatEnum.CSV],
        code: str,
        dt: Union[datetime, str],
        time_zone: TimeZoneEnum,
        currency: CurrencyEnum,
        delimiter: DelimiterEnum = "comma",
    ) -> PriceVolumeCurveCSV: ...
    
    @overload
    async def get(
        self,
        response_format: Literal["xml", ResponseFormatEnum.XML],
        code: str,
        dt: Union[datetime, str],
        time_zone: TimeZoneEnum,
        currency: CurrencyEnum,
    ) -> PriceVolumeCurveXML: ...

    async def get(
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
        self._add_dt(params, dt, "minperiod", "dt")
        self._add_time_zone(params, time_zone)
        self._add_currency(params, currency)
        self._add_delimiter(params, delimiter, response_format)
        params["tag"] = response_format.chart_tag

        url = "datadownload"

        response = await self._session.get(url, params)

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

    async def get_multiple(
        self,
        response_format: str,
        code: str,
        start_dt: str,
        end_dt: str,
        product: str,
        time_zone: str = "CET",
        currency: str = "EUR",
        rename_columns: list | dict | None = None,
        unit_in_columns: bool = False,
    ) -> pd.DataFrame:
        """Fetch multiple price-volume curves concurrently and process into a DataFrame."""
        import pandas as pd
        dt_range = pd.date_range(start=start_dt, end=end_dt, freq=product, inclusive="left")

        async def fetch_one(dt):
            resp = await self.get(
                response_format,
                code=code,
                dt=dt.strftime("%Y-%m-%dT%H:%M"),
                time_zone=time_zone,
                currency=currency,
            )
            return resp.to_df(
                rename_columns=rename_columns,
                unit_in_columns=unit_in_columns,
            )

        tasks = [asyncio.create_task(fetch_one(dt)) for dt in dt_range]
        results = await asyncio.gather(*tasks)
        return pd.concat(results).sort_index()
