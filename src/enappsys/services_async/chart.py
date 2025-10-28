from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Literal, overload, Union

from .base import APIBaseAsync
from ..services.chart import ChartCSV, ChartJSON, ChartJSONMap, ChartXML
from ..enum import (
    CurrencyEnum,
    DelimiterEnum,
    ResolutionEnum,
    ResponseFormatEnum,
    TimeZoneEnum,
)
from ..exceptions import ContentTooLarge


class AsyncChartAPI(APIBaseAsync):
    _RESPONSE_FORMAT_MAP = {
        ResponseFormatEnum.CSV: ChartCSV,
        ResponseFormatEnum.JSON: ChartJSON,
        ResponseFormatEnum.JSON_MAP: ChartJSONMap,
        ResponseFormatEnum.XML: ChartXML,
    }

    @overload
    async def get(
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
    async def get(
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
    async def get(
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
    async def get(
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

    async def get(
        self,
        response_format: Union[str, ResponseFormatEnum],
        code: str,
        start_dt,
        end_dt,
        resolution: ResolutionEnum,
        time_zone: TimeZoneEnum = "UTC",
        currency: CurrencyEnum = "EUR",
        min_avg_max: bool = False,
        delimiter: DelimiterEnum = "comma",
    ) -> Union[ChartCSV, ChartJSON, ChartJSONMap, ChartXML]:
        response_format = self._get_response_format(response_format)
        params: Dict[str, Any] = {}
        self._add_code(params, code)
        self._add_dt(params, start_dt, "start", "start_dt")
        self._add_dt(params, end_dt, "end", "end_dt")
        self._add_resolution(params, resolution)
        self._add_time_zone(params, time_zone)
        self._add_currency(params, currency)
        self._add_min_avg_max(params, min_avg_max)
        self._add_delimiter(params, delimiter, response_format)
        params["tag"] = response_format.chart_tag

        url = "datadownload"

        try:
            response = await self._session.get(url, params)
        except ContentTooLarge:
            chunks = await self._get_in_chunks_async(
                url, params, start_dt, end_dt, resolution
            )
            response = self._assemble_chunks(chunks, response_format.platform)

        chart_class = self._RESPONSE_FORMAT_MAP[response_format]
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
