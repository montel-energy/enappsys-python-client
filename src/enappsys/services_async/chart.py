from __future__ import annotations

from datetime import datetime
from typing import Literal, overload

from enappsys.enum import (
    CurrencyEnum,
    DelimiterEnum,
    ResolutionEnum,
    ResponseFormatEnum,
    TimeZoneEnum,
)
from enappsys.exceptions import ContentTooLarge
from enappsys.services_async.base import APIBaseAsync
from enappsys.services.chart import ChartCSV, ChartJSON, ChartJSONMap, ChartXML


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
    async def get(
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
    async def get(
        self,
        response_format: Literal["json_map"] | ResponseFormatEnum.JSON_MAP,
        code: str,
        start_dt: str | datetime,
        end_dt: str | datetime,
        resolution: ResolutionEnum,
        time_zone: TimeZoneEnum,
        currency: CurrencyEnum,
        min_avg_max: bool,
    ) -> ChartJSONMap: ...
    
    @overload
    async def get(
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

    async def get(
        self,
        response_format: Literal["csv", "json", "json_map", "xml"] | ResponseFormatEnum,
        code: str,
        start_dt: str | datetime,
        end_dt: str | datetime,
        resolution: str | ResolutionEnum,
        time_zone: str | TimeZoneEnum = "UTC",
        currency: str | CurrencyEnum = "EUR",
        min_avg_max: bool = False,
        delimiter: str | DelimiterEnum = "comma",
    ) -> ChartCSV | ChartJSON | ChartJSONMap | ChartXML:
        response_format = self._get_response_format(response_format)
        params = {}
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
        )
