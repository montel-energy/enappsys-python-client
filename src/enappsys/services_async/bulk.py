from __future__ import annotations

from datetime import datetime
from typing import Literal, overload

from enappsys.enum import (
    DelimiterEnum,
    ResponseFormatEnum,
    ResolutionEnum,
    TimeZoneEnum,
)
from enappsys.exceptions import ContentTooLarge
from enappsys.services_async.base import APIBaseAsync
from enappsys.services.bulk import BulkCSV, BulkJSON, BulkJSONMap, BulkXML


class AsyncBulkAPI(APIBaseAsync):
    _RESPONSE_FORMAT_MAP = {
        ResponseFormatEnum.CSV: BulkCSV,
        ResponseFormatEnum.JSON: BulkJSON,
        ResponseFormatEnum.JSON_MAP: BulkJSONMap,
        ResponseFormatEnum.XML: BulkXML,
    }

    @overload
    async def get(
        self,
        response_format: Literal["csv"] | ResponseFormatEnum.CSV,
        data_type: str,
        entities: list,
        start_dt: str | datetime,
        end_dt: str | datetime,
        resolution: str | ResolutionEnum,
        time_zone: str | TimeZoneEnum,
        min_avg_max: bool = False,
        delimiter: str | DelimiterEnum = "comma",
    ) -> BulkCSV: ...

    @overload
    async def get(
        self,
        response_format: Literal["json"] | ResponseFormatEnum.JSON,
        data_type: str,
        entities: list,
        start_dt: str | datetime,
        end_dt: str | datetime,
        resolution: str | ResolutionEnum,
        time_zone: str | TimeZoneEnum,
        min_avg_max: bool = False,
    ) -> BulkJSON: ...
    
    @overload
    async def get(
        self,
        response_format: Literal["json_map"] | ResponseFormatEnum.JSON_MAP,
        data_type: str,
        entities: list,
        start_dt: str | datetime,
        end_dt: str | datetime,
        resolution: str | ResolutionEnum,
        time_zone: str | TimeZoneEnum,
        min_avg_max: bool = False,
    ) -> BulkJSONMap: ...

    @overload
    async def get(
        self,
        response_format: Literal["xml"] | ResponseFormatEnum.XML,
        data_type: str,
        entities: list,
        start_dt: str | datetime,
        end_dt: str | datetime,
        resolution: str | ResolutionEnum,
        time_zone: str | TimeZoneEnum,
        min_avg_max: bool = False,
    ) -> BulkXML: ...

    async def get(
        self,
        response_format: Literal["csv", "json", "json_map", "xml"] | ResponseFormatEnum,
        data_type: str,
        entities: list,
        start_dt: str | datetime,
        end_dt: str | datetime,
        resolution: str | ResolutionEnum,
        time_zone: str | TimeZoneEnum,
        min_avg_max: bool = False,
        delimiter: str | DelimiterEnum = "comma",
    ) -> BulkCSV | BulkJSON | BulkJSONMap | BulkXML:
        response_format = self._get_response_format(response_format)
        params = {}
        self._add_data_type(params, data_type)
        self._add_entities(params, entities)
        self._add_dt(params, start_dt, "start", "start_dt")
        self._add_dt(params, end_dt, "end", "end_dt")
        self._add_resolution(params, resolution)
        self._add_time_zone(params, time_zone)
        self._add_min_avg_max(params, min_avg_max)
        self._add_delimiter(params, delimiter, response_format)

        url = response_format.bulk_url

        try:
            response = await self._session.get(url, params)
        except ContentTooLarge:
            chunks = await self._get_in_chunks_async(
                url, params, start_dt, end_dt, resolution
            )
            response = self._assemble_chunks(chunks, response_format.platform)

        bulk_class = self._RESPONSE_FORMAT_MAP[response_format]
        return bulk_class(
            response,
            self._session.build_url(url),
            params,
            response_format,
            data_type,
            entities,
            start_dt,
            end_dt,
            resolution,
            time_zone,
            min_avg_max,
        )
