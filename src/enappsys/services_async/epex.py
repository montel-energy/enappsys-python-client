from __future__ import annotations

from datetime import date, datetime
from typing import Literal, overload

from enappsys.enum import ResponseFormatEnum, TimeZoneEnum
from enappsys.exceptions import ValidationError
from enappsys.services.epex import (
    EpexContractCSV,
    EpexContractJSON,
    EpexContractXML,
    EpexSettlementCSV,
    EpexSettlementJSON,
    EpexSettlementXML,
    _add_max_points,
    _normalize_settlement_inputs,
)
from enappsys.services_async.base import APIBaseAsync


class AsyncEpexContractAPI(APIBaseAsync):
    _RESPONSE_FORMAT_MAP = {
        ResponseFormatEnum.CSV: EpexContractCSV,
        ResponseFormatEnum.JSON: EpexContractJSON,
        ResponseFormatEnum.XML: EpexContractXML,
    }

    @overload
    async def get(
        self,
        response_format: Literal["csv"] | ResponseFormatEnum.CSV,
        code: str,
        contract: str,
        max_points: int | Literal["all"] = "all",
        time_zone: str | TimeZoneEnum = "CET",
    ) -> EpexContractCSV: ...

    @overload
    async def get(
        self,
        response_format: Literal["json"] | ResponseFormatEnum.JSON,
        code: str,
        contract: str,
        max_points: int | Literal["all"] = "all",
        time_zone: str | TimeZoneEnum = "CET",
    ) -> EpexContractJSON: ...

    @overload
    async def get(
        self,
        response_format: Literal["xml"] | ResponseFormatEnum.XML,
        code: str,
        contract: str,
        max_points: int | Literal["all"] = "all",
        time_zone: str | TimeZoneEnum = "CET",
    ) -> EpexContractXML: ...

    async def get(
        self,
        response_format: Literal["csv", "json", "xml"] | ResponseFormatEnum,
        code: str,
        contract: str,
        max_points: int | Literal["all"] = "all",
        time_zone: str | TimeZoneEnum = "CET",
    ) -> EpexContractCSV | EpexContractJSON | EpexContractXML:
        response_format_enum = self._get_response_format(response_format)
        params = {}
        self._add_code(params, code)
        self._add_code(params, contract, "contract")
        _add_max_points(params, max_points)
        self._add_time_zone(params, time_zone)
        self._add_resolution(params, "qh")
        if response_format_enum.epex_tag is None:
            raise ValidationError(
                reason="Provide one of 'csv', 'json', or 'xml'.",
                parameter="response_format",
            )
        params["tag"] = response_format_enum.epex_tag

        url = "apxdownload"
        response = await self._session.get(url, params)

        contract_class = self._RESPONSE_FORMAT_MAP[response_format_enum]
        return contract_class(
            response,
            url,
            params,
            response_format_enum,
            code,
            contract,
            max_points,
            time_zone,
        )


class AsyncEpexSettlementAPI(APIBaseAsync):
    _RESPONSE_FORMAT_MAP = {
        ResponseFormatEnum.CSV: EpexSettlementCSV,
        ResponseFormatEnum.JSON: EpexSettlementJSON,
        ResponseFormatEnum.XML: EpexSettlementXML,
    }

    @overload
    async def get(
        self,
        response_format: Literal["csv"] | ResponseFormatEnum.CSV,
        code: str,
        settlement_date: date | datetime | str | None = None,
        settlement_period: int | None = None,
        settlement_datetime: datetime | str | None = None,
        max_points: int | Literal["all"] = "all",
        time_zone: str | TimeZoneEnum = "CET",
    ) -> EpexSettlementCSV: ...

    @overload
    async def get(
        self,
        response_format: Literal["json"] | ResponseFormatEnum.JSON,
        code: str,
        settlement_date: date | datetime | str | None = None,
        settlement_period: int | None = None,
        settlement_datetime: datetime | str | None = None,
        max_points: int | Literal["all"] = "all",
        time_zone: str | TimeZoneEnum = "CET",
    ) -> EpexSettlementJSON: ...

    @overload
    async def get(
        self,
        response_format: Literal["xml"] | ResponseFormatEnum.XML,
        code: str,
        settlement_date: date | datetime | str | None = None,
        settlement_period: int | None = None,
        settlement_datetime: datetime | str | None = None,
        max_points: int | Literal["all"] = "all",
        time_zone: str | TimeZoneEnum = "CET",
    ) -> EpexSettlementXML: ...

    async def get(
        self,
        response_format: Literal["csv", "json", "xml"] | ResponseFormatEnum,
        code: str,
        settlement_date: date | datetime | str | None = None,
        settlement_period: int | None = None,
        settlement_datetime: datetime | str | None = None,
        max_points: int | Literal["all"] = "all",
        time_zone: str | TimeZoneEnum = "CET",
    ) -> EpexSettlementCSV | EpexSettlementJSON | EpexSettlementXML:
        response_format_enum = self._get_response_format(response_format)
        normalized_date, normalized_period, normalized_dt, start_dt, end_dt = _normalize_settlement_inputs(
            settlement_date,
            settlement_period,
            settlement_datetime,
        )

        params = {}
        self._add_code(params, code)
        self._add_dt(params, start_dt, "start", "start_dt")
        self._add_dt(params, end_dt, "end", "end_dt")
        _add_max_points(params, max_points)
        self._add_time_zone(params, time_zone)
        self._add_resolution(params, "qh")
        if response_format_enum.epex_tag is None:
            raise ValidationError(
                reason="Provide one of 'csv', 'json', or 'xml'.",
                parameter="response_format",
            )
        params["tag"] = response_format_enum.epex_tag

        url = "apxdownload"
        response = await self._session.get(url, params)

        settlement_class = self._RESPONSE_FORMAT_MAP[response_format_enum]
        return settlement_class(
            response,
            url,
            params,
            response_format_enum,
            code,
            normalized_date,
            normalized_period,
            normalized_dt,
            start_dt,
            end_dt,
            max_points,
            time_zone,
        )


class AsyncEpexAPI:
    def __init__(self, client):
        self._client = client
        self.contract = AsyncEpexContractAPI(client)
        self.settlement = AsyncEpexSettlementAPI(client)
