from __future__ import annotations

import io

from datetime import date, datetime, time, timedelta
from typing import Literal, TYPE_CHECKING, overload

from enappsys.enum import ResponseFormatEnum, TimeZoneEnum
from enappsys.exceptions import ValidationError
from enappsys.services.base import APIBase
from enappsys.utils import require_pandas, validate_rename_columns_length

if TYPE_CHECKING:
    import pandas as pd
    from enappsys import EnAppSys


def _parse_epex_csv(
    response,
    time_zone: str | TimeZoneEnum,
    tz_localize: bool = True,
    rename_columns: list | dict | None = None,
    unit_in_columns: bool = False,
) -> pd.DataFrame:
    pd = require_pandas()

    df = pd.read_csv(
        io.StringIO(response),
        header=[0, 1],
        index_col=0,
        parse_dates=True,
        date_format="[%d/%m/%Y %H:%M:%S]",
    )
    df.index.name = "dateTime"
    if tz_localize:
        df.index = df.index.tz_localize(TimeZoneEnum._from_value(time_zone).platform, ambiguous="infer")

    columns = df.columns.get_level_values(0).to_list()
    units = df.columns.get_level_values(1).to_list()
    if rename_columns or unit_in_columns:
        if isinstance(rename_columns, list):
            validate_rename_columns_length(rename_columns, columns, 1)

        for idx, column in enumerate(columns):
            column_name = column
            if isinstance(rename_columns, list):
                column_name = rename_columns[idx]
            elif isinstance(rename_columns, dict) and column in rename_columns:
                column_name = rename_columns[column]
            if unit_in_columns:
                column_name = f"{column_name} ({units[idx]})"
            columns[idx] = column_name

        df.columns = columns
    else:
        df.columns = columns

    return df


def _parse_epex_json(
    response: dict,
    time_zone: str | TimeZoneEnum,
    tz_localize: bool = True,
    rename_columns: list | dict | None = None,
    unit_in_columns: bool = False,
) -> pd.DataFrame:
    pd = require_pandas()

    df = pd.DataFrame(response["data"]).set_index("dateTime")
    df.index = pd.to_datetime(df.index, format="%Y-%m-%dT%H:%M:%S")
    if tz_localize:
        df.index = df.index.tz_localize(TimeZoneEnum._from_value(time_zone).platform, ambiguous="infer")
    df.index.name = "dateTime"

    metadata_items = response.get("metadata", {}).get("items", {})
    ordered_columns = [column for column in metadata_items.keys() if column in df.columns]
    if ordered_columns:
        df = df[ordered_columns]

    columns = list(df.columns)
    if rename_columns or unit_in_columns:
        if isinstance(rename_columns, list):
            validate_rename_columns_length(rename_columns, columns, 1)

        renamed_columns = []
        for idx, column in enumerate(columns):
            column_name = column
            if isinstance(rename_columns, list):
                column_name = rename_columns[idx]
            elif isinstance(rename_columns, dict) and column in rename_columns:
                column_name = rename_columns[column]
            if unit_in_columns:
                unit = metadata_items.get(column, {}).get("unit", "")
                column_name = f"{column_name} ({unit})" if unit else column_name
            renamed_columns.append(column_name)
        df.columns = renamed_columns

    return df


def _get_date(value: date | datetime | str, client_name: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValidationError(
                reason=f"Invalid date format: {value}. Expected format: YYYY-MM-DD or date",
                parameter=client_name,
            ) from exc
    raise ValidationError(
        reason="Provide a valid date, datetime, or str in YYYY-MM-DD format",
        parameter=client_name,
    )


def _add_max_points(params: dict, max_points: int | str, api_name: str = "max_points") -> int | str:
    if max_points == "all":
        params[api_name] = max_points
        return max_points
    if isinstance(max_points, int) and max_points > 0:
        params[api_name] = max_points
        return max_points
    raise ValidationError(
        reason="Provide a positive integer or 'all'.",
        parameter="max_points",
    )


def _normalize_settlement_inputs(
    settlement_date: date | datetime | str | None,
    settlement_period: int | None,
    settlement_datetime: datetime | str | None,
) -> tuple[date, int, datetime, datetime, datetime]:
    has_date_period = settlement_date is not None or settlement_period is not None
    has_datetime = settlement_datetime is not None

    if has_date_period and has_datetime:
        raise ValidationError(
            reason=(
                "Either provide both 'settlement_date' and 'settlement_period', "
                "or 'settlement_datetime' alone."
            ),
            parameter="settlement_datetime",
        )

    if has_datetime:
        normalized_dt = APIBase._get_dt(settlement_datetime, "settlement_datetime")
        normalized_dt = normalized_dt.replace(
            minute=(normalized_dt.minute // 15) * 15,
            second=0,
            microsecond=0,
        )
        normalized_date = normalized_dt.date()
        normalized_period = normalized_dt.hour * 4 + normalized_dt.minute // 15 + 1
        return (
            normalized_date,
            normalized_period,
            normalized_dt,
            normalized_dt,
            normalized_dt,
        )

    if settlement_date is None or settlement_period is None:
        raise ValidationError(
            reason=(
                "Provide both 'settlement_date' and 'settlement_period', "
                "or 'settlement_datetime' alone."
            ),
            parameter="settlement_date",
        )
    if not isinstance(settlement_period, int) or not 1 <= settlement_period <= 96:
        raise ValidationError(
            reason="Provide an integer between 1 and 96.",
            parameter="settlement_period",
        )

    normalized_date = _get_date(settlement_date, "settlement_date")
    normalized_dt = datetime.combine(normalized_date, time.min) + timedelta(
        minutes=15 * (settlement_period - 1)
    )
    return (
        normalized_date,
        settlement_period,
        normalized_dt,
        normalized_dt,
        normalized_dt,
    )


class EpexContractBase:
    def __init__(
        self,
        response,
        url,
        params,
        response_format,
        code,
        contract,
        max_points,
        time_zone,
    ):
        self.response = response
        self.url = url
        self.params = params
        self.response_format = response_format
        self.code = code
        self.contract = contract
        self.max_points = max_points
        self.time_zone = time_zone


class EpexContractCSV(EpexContractBase):
    def to_df(
        self,
        tz_localize: bool = True,
        rename_columns: list | dict | None = None,
        unit_in_columns: bool = False,
    ) -> pd.DataFrame:
        return _parse_epex_csv(
            self.response,
            self.time_zone,
            tz_localize,
            rename_columns,
            unit_in_columns,
        )


class EpexContractJSON(EpexContractBase):
    def to_df(
        self,
        tz_localize: bool = True,
        rename_columns: list | dict | None = None,
        unit_in_columns: bool = False,
    ) -> pd.DataFrame:
        return _parse_epex_json(
            self.response,
            self.time_zone,
            tz_localize,
            rename_columns,
            unit_in_columns,
        )


class EpexContractXML(EpexContractBase):
    pass


class EpexSettlementBase:
    def __init__(
        self,
        response,
        url,
        params,
        response_format,
        code,
        settlement_date,
        settlement_period,
        settlement_datetime,
        start_dt,
        end_dt,
        max_points,
        time_zone,
    ):
        self.response = response
        self.url = url
        self.params = params
        self.response_format = response_format
        self.code = code
        self.settlement_date = settlement_date
        self.settlement_period = settlement_period
        self.settlement_datetime = settlement_datetime
        self.start_dt = start_dt
        self.end_dt = end_dt
        self.max_points = max_points
        self.time_zone = time_zone


class EpexSettlementCSV(EpexSettlementBase):
    def to_df(
        self,
        tz_localize: bool = True,
        rename_columns: list | dict | None = None,
        unit_in_columns: bool = False,
    ) -> pd.DataFrame:
        return _parse_epex_csv(
            self.response,
            self.time_zone,
            tz_localize,
            rename_columns,
            unit_in_columns,
        )


class EpexSettlementJSON(EpexSettlementBase):
    def to_df(
        self,
        tz_localize: bool = True,
        rename_columns: list | dict | None = None,
        unit_in_columns: bool = False,
    ) -> pd.DataFrame:
        return _parse_epex_json(
            self.response,
            self.time_zone,
            tz_localize,
            rename_columns,
            unit_in_columns,
        )


class EpexSettlementXML(EpexSettlementBase):
    pass


class EpexContractAPI(APIBase):
    _RESPONSE_FORMAT_MAP = {
        ResponseFormatEnum.CSV: EpexContractCSV,
        ResponseFormatEnum.JSON: EpexContractJSON,
        ResponseFormatEnum.XML: EpexContractXML,
    }

    @overload
    def get(
        self,
        response_format: Literal["csv"] | ResponseFormatEnum.CSV,
        code: str,
        contract: str,
        max_points: int | Literal["all"] = "all",
        time_zone: str | TimeZoneEnum = "CET",
    ) -> EpexContractCSV: ...

    @overload
    def get(
        self,
        response_format: Literal["json"] | ResponseFormatEnum.JSON,
        code: str,
        contract: str,
        max_points: int | Literal["all"] = "all",
        time_zone: str | TimeZoneEnum = "CET",
    ) -> EpexContractJSON: ...

    @overload
    def get(
        self,
        response_format: Literal["xml"] | ResponseFormatEnum.XML,
        code: str,
        contract: str,
        max_points: int | Literal["all"] = "all",
        time_zone: str | TimeZoneEnum = "CET",
    ) -> EpexContractXML: ...

    def get(
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
        response = self._session.get(url, params)

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


class EpexSettlementAPI(APIBase):
    _RESPONSE_FORMAT_MAP = {
        ResponseFormatEnum.CSV: EpexSettlementCSV,
        ResponseFormatEnum.JSON: EpexSettlementJSON,
        ResponseFormatEnum.XML: EpexSettlementXML,
    }

    @overload
    def get(
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
    def get(
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
    def get(
        self,
        response_format: Literal["xml"] | ResponseFormatEnum.XML,
        code: str,
        settlement_date: date | datetime | str | None = None,
        settlement_period: int | None = None,
        settlement_datetime: datetime | str | None = None,
        max_points: int | Literal["all"] = "all",
        time_zone: str | TimeZoneEnum = "CET",
    ) -> EpexSettlementXML: ...

    def get(
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
        response = self._session.get(url, params)

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


class EpexAPI:
    def __init__(self, client: EnAppSys):
        self._client = client
        self.contract = EpexContractAPI(client)
        self.settlement = EpexSettlementAPI(client)
