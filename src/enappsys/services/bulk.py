from __future__ import annotations

import io
import logging

from datetime import datetime
from typing import Any, Dict, Literal, overload, TYPE_CHECKING, Union

from .base import APIBase, JSONBase, JSONMapBase
from ..enum import (
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


class BulkBase:
    def __init__(
        self,
        response,
        url,
        params,
        response_format,
        data_type,
        entities,
        start_dt,
        end_dt,
        resolution,
        time_zone,
        min_avg_max,
    ):
        self.response = response
        self.url = url
        self.params = params
        self.response_format = response_format
        self.data_type = data_type
        self.entities = entities
        self.start_dt = start_dt
        self.end_dt = end_dt
        self.resolution = resolution
        self.time_zone = time_zone
        self.min_avg_max = min_avg_max


class BulkCSV(BulkBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def to_df(
        self,
        tz_localize: bool = True,
        rename_columns: list | dict | None = None,
        data_type_in_columns: bool = False,
        unit_in_columns: bool = False,
        remove_units_column: bool = True,
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
        data_type_in_columns: bool, optional
            If True, prepends the data type: "<data_type>.<column_name>".
            Default is False.
        unit_in_columns : bool, optional
            If True, includes units: "<column_name> (<unit>)". Default is False.
        remove_units_column: bool, optional
            If True, remove "Units" column. Default is True.

        Returns
        -------
        pandas.DataFrame
            Processed API response formatted as a `pandas.DataFrame`.
        """
        import pandas as pd

        # TODO: Determine to include seconds manually
        df = pd.read_csv(
            io.StringIO(self.response),
            index_col=0,
            parse_dates=True,
            date_format="%d/%m/%Y %H:%M",
        )
        df.index.name = "dateTime"
        if tz_localize:
            df.index = df.index.tz_localize(self.time_zone, ambiguous="infer")

        step_size = 1
        if self.min_avg_max:
            step_size = 3

        if rename_columns or data_type_in_columns or unit_in_columns:
            columns = list(df.columns)

            if isinstance(rename_columns, list):
                # [:1] to not include Units when comparing with rename_columns
                validate_rename_columns_length(rename_columns, columns[1:], step_size)

            # Units are in first column, for every row, so take first
            units_colon_delimited = df.iloc[0, 0]
            if not pd.isnull(units_colon_delimited):
                units = units_colon_delimited.split(":")
                if len(units) == 1:
                    units = units * ((len(columns) - 1) // step_size)
            else:
                units = [""] * ((len(columns) - 1) // step_size)

            # Add datatype to "Units" column
            if data_type_in_columns:
                columns = [f"{self.data_type}.Units"] + columns[1:]

            for idx in range(1, len(columns), step_size):
                entity = columns[idx]
                column_name = entity.rsplit(" (MIN)")[0]

                if isinstance(rename_columns, list):
                    column_name = rename_columns[(idx - 1) // step_size]
                elif isinstance(rename_columns, dict) and column_name in rename_columns:
                    column_name = rename_columns[column_name]
                if data_type_in_columns:
                    column_name = f"{self.data_type}.{column_name}"
                if unit_in_columns:
                    column_name = f"{column_name} ({units[(idx - 1) // step_size]})"

                if self.min_avg_max:
                    columns[idx] = f"{column_name} (MIN)"
                    columns[idx + 1] = f"{column_name} (AV)"
                    columns[idx + 2] = f"{column_name} (MAX)"
                else:
                    columns[idx] = column_name

            df.columns = columns

        if remove_units_column:
            df = df.iloc[:, 1:]

        return df


class BulkJSON(BulkBase, JSONBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class BulkJSONMap(BulkBase, JSONMapBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class BulkXML(BulkBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class BulkAPI(APIBase):
    """Service providing access to the Bulk API."""

    _RESPONSE_FORMAT_MAP = {
        ResponseFormatEnum.CSV: BulkCSV,
        ResponseFormatEnum.JSON: BulkJSON,
        ResponseFormatEnum.JSON_MAP: BulkJSONMap,
        ResponseFormatEnum.XML: BulkXML,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @overload
    def get(
        self,
        response_format: Literal["csv", ResponseFormatEnum.CSV],
        data_type: str,
        entities: list,
        start_dt: Union[datetime, str],
        end_dt: Union[datetime, str],
        resolution: ResolutionEnum,
        time_zone: TimeZoneEnum,
        min_avg_max: bool = False,
        delimiter: DelimiterEnum = "comma",
    ) -> BulkCSV: ...

    @overload
    def get(
        self,
        response_format: Literal["json", ResponseFormatEnum.JSON],
        data_type: str,
        entities: list,
        start_dt: Union[datetime, str],
        end_dt: Union[datetime, str],
        resolution: ResolutionEnum,
        time_zone: TimeZoneEnum,
        min_avg_max: bool = False,
    ) -> BulkJSON: ...

    @overload
    def get(
        self,
        response_format: Literal["json_map", ResponseFormatEnum.JSON_MAP],
        data_type: str,
        entities: list,
        start_dt: Union[datetime, str],
        end_dt: Union[datetime, str],
        resolution: ResolutionEnum,
        time_zone: TimeZoneEnum,
        min_avg_max: bool = False,
    ) -> BulkJSONMap: ...

    @overload
    def get(
        self,
        response_format: Literal["xml", ResponseFormatEnum.XML],
        data_type: str,
        entities: list,
        start_dt: Union[datetime, str],
        end_dt: Union[datetime, str],
        resolution: ResolutionEnum,
        time_zone: TimeZoneEnum,
        min_avg_max: bool = False,
    ) -> BulkXML: ...

    def get(
        self,
        response_format: ResponseFormatEnum,
        data_type: str,
        entities: list,
        start_dt: Union[datetime, str],
        end_dt: Union[datetime, str],
        resolution: ResolutionEnum,
        time_zone: TimeZoneEnum,
        min_avg_max: bool = False,
        delimiter: DelimiterEnum = "comma",
    ) -> Union[BulkCSV, BulkJSON, BulkJSONMap, BulkXML]:
        response_format_enum = self._get_response_format(response_format)
        params: Dict[str, Any] = {}
        self._add_data_type(params, data_type)
        self._add_entities(params, entities)
        self._add_dt(params, start_dt, "start", "start_dt")
        self._add_dt(params, end_dt, "end", "end_dt")
        self._add_resolution(params, resolution)
        self._add_time_zone(params, time_zone)
        self._add_min_avg_max(params, min_avg_max)
        self._add_delimiter(params, delimiter, response_format_enum)

        url = response_format_enum.bulk_url

        try:
            response = self._session.get(url, params)
        except ContentTooLarge as e:
            chunks = self._get_in_chunks(url, params, start_dt, end_dt, resolution)
            response = self._assemble_chunks(chunks, response_format_enum.platform)

        bulk_class = self._RESPONSE_FORMAT_MAP.get(response_format_enum)

        return bulk_class(
            response,
            url,
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
