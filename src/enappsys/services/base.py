from __future__ import annotations

import copy
import logging

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Union

from ..enum import (
    AppEnvEnum,
    CurrencyEnum,
    DelimiterEnum,
    ResolutionEnum,
    ResponseFormatEnum,
    TimeZoneEnum,
)
from ..exceptions import ValidationError
from ..utils import dt_series_format

if TYPE_CHECKING:
    import pandas as pd
    from ..client import EnAppSys

logger = logging.getLogger(__name__)


class APIBase:
    API_MAX_ROWS = 150000
    
    def __init__(self, client: EnAppSys):
        self._client = client
        self._session = client._session

    @staticmethod
    def _get_app_env(app_env: Union[str, AppEnvEnum]) -> AppEnvEnum:
        return AppEnvEnum._from_value(app_env).app_env_url

    @staticmethod
    def _get_response_format(
        response_format: Union[str, ResponseFormatEnum],
    ) -> ResponseFormatEnum:
        return ResponseFormatEnum._from_value(response_format)

    @staticmethod
    def _add_resolution(
        params, resolution: Union[str, ResolutionEnum], api_name: str = "res"
    ):
        params[api_name] = ResolutionEnum._from_value(resolution).platform

    @staticmethod
    def _add_time_zone(
        params, time_zone: Union[str, TimeZoneEnum], api_name: str = "timezone"
    ):
        params[api_name] = TimeZoneEnum._from_value(time_zone).platform

    @staticmethod
    def _add_currency(
        params, currency: Union[str, CurrencyEnum], api_name: str = "currency"
    ):
        params[api_name] = CurrencyEnum._from_value(currency).platform

    @staticmethod
    def _add_data_type(params, data_type: str, api_name: str = "type"):
        if isinstance(data_type, str):
            params[api_name] = data_type
        else:
            raise ValidationError(reason="Provide a valid str", parameter="data_type")

    @staticmethod
    def _add_entities(params, entities: Union[str, list], api_name: str = "entities"):
        if isinstance(entities, str):
            params[api_name] = [entities]
        elif isinstance(entities, list):
            params[api_name] = entities
        else:
            raise ValidationError(reason="Provide a valid list", parameter="entities")

    @staticmethod
    def _add_code(params, code: str, api_name: str = "code"):
        if isinstance(code, str):
            params[api_name] = code
        else:
            raise ValidationError(reason="Provide a valid str", parameter="code")

    @staticmethod
    def _add_dt(params, dt: Union[datetime, str], api_name: str, client_name: str) -> datetime:
        """Add datetime to params and return the datetime object."""
        dt_obj = APIBase._get_dt(dt, client_name)
        params[api_name] = dt_obj.strftime("%Y%m%d%H%M")
        return dt_obj

    @staticmethod
    def _add_min_avg_max(params, min_avg_max: bool, api_name: str = "minavmax"):
        if isinstance(min_avg_max, bool):
            params[api_name] = str(min_avg_max).lower()
        else:
            raise ValidationError(reason="Provide a boolean", parameter="min_avg_max")

    @staticmethod
    def _add_delimiter(
        params,
        delimiter: Union[str, DelimiterEnum],
        response_format_enum: ResponseFormatEnum,
        api_name: str = "delimiter",
    ):
        if response_format_enum == ResponseFormatEnum.CSV:
            params[api_name] = DelimiterEnum._from_value(delimiter).platform

    @staticmethod
    def _get_dt(dt: Union[datetime, str], client_name: str) -> datetime:
        """Convert datetime or string to datetime object.
        
        Parameters
        ----------
        dt : datetime or str
            Datetime object or string in YYYY-MM-DDTHH:MM format.
        client_name : str
            Parameter name for error reporting.
            
        Returns
        -------
        datetime
            Parsed datetime object.
        """
        if isinstance(dt, datetime):
            return dt
        elif isinstance(dt, str):
            try:
                return datetime.strptime(dt, "%Y-%m-%dT%H:%M")
            except ValueError:
                raise ValidationError(
                    reason=f"Invalid datetime format: {dt}. Expected format: YYYY-MM-DDTHH:MM or datetime",
                    parameter=client_name,
                )
        else:
            raise ValidationError(
                reason="Provide a valid datetime or str in YYYY-MM-DDTHH:MM format",
                parameter=client_name,
            )
      
    def _get_in_chunks(
        self,
        url,
        params,
        start_dt: Union[datetime, str],
        end_dt: Union[datetime, str],
        resolution: ResolutionEnum,
    ):
        start_dt_obj = self._get_dt(start_dt, "start_dt")
        end_dt_obj = self._get_dt(end_dt, "end_dt")
        
        data_chunks = []
        delta = timedelta(**ResolutionEnum._from_value(resolution).delta)
        chunk_params = copy.deepcopy(params)
        chunk_start_dt = copy.deepcopy(start_dt_obj)  # Now guaranteed to be datetime
        
        while chunk_start_dt < end_dt_obj:
            chunk_end_dt = chunk_start_dt + self.API_MAX_ROWS * delta
            if chunk_end_dt > end_dt_obj:
                chunk_end_dt = end_dt_obj
                
            # Update chunk parameters with new date range
            self._add_dt(chunk_params, chunk_start_dt, "start", "start_dt")
            self._add_dt(chunk_params, chunk_end_dt, "end", "end_dt")
            chunk_start_dt = chunk_end_dt

            # Make the chunked request
            response = self._session.get(url, chunk_params)
            data_chunks.append(response)

        return data_chunks
    
    def _assemble_chunks(self, chunks: list, response_type: str):
        for count, data_chunk in enumerate(chunks):
            if count == 0:
                data = data_chunk
            elif response_type == "json":
                data["data"].extend(data_chunk["data"])
            elif response_type == "json_map":
                data["data"].update(data_chunk["data"])
            elif response_type == "csv":
                # Remove header row before appending
                data += data_chunk.split("\n", 1)[1]
            # XML
            else:
                data = chunks
        return data


class JSONBase:
    def to_df(
        self,
        timestamp: bool = False,
        last_updated: bool = False,
        convert_timestamps: bool = False,
        tz_localize: bool = True,
        rename_columns: list | dict | None = None,
        data_type_in_columns: bool = False,
        unit_in_columns: bool = False,
    ) -> pd.DataFrame:
        """
        Process ``json`` response type into a ``pandas.DataFrame``.

        Parameters
        ----------
        timestamp: bool,  optional
            Indicates when the data was first entered into the database or
            created as a forecast, in UTC. Set to True to include this
            column in the DataFrame. Default is False.
        last_updated: bool, optional
            Represents the last time the data was updated in the database, in UTC.
            Set to True to include this column in the DataFrame. Default is False.
        convert_timestamps: bool
            If True, converts 'timestamp' and 'lastUpdated' columns from UTC to
            the time zone specified in the data request.
        tz_localize: bool, optional
            If True, localize all datetimes to respective time zone. Default is True.
        rename_columns: list, dict, optional
            If a list, provide new names for all entities.
            If a dict, specify original entity names as keys and new names as values.
            Default is None.
        data_type_in_columns: bool, optional
            If True, prepends the data type: "<data_type>.<column_name>".
            Default is False.
        unit_in_columns : bool, optional
            If True, includes units: "<column_name> (<unit>)". Default is False.

        Returns
        -------
        ``pandas.DataFrame``
            ``json`` response type processed into a ``pandas.DataFrame``
        """
        import pandas as pd

        columns = []
        if timestamp:
            columns += ["timestamp"]
        if last_updated:
            columns += ["lastUpdated"]
        if self.min_avg_max:
            columns += ["min", "avg", "max"]
        else:
            columns += ["value"]

        df_records = pd.DataFrame(self.response["data"])
        df_records = df_records.set_index("dateTime", drop=True)
        df_records.index = pd.to_datetime(df_records.index, format="%Y-%m-%dT%H:%M:%S")
        if tz_localize:
            df_records.index = df_records.index.tz_localize(
                self.time_zone, ambiguous="infer"
            )

        data_type_entity_groups = df_records.groupby("dataTypeEntity")
        outputs = {}
        for idx, (data_type_entity, values) in enumerate(
            self.response["metadata"]["dataTypes"].items()
        ):
            if data_type_entity not in data_type_entity_groups.groups:
                logger.debug(
                    f"The following data was empty and has not been added - "
                    f"data type: {values['dataType']}, entity: {values['header']}"
                )
                continue

            df_entity = data_type_entity_groups.get_group(data_type_entity)
            df_entity = df_entity[columns]

            # In JSON format, data type is prefixed by default
            data_type = self.response["metadata"]["dataTypes"][data_type_entity][
                "dataType"
            ]
            column_renamed = data_type_entity.split(data_type + ".")[-1]

            # In JSON format, data type is prefixed by default
            if not data_type_in_columns:
                data_type = self.response["metadata"]["dataTypes"][data_type_entity][
                    "dataType"
                ]
                column_renamed = data_type_entity.split(data_type + ".")[-1]
            if isinstance(rename_columns, list):
                column_renamed = rename_columns[idx]
            elif isinstance(rename_columns, dict):
                column_renamed = rename_columns[data_type_entity]
            if data_type_in_columns:
                column_renamed = f"{data_type}.{column_renamed}"
            if unit_in_columns:
                unit = self.response["metadata"]["dataTypes"][data_type_entity]["unit"]
                column_renamed = f"{column_renamed} ({unit})"
            if timestamp:
                df_entity["timestamp"] = dt_series_format(
                    df_entity["timestamp"],
                    "%Y-%m-%dT%H:%M:%S",
                    tz_localize,
                    "UTC",
                    convert_timestamps,
                    self.time_zone,
                )
            if last_updated:
                df_entity["lastUpdated"] = dt_series_format(
                    df_entity["lastUpdated"],
                    "%Y-%m-%dT%H:%M:%S",
                    tz_localize,
                    "UTC",
                    convert_timestamps,
                    self.time_zone,
                )

            df_entity = df_entity.rename(
                columns=lambda x: f"{column_renamed} ({x})"
                if x != "value"
                else f"{column_renamed}"
            )

            outputs[column_renamed] = df_entity

        df = pd.concat(outputs.values(), axis=1)

        return df


class JSONMapBase:
    def to_df(
        self,
        timestamp: bool = False,
        last_updated: bool = False,
        convert_timestamps: bool = False,
        tz_localize: bool = True,
        rename_columns: list | dict | None = None,
        data_type_in_columns: bool = False,
        unit_in_columns: bool = False,
    ) -> pd.DataFrame:
        """
        Process ``json_map`` response type into a ``pandas.DataFrame``.

        Parameters
        ----------
        timestamp: bool,  optional
            Indicates when the data was first entered into the database or
            created as a forecast, in UTC. Set to True to include this
            column in the DataFrame. Default is False.
        last_updated: bool, optional
            Represents the last time the data was updated in the database, in UTC.
            Set to True to include this column in the DataFrame. Default is False.
        convert_timestamps: bool
            If True, converts 'timestamp' and 'lastUpdated' columns from UTC to
            the time zone specified in the data request.
        tz_localize: bool, optional
            If True, localize all datetimes to respective time zone. Default is True.
        rename_columns: list, dict, optional
            If a list, provide new names for all entities.
            If a dict, specify original entity names as keys and new names as values.
            Default is None.
        data_type_in_columns: bool, optional
            If True, prepends the data type: "<data_type>.<column_name>".
            Default is False.
        unit_in_columns : bool, optional
            If True, includes units: "<column_name> (<unit>)". Default is False.

        Returns
        -------
        ``pandas.DataFrame``
            ``json_map`` response type processed into a ``pandas.DataFrame``
        """
        import pandas as pd

        df_map = pd.DataFrame.from_dict(self.response["data"], orient="index")
        df_map.index = pd.to_datetime(df_map.index, format="%Y-%m-%dT%H:%M:%S")
        if tz_localize:
            df_map.index = df_map.index.tz_localize(self.time_zone, ambiguous="infer")

        df = pd.DataFrame(index=df_map.index)
        df.index.name = "dateTime"
        # Expand each dictionary in the cells into separate columns
        for idx, data_type_entity in enumerate(df_map.columns):
            df_entity = df_map[data_type_entity].apply(pd.Series)

            column_renamed = data_type_entity
            # In JSON format, data type is prefixed by default
            data_type = self.response["metadata"]["dataTypes"][data_type_entity][
                "dataType"
            ]
            column_renamed = data_type_entity.split(data_type + ".")[-1]
            if isinstance(rename_columns, list):
                column_renamed = rename_columns[idx]
            elif isinstance(rename_columns, dict):
                column_renamed = rename_columns[data_type_entity]
            if data_type_in_columns:
                column_renamed = f"{data_type}.{column_renamed}"
            if unit_in_columns:
                unit = self.response["metadata"]["dataTypes"][data_type_entity]["unit"]
                column_renamed = f"{column_renamed} ({unit})"

            if not timestamp:
                df_entity = df_entity.drop("timestamp", axis=1)
            else:
                df_entity["timestamp"] = dt_series_format(
                    df_entity["timestamp"],
                    "%Y-%m-%dT%H:%M:%S",
                    tz_localize,
                    "UTC",
                    convert_timestamps,
                    self.time_zone,
                )
            if not last_updated:
                df_entity = df_entity.drop("lastUpdated", axis=1)
            else:
                df_entity["lastUpdated"] = dt_series_format(
                    df_entity["lastUpdated"],
                    "%Y-%m-%dT%H:%M:%S",
                    tz_localize,
                    "UTC",
                    convert_timestamps,
                    self.time_zone,
                )

            df_entity = df_entity.rename(
                columns=lambda x: f"{column_renamed} ({x})"
                if x != "value"
                else f"{column_renamed}"
            )

            df = df.join(df_entity)

        return df
