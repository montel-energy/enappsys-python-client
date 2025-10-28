from __future__ import annotations

import asyncio
import copy

from datetime import timedelta
from typing import Union

from ..enum import ResolutionEnum
from ..services.base import APIBase


class APIBaseAsync(APIBase):
    async def _get_in_chunks_async(
        self,
        url,
        params,
        start_dt: Union[str, "datetime"],
        end_dt: Union[str, "datetime"],
        resolution: ResolutionEnum,
        chunk_rows: int | None = None,
    ):
        """Fire fixed-size chunk requests concurrently; AsyncSession rate limiter spaces starts."""
        start_dt_obj = self._get_dt(start_dt, "start_dt")
        end_dt_obj = self._get_dt(end_dt, "end_dt")

        delta = ResolutionEnum._from_value(resolution).delta
        # TODO: now only chunking 'smarter' for 1 sec,
        # smarter chunking for all resolutions based on payload size
        sec = int(delta.total_seconds())
        rows_per_chunk = (
            chunk_rows
            if chunk_rows is not None
            else (30_000 if sec == 1 else min(self.API_MAX_ROWS, 100_000))
        )

        tasks = []
        cursor = start_dt_obj
        while cursor < end_dt_obj:
            chunk_end_dt = min(cursor + rows_per_chunk * delta, end_dt_obj)
            chunk_params = copy.deepcopy(params)
            self._add_dt(chunk_params, cursor, "start", "start_dt")
            self._add_dt(chunk_params, chunk_end_dt, "end", "end_dt")
            tasks.append(self._session.get(url, chunk_params))
            cursor = chunk_end_dt

        # Let AsyncSession’s limiter space attempt starts; gather overlaps in-flight I/O.
        return await asyncio.gather(*tasks)