from typing import Literal, Union

from .enum import AppEnvEnum
from .services_async.bulk import AsyncBulkAPI
from .services_async.chart import AsyncChartAPI
from .services_async.price_volume_curve import AsyncPriceVolumeCurveAPI
from .session_async import AsyncSession


class EnAppSysAsync:
    def __init__(
        self,
        user: str | None = None,
        secret: str | None = None,
        credentials_file: str | None = None,
        max_retries: int = 3
    ):
        """Client for the EnAppSys API.

        Parameters
        ----------
        user : str | None, optional
            EnAppSys username.
        secret : str | None, optional
            EnAppSys secret.
        credentials_file: str | None, optional
            Specify path to the credentials file to have it at another place
            than ~/.enappsys/api_credentials.json
        profile_name : str | None, optional
            Specify the profile name to load credentials from. If it is not found,
            the ``default`` profile name will be used.
        """
        self._session = AsyncSession(
            user, secret, credentials_file, max_retries
        )

        # --- Public members ---
        self.bulk = AsyncBulkAPI(self)
        """An instance of :class:`BulkAPI`.
        
        Provides the interface for retrieving time series data.
        """

        self.chart = AsyncChartAPI(self)
        """
        An instance of :class:`ChartAPI`.
        
        Provides the interface for retrieving data from a chart.
        """

        self.price_volume_curve = AsyncPriceVolumeCurveAPI(self)
        """
        An instance of :class:`PriceVolumeCurveAPI`.
        
        Provides the interface for retrieving price volume curve data.
        """

    async def aclose(self) -> None:
        await self._session.close()

    async def __aenter__(self) -> "EnAppSysAsync":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()
