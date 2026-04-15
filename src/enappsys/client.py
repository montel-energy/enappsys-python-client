from enappsys.services.bulk import BulkAPI
from enappsys.services.chart import ChartAPI
from enappsys.services.epex import EpexAPI
from enappsys.services.price_volume_curve import PriceVolumeCurveAPI
from enappsys.session import Session


class EnAppSys:
    def __init__(
        self,
        user: str | None = None,
        secret: str | None = None,
        credentials_file: str | None = None,
        max_retries: int = 3,
        agent_id: str | None = None,
    ):
        """
        Client interface for the EnAppSys API.

        Provides convenient access to the various service endpoints through dedicated 
        interfaces for bulk data retrieval, chart data, and price-volume curves.

        This client handles authentication, request validation, chunked retrieval
        for large datasets, automatic retries, and rate limiting.

        Parameters
        ----------
        user : str | None, optional
            EnAppSys username for authentication.
        secret : str | None, optional
            EnAppSys secret associated with the username.
        credentials_file: str | None, optional
            Path to a credentials file containing the user and secret.
            Defaults to ``~/.credentials/enappsys.json``.
        max_retries : int, default=3
            Maximum number of retry attempts for failed HTTP requests.
        agent_id : str | None, optional
            Optional agent identifier to include in the User-Agent header.
        """
        self._session = Session(user, secret, credentials_file, max_retries, agent_id)

        # --- Public members ---
        self.bulk = BulkAPI(self)
        """An instance of :class:`BulkAPI`.
        
        Provides access to the Bulk API for retrieving time series data,
        such as generation, demand, and price data.
        """

        self.chart = ChartAPI(self)
        """
        An instance of :class:`ChartAPI`.
        
        Provides access to the Chart API for retrieving time series data 
        from charts.
        """

        self.price_volume_curve = PriceVolumeCurveAPI(self)
        """
        An instance of :class:`PriceVolumeCurveAPI`.
        
        Provides access to the Price Volume Curve API for retrieving 
        price volume curves from charts.
        """

        self.epex = EpexAPI(self)
        """
        An instance of :class:`EpexAPI`.

        Provides access to the APX download endpoint for EPEX contract
        and settlement data.
        """

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self._session.session.close()

    def __enter__(self) -> "EnAppSys":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
