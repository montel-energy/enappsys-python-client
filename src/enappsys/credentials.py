import json
import logging
import os

from pathlib import Path

from .exceptions import (
    NoCredentialsError,
    PartialCredentialsError,
    InvalidCredentialsFile,
)

logger = logging.getLogger(__name__)


class Credentials:
    """Holds the credentials needed to authenticate requests."""

    def __init__(
        self,
        user: str | None = None,
        secret: str | None = None,
        credentials_file: str | None = None
    ) -> None:
        """Initialize a :class:`Credentials` instance.

        The EnAppSys client will look for credentials in the following order:
        explicitly passed arguments, environment variables or a credentials file.

        Parameters
        ----------
        user : str | None, optional
            EnAppSys username.
        secret : str | None, optional
            EnAppSys secret.
        credentials_file: str | None, optional
            Specify path to the credentials file to have it at another place
            than ~/.enappsys/api_credentials.json
        """
        self.user = user
        self.secret = secret
        self.credentials_file = credentials_file
        self.method = None

        if (
            not self._set_with_explicit_args()
            and not self._set_with_environment_vars()
            and not self._set_with_config_file()
        ):
            raise NoCredentialsError()

        self.api_format = {"user": self.user, "pass": self.secret}

    def _set_with_explicit_args(self) -> bool:
        self.method = "explicit arguments"

        return self._validate_credentials()

    def _set_with_environment_vars(self) -> bool:
        self.method = "environment variables"

        self.user = self.user or os.getenv("ENAPPSYS_USER")
        self.secret = self.secret or os.getenv("ENAPPSYS_SECRET")

        return self._validate_credentials()

    def _set_with_config_file(self) -> bool:
        self.method = "credentials file"

        self.credentials_file = (
            self.credentials_file or Path.home() / ".credentials" / "enappsys.json"
        )

        try:
            with open(self.credentials_file, mode="r", encoding="utf-8") as f:
                credentials = json.load(f)
        except IOError as e:
            raise InvalidCredentialsFile(
                f"Could not retrieve credentials from '{self.credentials_file}'. "
                f"Reason: {e}"
            ) from e

        self.user = credentials.get("user")
        self.secret = credentials.get("secret")

        if not self.user or not self.secret:
            raise InvalidCredentialsFile(
                f"Invalid format in credentials file '{self.credentials_file}'. "
                f"Expected keys: 'user' and 'secret'. Got: {list(credentials.keys())}"
            )
        
        return self._validate_credentials()

    def _validate_credentials(self) -> str | None:
        if not self.user and not self.secret:
            return False
        elif self.user and not self.secret:
            raise PartialCredentialsError(provider=self.method, cred_var="secret")
        elif self.secret and not self.user:
            raise PartialCredentialsError(provider=self.method, cred_var="user")
        return True
