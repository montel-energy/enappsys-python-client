"""EnAppSys exception classes.

Includes two main exceptions: :class:`.APIError` for when something goes
wrong on the server side, and :class:`.ClientError` for when something goes wrong
on the client side. Both of these classes extend :class:`.EnAppSysException`.
"""


class EnAppSysException(Exception):
    """The base Exception that all other exception classes extend."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class ClientError(EnAppSysException):
    """
    Base exception for all errors that may occur in this client.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class APIError(EnAppSysException):
    """
    Base exception for all errors that may occur during an API call.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class ValidationError(APIError):
    """
    Validation error of some kind (invalid parameter or combination
    of parameters).
    """

    def __init__(self, reason=None, parameter=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.reason = reason
        self.parameter = parameter

    def __str__(self):
        if self.parameter:
            return f'Field="{self.parameter}", message: {self.reason}'
        else:
            return self.reason


class HTTPError(APIError):
    """
    Base class for all HTTP errors. Mostly used to wrap request's HTTP errors.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class BadRequest(APIError):
    """Malformed request parameters."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class ContentTooLarge(APIError):
    """Request exceeds defined limits."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class InternalServerError(APIError):
    """Other type of errors."""

    def __init__(self, msg):
        super().__init__(f"Data type might be incorrect, original message: \n\n{msg}.")


class NoCredentialsError(ClientError):
    """No credentials were found."""

    def __init__(self):
        super().__init__(
            "No credentials were found, either pass them to the client"
            " explicitly, through environment variables or by specifying"
            " them in a credentials JSON file."
        )


class PartialCredentialsError(ClientError):
    """Only partial credentials were found."""

    def __init__(self, provider, cred_var):
        super().__init__(
            f"Partial credentials found in {provider}, missing: {cred_var}"
        )


class InvalidCredentials(ClientError):
    """Username and/or password invalid."""

    def __init__(self):
        super().__init__("Username and/or password are invalid.")


class InvalidCredentialsFile(ClientError):
    """Malformed credentials file"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
