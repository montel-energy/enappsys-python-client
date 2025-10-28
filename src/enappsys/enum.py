from datetime import timedelta
from enum import Enum
from typing import Union

from .exceptions import ValidationError


class BaseEnum(Enum):
    """Base Enum that resolves a member by matching any of its public attributes."""

    @classmethod
    def _from_value(cls, value: Union[str, int, float, bool, Enum, object]):
        """Return the Enum member matching any of its public attribute values.

        Accepts either:
        - the Enum member itself
        - any attribute value belonging to one of the Enum members
        """
        if isinstance(value, cls):
            return value

        # Iterate through all members and their public attributes
        for member in cls:
            for attr, attr_value in member.__dict__.items():
                if attr.startswith("_"):
                    continue
                if attr_value == value:
                    return member

        # no match found → raise informative error
        value_type = type(value).__name__
        public_attrs = {
            m.name: {k: v for k, v in m.__dict__.items() if not k.startswith("_")}
            for m in cls
        }
        raise ValidationError(
            reason=(
                f"No match for {value!r} ({value_type}) in {cls.__name__}. "
                f"Checked attributes:\n{public_attrs}"
            )
        )


class AppEnvEnum(BaseEnum):
    APP = ("app", "https://app.enappsys.com")
    APPQA = ("appqa", "https://appqa.enappsys.com")
    APPDEV = ("appdev", "https://appdev.enappsys.com")

    def __init__(self, platform, app_env_url):
        self.platform = platform
        self.app_env_url = app_env_url


class ResponseFormatEnum(BaseEnum):
    JSON = ("json", "jsonapi", "json", "JSON")
    JSON_MAP = ("json_map", "jsonmapapi", "json_map", None)
    CSV = ("csv", "csvapi", "csv", "CSV")
    XML = ("xml", "xmlapi", "xml", "XML")

    def __init__(self, platform, bulk_url, chart_tag, epex_tag):
        self.platform = platform
        self.bulk_url = bulk_url
        self.chart_tag = chart_tag
        self.epex_tag = epex_tag


class ResolutionEnum(BaseEnum):
    """Supported temporal resolutions with platform, ISO 8601, pandas and timedelta."""

    PT1S = ("1s", "PT1S", "s", timedelta(seconds=1))
    PT15S = ("15s", "PT15S", "15s", timedelta(seconds=15))
    PT1M = ("min", "PT1M", "min", timedelta(minutes=1))
    PT5M = ("5min", "PT5M", "5min", timedelta(minutes=5))
    PT15M = ("qh", "PT15M", "15min", timedelta(minutes=15))
    PT30M = ("hh", "PT30M", "30min", timedelta(minutes=30))
    PT1H = ("hourly", "PT1H", "h", timedelta(hours=1))
    PT3H = ("3hourly", "PT3H", "3h", timedelta(hours=3))
    PT6H = ("6hourly", "PT6H", "6h", timedelta(hours=6))
    PT12H = ("12hourly", "PT12H", "12h", timedelta(hours=12))
    P1D = ("daily", "P1D", "D", timedelta(days=1))
    P1W = ("weekly", "P1W", "W", timedelta(weeks=1))
    P1M = ("monthly", "P1M", "M", timedelta(days=30))
    P1Y = ("yearly", "P1Y", "Y", timedelta(days=365))

    def __init__(self, platform: str, iso: str, pandas: str, delta: timedelta):
        self.platform = platform
        self.iso = iso
        self.pandas = pandas
        self.delta = delta


class TimeZoneEnum(BaseEnum):
    UTC = "UTC"
    WET = "WET"
    CET = "CET"
    EET = "EET"

    def __init__(self, platform):
        self.platform = platform


class CurrencyEnum(BaseEnum):
    BAM = "BAM"
    BGN = "BGN"
    CAD = "CAD"
    CHF = "CHF"
    CNY = "CNY"
    CZK = "CZK"
    DKK = "DKK"
    EUR = "EUR"
    EURCENTS = "EURcents"
    GBP = "GBP"
    GBPPENCE = "GBPpence"
    HKD = "HKD"
    HRK = "HRK"
    HUF = "HUF"
    JPY = "JPY"
    NOK = "NOK"
    PLN = "PLN"
    RON = "RON"
    RUB = "RUB"
    SEK = "SEK"
    SGD = "SGD"
    TRY = "TRY"
    USD = "USD"

    def __init__(self, platform):
        self.platform = platform


class DelimiterEnum(BaseEnum):
    COMMA = ("comma", ",")
    TAB = ("tab", "")
    SEMICOLON = ("semicolon", ";")
    PIPE = ("pipe", "|")

    def __init__(self, platform, character):
        self.platform = platform
        self.character = character
