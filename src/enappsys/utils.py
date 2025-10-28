def dt_series_format(
    dt_series,
    dt_format: str,
    localize: bool = False,
    tz_localize: str | None = None,
    convert: bool = False,
    tz_convert: str | None = None,
):
    import pandas as pd

    dt_series = pd.to_datetime(dt_series, format=dt_format)
    if localize:
        dt_series = dt_series.dt.tz_localize(tz_localize)
    if convert:
        dt_series = dt_series.dt.tz_convert(tz_convert)
    return dt_series


def validate_rename_columns_length(rename_columns: list, columns, step_size):
    """
    Validates whether the length of 'rename_columns' matches the expected length
    based on the 'columns' and 'step_size'.

    Parameters
    ----------
    rename_columns : list
        The list of column names to be validated.
    columns : list
        The original list of columns from the data.
    step_size : int
        The step size used for calculating the expected length of 'rename_columns'.

    Raises
    ------
    ValueError
        If the length of 'rename_columns' does not match the expected length.
    """
    # # Cannot compare with self.entities itself because they can be "ALL"
    expected_length = len(columns) // step_size

    if len(rename_columns) != expected_length:
        raise ValueError(
            f"Length of 'rename_columns' must match the amount of entities "
            f"fetched.\n"
            f"rename_columns: {rename_columns}.\n"
            f"original columns: {columns[1:]}.\n"
            f"If 'min_avg_max' is True count every three as one."
        )
