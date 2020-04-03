import datetime
import typing

import pandas as pd


def load_field(
        system: str,
        stream: str,
        data_type: str,
        data_name: str,
        start_time: datetime.datetime or pd.Timestamp,
        forecast_time: pd.Timedelta,
        data_class: str = "od",
):

    return
