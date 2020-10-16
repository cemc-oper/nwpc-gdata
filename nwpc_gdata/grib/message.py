import datetime
import typing

import pandas as pd

from .bytes import load_bytes
from nwpc_gdata.index import IndexRetrieval

from nwpc_data.grib.eccodes._bytes import create_message_from_bytes


def load_message(
        system: str,
        stream: str,
        data_type: str,
        data_name: str,
        start_time: typing.Union[datetime.datetime, pd.Timestamp, str],
        forecast_time: typing.Union[pd.Timedelta, str],
        parameter: str,
        level_type: str = None,
        level: int = None,
        data_class: str = "od",
        index_retrieval: IndexRetrieval = None,
) -> typing.Optional[int]:
    raw_bytes = load_bytes(
        system=system,
        stream=stream,
        data_type=data_type,
        data_name=data_name,
        start_time=start_time,
        forecast_time=forecast_time,
        parameter=parameter,
        level_type=level_type,
        level=level,
        data_class=data_class,
        index_retrieval = index_retrieval,
    )

    grib_message = create_message_from_bytes(raw_bytes)
    return grib_message