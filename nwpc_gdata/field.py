import datetime
import typing

import pandas as pd

from .index_retrieval import IndexRetrieval
from .grib import load_bytes_from_index


def load_field_bytes(
        system: str,
        stream: str,
        data_type: str,
        data_name: str,
        start_time: typing.Union[datetime.datetime, pd.Timestamp],
        forecast_time: pd.Timedelta,
        parameter: str,
        level_type: str,
        level: int,
        index_retrieval: IndexRetrieval,
        data_class: str = "od",
) -> typing.Optional[bytes]:
    grib_index = index_retrieval.query(
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
    )
    if grib_index is None:
        return None
    else:
        raw_bytes = load_bytes_from_index(grib_index)
        return raw_bytes
