import datetime
import typing

import pandas as pd

from nwpc_gdata.index import IndexRetrieval, create_index_retrieval
from nwpc_gdata.core import load_bytes_from_index


def load_bytes(
        system: str,
        stream: str,
        data_type: str,
        data_name: str,
        start_time: typing.Union[datetime.datetime, pd.Timestamp],
        forecast_time: pd.Timedelta,
        parameter: str,
        level_type: str,
        level: int,
        data_class: str = "od",
        index_retrieval: IndexRetrieval = None,
) -> typing.Optional[bytes]:
    if index_retrieval is None:
        index_retrieval = create_index_retrieval()

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
