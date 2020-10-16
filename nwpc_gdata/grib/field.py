import datetime
import typing

import pandas as pd
import xarray as xr

from .message import load_message
from nwpc_gdata.index import IndexRetrieval

from nwpc_data.grib.eccodes._xarray import create_data_array_from_message


def load_field(
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
) -> typing.Optional[xr.DataArray]:
    message = load_message(
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

    field = create_data_array_from_message(message)
    return field
