import typing

import pandas as pd


def parse_query(
        query: typing.Dict
) -> typing.Dict:
    fixed_query = query.copy()

    if "start_time" in fixed_query:
        fixed_query["start_time"] = pd.to_datetime(query["start_time"])

    if "forecast_time" in fixed_query:
        fixed_query["forecast_time"] = pd.to_timedelta(query["forecast_time"])

    if "class" in fixed_query:
        fixed_query["data_class"] = fixed_query.pop("class")

    if "type" in fixed_query:
        fixed_query["data_type"] = fixed_query.pop("type")

    if "name" in fixed_query:
        fixed_query["data_name"] = fixed_query.pop("name")

    return fixed_query
