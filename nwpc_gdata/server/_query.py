import pandas as pd


def parse_query(query: dict):
    if "start_time" in query:
        query["start_time"] = pd.to_datetime(query["start_time"])

    if "forecast_time" in query:
        query["forecast_time"] = pd.to_timedelta(query["forecast_time"])

    return query
