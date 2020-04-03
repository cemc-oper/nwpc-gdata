import typing
import datetime

import pandas as pd
from elasticsearch import Elasticsearch

from .grib import GribMessageIndex


def get_hour(s: pd.Timedelta) -> int:
    return int(s.seconds/3600) + s.days * 24


class IndexRetrieval(object):
    def __init__(self, elastic_servers: typing.List):
        self.elastic_servers = elastic_servers
        self.client = Elasticsearch(hosts=self.elastic_servers)

    def query(
            self,
            system: str,
            stream: str,
            data_type: str,
            data_name: str,
            start_time: datetime.datetime or pd.Timestamp,
            forecast_time: pd.Timedelta,
            parameter: str,
            level_type: str = None,
            level: int = None,
            data_class: str = "od",
    ):
        query_body = self._get_query_body(
            system,
            stream,
            data_type,
            data_name,
            start_time,
            forecast_time,
            parameter,
            level_type,
            level,
            data_class,
        )
        print(query_body)

        result = self._get_result(
            index="grb_index",
            query_body=query_body,
            search_from=0,
            search_size=1,
        )
        print(result)
        hints = result["hits"]
        if hints["total"] == 0:
            return None

        record = hints["hits"][0]

        grib_index = self._get_grib_index(record)
        return grib_index

    def _get_result(
            self,
            index: str,
            query_body: dict,
            search_from: int = 0,
            search_size: int = 1
    ):
        search_body = {
            "size": search_size,
            "from": search_from,
        }
        search_body.update(**query_body)
        res = self.client.search(index=index, body=search_body)
        return res

    def _get_query_body(
            self,
            system: str,
            stream: str,
            data_type: str,
            data_name: str,
            start_time: datetime.datetime or pd.Timestamp,
            forecast_time: pd.Timedelta,
            parameter: str,
            level_type: str = None,
            level: int = None,
            data_class: str = "od",):

        if not (system == "grapes_gfs_gmf" and stream == "oper" and data_type == "grib2" and data_name == "orig"):
            raise ValueError("query params is not supported")

        must_conditions = [{
            "match": {"DataName": "GFSGMFGRIB2"}
        }]

        filter_conditions=[{
            "term": {"date": int(start_time.strftime("%Y%m%d"))}
        }]

        filter_conditions.append({
            "term": {"time": int(start_time.strftime("%H%M"))}
        })

        filter_conditions.append({
            "term": {"stepRange": get_hour(forecast_time)}
        })

        filter_conditions.append({
            "term": {"shortName": parameter}
        })

        if level_type is not None:
            must_conditions.append({
                "match": {"typeOfLevel": level_type}
            })

        if level is not None:
            filter_conditions.append({
                "term": {"level": level}
            })

        query_body = {
            "query": {
                "bool": {
                    "must": must_conditions,
                    "filter": filter_conditions
                },
            },
        }
        return query_body

    def _get_grib_index(self, record) -> GribMessageIndex or None:
        """

        {
            "_index": "grb_index",
            "_type": "grb_type",
            "_id": "EXP1GRIB#/sstorage1/COMMONDATA/OPER/NWPC/GRAPES_GFS_GMF/Prod-grib/2019093021/ORIG/gmf.gra.2019100100003.grb2#count091#1",
            "_score": 0.110632196,
            "_source": {
                "gridType": "regular_ll",
                "typeOfLevel": "isobaricInhPa",
                "time": "0",
                "packingType": "grid_jpeg",
                "centre": "babj",
                "dataType": "fc",
                "level": "850",
                "component": "weather",
                "count": "91",
                "Depiction": "中文叙述",
                "DataName": "GFSGMFGRIB2",
                "length": "614385",
                "FileSize": "TB",
                "offset": "50345706",
                "date": "20191001",
                "edition": "2",
                "stepRange": "3",
                "shortName": "t"
            }
        }
        """
        record_id = record["_id"]
        tokens = record_id.split("#")
        if len(tokens) < 2:
            return None
        file_path = tokens[1]

        record_source = record["_source"]
        offset = int(record_source["offset"])
        length = int(record_source["length"])

        grib_index = GribMessageIndex(
            file_path=file_path,
            offset=offset,
            length=length,
        )

        return grib_index
