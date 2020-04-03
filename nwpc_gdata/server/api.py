from flask import Blueprint, request, current_app, jsonify

from nwpc_gdata.field import load_field_bytes
from nwpc_gdata.index_retrieval import IndexRetrieval
from nwpc_gdata.transport import RawField, convert_to_json

from ._query import parse_query

api_app = Blueprint('api_app', __name__, template_folder='template')


@api_app.route('/gdata/fetch/field', methods=['POST'])
def fetch_field():
    """

    POST data
    {
        "query": {
            "system": "grapes_gfs_gmf",
            .....
            "start_time": "",
        }
    }

    """
    request_body = request.json
    query = request_body["query"]
    query = parse_query(query)

    servers = current_app.config["SERVER_CONFIG"]["elasticsearch"]["hosts"]
    servers = servers.split(",")
    retrieval = IndexRetrieval(servers)

    raw_bytes = load_field_bytes(
        index_retrieval=retrieval,
        **query,
    )

    if raw_bytes is None:
        return jsonify({
            "status": "failed",
        })

    raw_field = RawField()
    raw_field.raw_bytes = raw_bytes

    return jsonify({
        "status": "complete",
        "raw_field": convert_to_json(raw_field),
    })
