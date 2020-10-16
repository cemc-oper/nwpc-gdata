from .index_retrieval import IndexRetrieval

from ._config import load_config_file

def create_index_retrieval(config_file_path=None):
    config = load_config_file(config_file_path)
    client = IndexRetrieval(
        elastic_servers=config["indexdb"]["hosts"]
    )
    return client
