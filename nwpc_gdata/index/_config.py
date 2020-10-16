import pathlib
import yaml


def get_user_config_file_path():
    config_file_path = pathlib.Path.joinpath(
        pathlib.Path.home(),
        ".config/nwpc-tool/gdata/config.yaml"
    )
    return config_file_path


def load_config_file(file_path = None):
    if file_path is None:
        file_path = get_user_config_file_path()

    with open(file_path, "r") as f:
        config = yaml.safe_load(f)
        return config
