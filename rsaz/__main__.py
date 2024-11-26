import logging
import os

import toml
from cerberus import Validator

from rsaz import launch

CONF_PATH = "rstracer-azure.toml"

CONF_SCHEMA = {
    "frequency": {
        "type": "integer",
        "required": True,
        "min": 1,
    },
    "source": {
        "type": "dict",
        "required": True,
        "schema": {
            "directory": {"type": "string", "required": True},
            "format": {"type": "string", "required": True, "allowed": ["parquet", "csv"]},
        },
    },
    "credentials": {
        "type": "dict",
        "required": True,
        "schema": {
            "account": {"type": "string", "required": True, "empty": False},
            "access_key": {"type": "string", "required": True, "empty": False},
        },
    },
    "storage": {
        "type": "dict",
        "required": True,
        "schema": {
            "container": {"type": "string", "required": True, "empty": False},
            "directory": {"type": "string", "required": True},
        },
    },
}


def read_config():
    validator = Validator(CONF_SCHEMA)
    if not os.path.exists(CONF_PATH):
        raise ValueError(f"Config file {CONF_PATH} does not exist")
    else:
        with open(CONF_PATH, "r") as f:
            config = toml.load(f)
            if validator.validate(config):
                return config
            else:
                raise ValueError(validator.errors)


if __name__ == "__main__":
    logging.basicConfig(filename="rsaz.log", encoding="utf-8", level=logging.INFO)
    config = read_config()
    launch(config)
