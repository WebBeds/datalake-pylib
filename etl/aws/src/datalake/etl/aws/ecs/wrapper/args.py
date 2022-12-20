from .types import Argument
from .schema import (
    ParsedTimeSeconds,
    ParsedTimeMilliseconds,
    ParsedExecutionIdArn,
    ParsedCLIArgument,
)

import configparser
import argparse
import json
import os

DEFAULT_CONFIG = "wrapper.conf"

DEFAULT_TIMEOUT = 3600
DEFAULT_AWS_REGION = "eu-west-1"
DEFAULT_RATE = 60

def parse_argv() -> argparse.Namespace:
    
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config',
        help="Config file",
        default=DEFAULT_CONFIG,
        type=str
    )
    parser.add_argument(
        '--cli-json',
        help='JSON file containing the CLI arguments',
        type=json.loads,
        required=True,
    )
    parser.add_argument("--dry", help="Dry mode", action="store_true")
    parser.add_argument("--verbose", help="Verbose mode", action="store_true")
    parser.add_argument("--debug", help="Debug mode", action="store_true")

    return parser.parse_args()

def parse_os_args() -> dict:

    args = [
        Argument('AWS_EXECUTION_ARN'),
        Argument('AWS_EXECUTION_START'),
        Argument('AWS_STATE_MACHINE_ID'),
        Argument('AWS_STATE_ENTERED'),
        Argument('CreationTimestampSeconds',
            search_key='AWS_EXECUTION_START',
            schema=ParsedTimeSeconds(),
        ),
        Argument('CreationTimestampMilliseconds',
            search_key='AWS_EXECUTION_START',
            schema=ParsedTimeMilliseconds(),
        ),
        Argument('ExecutionId',
            search_key='AWS_EXECUTION_ARN',
            schema=ParsedExecutionIdArn(),
        )
    ]

    return {
        arg.key: arg.parse(os.environ) for arg in args
    }

def parse_config(
    config_path: str
) -> configparser.ConfigParser:
    
    config = None
    if config_path and os.path.exists(os.path.abspath(config_path)):
        config = configparser.ConfigParser()
        config.read(os.path.abspath(config_path))

    keys = {
        'wrapper': [
            Argument('entrypoint'),
            Argument('timeout', default=DEFAULT_TIMEOUT),
        ],
        'metrics': [
            Argument('namespace'),
            Argument('team'),
            Argument('group'),
            Argument('region', default=DEFAULT_AWS_REGION),
            Argument('rate', default=DEFAULT_RATE),
        ]
    }

    return {
        key: {
            arg.key: arg.parse(config[key] if config else None) for arg in keys[key]
        } for key in keys
    }

def parse_cli(
    cli_json: dict,
    config: configparser.ConfigParser
) -> dict:
    
    default_entrypoint = None
    if "wrapper" in config and "entrypoint" in config["wrapper"]:
        default_entrypoint = config["wrapper"]["entrypoint"]
    default_group = None
    if "metrics" in config and "group" in config["metrics"]:
        default_group = config["metrics"]["group"]

    args = [
        Argument('entrypoint',
            schema=ParsedCLIArgument(arg_type=list),
            default=default_entrypoint,
        ),
        Argument('command',
            schema=ParsedCLIArgument(arg_type=list),
            default=[],
        ),
        Argument('job',
            schema=ParsedCLIArgument(arg_type=str),
            default=None,
        ),
        Argument('group',
            schema=ParsedCLIArgument(arg_type=str),
            default=default_group,
        ),
        Argument('actions',
            schema=ParsedCLIArgument(arg_type=list),
            default=[],
        )
    ]

    return {
        arg.key: arg.parse(cli_json) for arg in args
    }