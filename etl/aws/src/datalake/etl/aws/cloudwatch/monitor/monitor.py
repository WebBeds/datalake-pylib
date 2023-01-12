from .watcher import Watcher

import configparser
import argparse
import logging
import ast
import sys
import os

DEFAULT_CONFIG = "monitor.conf"
DEFAULT_VALUE = 7 * 24 * 60 * 60

def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(description='SQLMonitor, monitor your SQL query, send messages to CloudWatch metrics or any other metrics system')

    parser.add_argument(
        "-c", "--config",
        help="config file",
        default=DEFAULT_CONFIG,
    )

    parser.add_argument(
        '-m', '--monitors',
        help='monitors to run',
        default=None,
        type=str
    )

    parser.add_argument('--dry', help='dry run', action='store_true')

    return parser.parse_args()

def parse_config(config_file: str) -> configparser.ConfigParser:
    
    config = None
    if config_file and os.path.exists(os.path.abspath(config_file)):
        config = configparser.ConfigParser()
        config.read(os.path.abspath(config_file))
    
    class ConfigArgument:
        def __init__(self, key, default = None) -> None:
            self.key=key
            self.default=default
        def parse(self, key, config):
            if not config or (config and key not in config):
                return self.default
            return config[key]
    
    keys = {
        'monitor': [
            ConfigArgument('namespace', None),
        ],
    }

    return {
        key: {
            arg.key: arg.parse(arg.key, config[key] if config else None) for arg in keys[key]
        } for key in keys
    }

def parse_monitor(monitor_file: str, conf: argparse.Namespace) -> list:

    if not os.access(monitor_file, os.R_OK):
        raise Exception("Monitor file \"{0}\" does not have read permissions".format(monitor_file))
    if not os.path.isfile(monitor_file):
        raise Exception("Monitor file \"{0}\" is not a file".format(monitor_file))

    spec = None
    with open(monitor_file, 'r') as f:
        spec = ast.literal_eval(f.read())
    if not isinstance(spec, dict) and not isinstance(spec, list):
        raise Exception("Monitor file \"{0}\" is not a valid list or dict data structure".format(monitor_file))

    if isinstance(spec, dict):
        if conf['monitor']['namespace']:
            spec['namespace'] = conf['monitor']['namespace']
        return [Watcher.parse(spec)]
    
    for idx in range(len(spec)):
        if 'namespace' in spec[idx]:
            continue
        spec[idx]['namespace'] = conf['monitor']['namespace']

    return [Watcher.parse(m) for m in spec]

def parse_monitors(monitors_path: str, conf: argparse.Namespace) -> list:

    monitors = []

    if not os.path.exists(os.path.abspath(monitors_path)):
        raise Exception("Monitors file \"{0}\" does not exist".format(monitors_path))

    if not os.path.isdir(os.path.abspath(monitors_path)):
        return parse_monitor(os.path.abspath(monitors_path), conf)

    for f in os.listdir(os.path.abspath(monitors_path)):
        monitors.extend(parse_monitor(os.path.abspath(os.path.join(monitors_path, f)), conf))

    return monitors

def main() -> None:

    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(message)s',
        level=logging.INFO
    )

    args = parse_args()
    conf = parse_config(args.config)

    logging.info("ARGV: {0}".format(sys.argv))
    logging.info("ARGS: {0}".format(args))
    logging.info("CONF: {0}".format(conf))

    if not args.monitors:
        logging.warning("No monitors specified, exiting...")
        return

    mnts = parse_monitors(args.monitors, conf)

    failed = None
    for _, monitor in enumerate(mnts):
        monitor: Watcher
        logging.info("Running monitor {0}".format(monitor.name))
        try:
            monitor.run(args.dry)
        except Exception as e:
            if str(type(e)) == "<class 'botocore.errorfactory.InvalidRequestException'>":
                logging.warning("Skipping monitor {0} due to invalid query".format(monitor.name))
                continue
            logging.error("Error running monitor {0}: {1}".format(monitor.name, e))
            failed = e
        logging.info("Completed monitor {0}".format(monitor.name))

    if failed:
        raise failed