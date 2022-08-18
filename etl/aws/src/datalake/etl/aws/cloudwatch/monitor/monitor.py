from ...ecs.wrapper.metrics import SingleMetric
from .metrics import MonitorMetrics
from dataclasses import dataclass
import awswrangler as wr
import configparser
import argparse
import logging
import ast
import sys
import os

DEFAULT_CONFIG = "monitor.conf"
DEFAULT_VALUE = 7 * 24 * 60 * 60
DEFAULT_ATHENA_KWARGS = {
    "database": "default",
    "ctas_approach": False
}

@dataclass
class Monitor:
    name: str
    dimensions: dict
    unit: str
    sql: str
    athena_kwargs: dict = None
    default_value: float = DEFAULT_VALUE

    def _process_dimensions(
        self,
        dimension,
        default,
        input
    ):
        if str(dimension).lower() in input:
            return str(input[str(dimension).lower()])
        else:
            return default

    def run(self, metrics: MonitorMetrics, dry: bool = False) -> None:

        if not self.athena_kwargs:
            self.athena_kwargs = DEFAULT_ATHENA_KWARGS
        else:
            self.athena_kwargs.update(DEFAULT_ATHENA_KWARGS)

        df = wr.athena.read_sql_query(
            sql=self.sql,
            **self.athena_kwargs
        )
        df.columns = df.columns.str.lower()

        if df.empty:
            logging.warning("No data returned from query")
            return

        df[self.name.lower()] = df[self.name.lower()].fillna(self.default_value)

        for _, row in df.iterrows():
            m = SingleMetric(
                metric_name=self.name,
                dimensions={
                    k: self._process_dimensions(
                        k,
                        v,
                        row
                    ) for k, v in self.dimensions.items()
                },
                value=row[self.name.lower()],
                unit=self.unit
            )
            logging.info(m.to_cloudwatch_metric())
            metrics.add(m)
 
        if not dry:
            metrics.send()

        return

def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(description='CloudWatch Metrics Monitor')

    parser.add_argument(
        '-c', '--config',
        help='config file',
        default=DEFAULT_CONFIG,
        type=str
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

def parse_monitor(monitor_file: str) -> list:

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
        return [Monitor(**spec)]
    return [Monitor(**m) for m in spec]

def parse_monitors(monitors_path: str) -> list:

    monitors = []

    if not os.path.exists(os.path.abspath(monitors_path)):
        raise Exception("Monitors file \"{0}\" does not exist".format(monitors_path))

    if not os.path.isdir(os.path.abspath(monitors_path)):
        return parse_monitor(os.path.abspath(monitors_path))

    for f in os.listdir(os.path.abspath(monitors_path)):
        monitors.extend(parse_monitor(os.path.abspath(os.path.join(monitors_path, f))))

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

    mnts = parse_monitors(args.monitors)
    metrics = MonitorMetrics(
        namespace=conf['monitor']['namespace'],
    )

    for _, monitor in enumerate(mnts):
        monitor: Monitor
        logging.info("Running monitor {0}".format(monitor.name))
        try:
            monitor.run(metrics,args.dry)
        except Exception as e:
            if str(type(e)) == "<class 'botocore.errorfactory.InvalidRequestException'>":
                logging.warning("Skipping monitor {0} due to invalid query".format(monitor.name))
                continue
            logging.error("Error running monitor {0}: {1}".format(monitor.name, e))
            raise e
        logging.info("Completed monitor {0}".format(monitor.name))