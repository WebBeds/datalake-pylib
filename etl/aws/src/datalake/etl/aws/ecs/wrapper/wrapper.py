from .metrics import WrapperMetrics, SingleMetric
from datetime import datetime
import configparser
import subprocess
import threading
import argparse
import logging
import base64
import socket
import json
import time
import uuid
import sys
import os
import re

DEFAULT_CONFIG = "wrapper.conf"

def parse_arguments() -> argparse.Namespace:

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

    return parser.parse_args()

def parse_os_arguments() -> dict:
    
    class OSArgument:
        def __init__(self, key, default = None) -> None:
            self.key=key
            self.default=default
        def parse(self, key, input):
            if not input or (input and key not in input):
                return self.default
            return input[key]

    args = [
        OSArgument('AWS_EXECUTION_ARN', None),
        OSArgument('AWS_EXECUTION_START', None),
        OSArgument('AWS_STATE_MACHINE_ID', None),
        OSArgument('AWS_STATE_ENTERED', None),
    ]

    return {
        arg.key: arg.parse(arg.key, os.environ) for arg in args
    }

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
        'wrapper': [
            ConfigArgument('entrypoint'),
            ConfigArgument('timeout', default=3600), # Default timeout is 1 hour
        ],
        'metrics': [
            ConfigArgument('namespace'),
            ConfigArgument('region', default='eu-west-1'),
            ConfigArgument('rate', default=60)
        ]
    }

    return {
        key: {
            arg.key: arg.parse(arg.key, config[key] if config else None) for arg in keys[key]
        } for key in keys
    }

def parse_cli_arguments(
    cli_json: dict,
    default_entrypoint = None
) -> dict:

    class CLIArgument:
        def __init__(self, key, default, type):
            self.key=key
            self.default=default
            self.type=type
        def parse(self, key, input):
            if key not in input:
                input[key] = self.default
            if isinstance(input[key], str) and self.type == list:
                return [input[key]]
            return self.type(input[key]) if isinstance(input[key], self.type) else input[key]

    args = [
        CLIArgument('entrypoint', default_entrypoint, list),
        CLIArgument('command', [], list),
        CLIArgument('job', None, str),
    ]

    return {
        arg.key: arg.parse(arg.key, cli_json) for arg in args
    }

def parse_command(commands: list, oenv: dict) -> list:
    for idx, command in enumerate(commands):
        r = re.compile(r'\$\{\{(.*?)\}\}')
        s = r.search(command)
        if not s:
            continue
        if s.group(1) not in oenv:
            continue
        commands[idx] = r.sub(lambda x: oenv[x.group(1)], command)
    return commands

def get_command(entrypoint: list, command: list, oenv: dict = None) -> list:
    cmd = []
    if entrypoint and len(entrypoint) > 0:
        cmd.extend(entrypoint)
    if command and len(command) > 0:
        cmd.extend(command)
    return parse_command(cmd, oenv)

def main() -> None:

    args = parse_arguments()
    oenv = parse_os_arguments()

    request_uuid = uuid.uuid4()
    now = datetime.utcnow()

    logging.basicConfig(
        format='%(asctime)s %(levelname)5s %(message)s',
        level=logging.INFO
    )
 
    logging.info("HTN: {0}".format(socket.gethostname()))
    logging.info("UID: {0}".format(request_uuid))
    logging.info("ARV: {0}".format(sys.argv))
    logging.info("OSV: {0}".format(oenv))

    config = parse_config(args.config)
    cli = parse_cli_arguments(
        args.cli_json,
        default_entrypoint=config['wrapper']['entrypoint']
    )

    logging.info("ARG: {0}".format(args))
    logging.info("CFG: {0}".format(config))
    logging.info("CLI: {0}".format(cli))

    cmd = get_command(cli['entrypoint'], cli['command'], oenv)
    logging.info("CMD: {0}".format(cmd))

    request = {
        'uuid': str(request_uuid),
        'host': socket.gethostname(),
        'args': sys.argv,
        'oenv': oenv,
        'config': config,
        'cli': cli,
        'cmd': cmd,
        'dry': args.dry,
        'timestamp': now.isoformat()
    }

    request_b64 = base64.b64encode(json.dumps(request).encode('utf-8'))
    logging.info("REQ: {0}".format(request_b64.decode('utf-8')))

    logging.info("MTS: {0}".format({
        'namespace': config['metrics']['namespace'],
        'region': config['metrics']['region'],
    }))
    metrics = WrapperMetrics(
        namespace=config['metrics']['namespace'],
        aws_region=config['metrics']['region']
    )

    metrics.add(SingleMetric(
        metric_name='Start',
        dimensions={
            'Job': cli['job'],
        },
        value=1,
    ))
    if not args.dry:
        _ = metrics.send()
    
    start = time.time()

    e = threading.Event()

    def cloudwatch_monitor(
        metrics: WrapperMetrics,
        job: str,
        start: float,
        rate: int,
        dry: bool = False
    ) -> None:
        while not e.is_set():
            m = SingleMetric(
                metric_name='Duration',
                dimensions={
                    'Job': job,
                },
                value=int(round(time.time() - start, 0)),
                unit='Seconds',
            )
            metrics.add(m)
            if not dry:
                _ = metrics.send()
            time.sleep(rate)
    
    t = threading.Thread(
        target=cloudwatch_monitor,
        args=(metrics, cli['job'], start, 5, args.dry)
    )
    t.start()

    try:
        p = subprocess.Popen(
            args=cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
    except Exception as e:    
        end = time.time()
        
        metrics.add(
            SingleMetric(
                metric_name='Exit',
                dimensions={
                    'Job': cli['job'],
                },
                value=1,
            ),
            SingleMetric(
                metric_name='End',
                dimensions={
                    'Job': cli['job'],
                },
                value=1,
            )
        )

        if not args.dry:
            _ = metrics.send()
        raise e

    exit_code = p.wait(timeout=float(config['wrapper']['timeout']))
    
    e.set()
    end = time.time()

    logging.info("EXC: {0}".format(exit_code))
    if exit_code != 0:
        logging.error("ERR: {0}".format(p.stderr.read()))
    logging.info("DUR: {0}".format(int(round(end - start, 0))))

    metrics.add(
        SingleMetric(
            metric_name='Exit',
            dimensions={
                'Job': cli['job'],
            },
            value=exit_code,
        ),
        SingleMetric(
            metric_name='End',
            dimensions={
                'Job': cli['job'],
            },
            value=1,
        )
    )

    if not args.dry:
        _ = metrics.send()