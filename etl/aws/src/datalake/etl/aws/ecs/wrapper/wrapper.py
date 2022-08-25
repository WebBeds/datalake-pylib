from datetime import datetime
import logging
import base64
import signal 
import socket
import json
import time
import uuid
import sys

from .args import (
    parse_argv,
    parse_os_args,
    parse_config,
    parse_cli,
)

from .command import (
    get_command,
)

from .metrics import (
    WrapperMetrics, 
    SingleMetric,
)

from .process import (
    WrapperProcess
)

def get_dimensions(job: str, metrics: WrapperMetrics) -> dict:
    return {
        'Team': metrics.team,
        'Group': metrics.group,
        'Job': job,
    }

def main() -> None:
    
    args = parse_argv()
    oenv = parse_os_args()

    request_uuid = uuid.uuid4()
    if "ExecutionId" in oenv and oenv["ExecutionId"]:
        request_uuid = oenv["ExecutionId"]

    now = datetime.utcnow()

    logging.basicConfig(
        format='%(asctime)s %(levelname)5s %(message)s',
        level=logging.DEBUG if args.debug else logging.INFO,
    )

    logging.info("HTN: {0}".format(socket.gethostname()))
    logging.info("UID: {0}".format(request_uuid))
    logging.info("ARV: {0}".format(sys.argv))
    logging.info("OSV: {0}".format(oenv))

    config = parse_config(
        config_path=args.config,
    )
    cli = parse_cli(
        cli_json=args.cli_json,
        config=config
    )
    cmd = get_command(
        entrypoint=cli["entrypoint"],
        command=cli["command"],
        oenv=oenv
    )

    logging.info("ARG: {0}".format(args))
    logging.info("CFG: {0}".format(config))
    logging.info("CLI: {0}".format(cli))
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

    metrics = WrapperMetrics(
        namespace=config['metrics']['namespace'],
        team=config['metrics']['team'],
        group=cli['group'],
        aws_region=config['metrics']['region']
    )
    logging.info("MTS: {0}".format(metrics))

    metrics.add(SingleMetric(
        metric_name='Start',
        dimensions=get_dimensions(cli['job'], metrics),
        value=1,
    ))
    if not args.dry:
        _ = metrics.send()

    proc = WrapperProcess(
        cmd=cmd,
        metrics=metrics,
        rate=int(config['metrics']['rate']),
        dimensions=get_dimensions(cli['job'], metrics)
    )

    signals_to_handle = [
        signal.SIGTERM
    ]
    for sig in signals_to_handle: 
        signal.signal(sig, proc.signal_handler)

    # TODO: Add actions to be performed on start

    # NOTE: Run Process
    exit_code, duration, p = proc.run(dry=args.dry)

    logging.info("EXC: {0}".format(exit_code))
    if exit_code != 0 and p:
        logging.error("ERR: {0}".format(p.stderr.read()))
    logging.info("DUR: {0}".format(int(round(duration, 0))))

    metrics.add(
        SingleMetric(
            metric_name='Duration',
            dimensions=get_dimensions(cli['job'], metrics),
            value=int(round(duration, 0)),
            unit='Seconds',
        ),
        SingleMetric(
            metric_name='Exit',
            dimensions=get_dimensions(cli['job'], metrics),
            value=0 if exit_code == 0 else 1,
        ),
        SingleMetric(
            metric_name='Exit',
            dimensions={
                'Team': metrics.team
            },
            value=0 if exit_code == 0 else 1,
        ),
        SingleMetric(
            metric_name='End',
            dimensions=get_dimensions(cli['job'], metrics),
            value=1,
        )
    )

    if not args.dry:
        _ = metrics.send()

    # TODO: Add actions to be performed on exit

    # NOTE: Return exit code to caller
    exit(exit_code)