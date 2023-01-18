import sys
from .metrics import WrapperMetrics, SingleMetric
from threading import Event, Thread
from subprocess import Popen, PIPE

import signal as sg
import logging
import time

DEFAULT_STOP_TIMEOUT = 120 # 2 minutes

class WrapperProcess:
    e: Event # Event for threads
    mt: Thread = None # Metrics Thread

    p: Popen = None # Process
    mp: Thread = None# Process Thread for handle signals

    def __init__(self, cmd: list, metrics: WrapperMetrics, rate: int, timeout: int = None, stop_timeout: int = DEFAULT_STOP_TIMEOUT, dimensions: dict = None) -> None:
        self.cmd = cmd
        self.metrics = metrics
        self.rate = rate
        self.timeout = timeout
        self.stop_timeout = stop_timeout
        self.dimensions = dimensions

        self.e = Event()

    def cloudwatch_monitor(self, start: int, rate: int, dry: bool = False) -> None:
        c = 0
        while not self.e.is_set():
            if c < rate:
                c += 1
                time.sleep(1)
                continue
            m = SingleMetric(
                metric_name='Duration',
                dimensions=self.dimensions,
                value=int(round(time.time() - start, 0)),
            )
            logging.debug('Adding rate metric: {0}'.format(m.to_cloudwatch_metric()))
            self.metrics.add(m)
            if not dry:
                _ = self.metrics.send()
            c = 0

    def signal_handler(self, signal, frame) -> None:
        logging.debug('Received signal: {}'.format(signal))

        if signal == sg.SIGTERM and self.p:
            logging.debug('Sending SIGTERM to process')
            self.p.send_signal(sg.SIGTERM)
            try:
                self.p.wait(timeout=self.stop_timeout)
            except Exception as e:
                logging.warning('Process did not stop within {} seconds'.format(self.stop_timeout))
                self.p.send_signal(sg.SIGKILL)
            self.e.set()
            return

        if signal == sg.SIGKILL and self.p:
            logging.debug('Sending SIGKILL to process')
            self.e.set()
            self.p.send_signal(sg.SIGKILL)
            try:
                self.p.wait(timeout=self.stop_timeout)
            except Exception as e:
                logging.warning('Process did not stop within {} seconds'.format(self.stop_timeout))
            return

        if signal == sg.SIGTERM and not self.p:
            sys.exit(0)

        # NOTE: Send signal to process
        return self.p.send_signal(signal) if self.p else None

    def _run_process(self):

        start = time.time()

        try:
            self.p = Popen(
                args=self.cmd,
                stdout=PIPE,
                stderr=PIPE,
                universal_newlines=True
            )
        except:
            end = time.time()
            self.e.set()
            return 1, (end - start), None

        exit_code = self.p.wait(timeout=self.timeout)
        self.e.set()
        end = time.time()

        return exit_code, (end - start), self.p

    def run(self, dry: bool = False, retries: int = None):
        
        if not retries or (retries and retries < 1):
            return self._run_process()

        exit_code, duration, p = self._run_process()
        if exit_code == 0:
            return exit_code, duration, p

        for retry in range(retries):
            
            exit_code, duration, p = self._run_process()
            if exit_code == 0:
                return exit_code, duration, p

            stderr = None
            stdout = None

            try:
                stderr = p.stderr.read() if p else None
            except:
                pass
        
            try:
                stdout = p.stdout.read() if p else None
            except:
                pass

            logging.error("{}{}ERR: ".format(
                f"RETRY {retry + 1}",
                "\t" * 2,
                stderr
            )) if stderr else None

            logging.error("{}{}OUT: ".format(
                f"RETRY {retry + 1}",
                "\t" * 2,
                stdout
            )) if stdout else None

            retries -= 1
        
        return exit_code, duration, p