import logging
import time
from dataclasses import dataclass

import requests

from ..command import parse_command
from .actions import Action

DEFAULT_METHOD = "POST"
MAX_RETRIES = 3
RETRY_SLEEP = 5


@dataclass
class HTTP(Action):
    url: str
    params: dict
    headers: dict
    payload: dict
    method: str = DEFAULT_METHOD
    retries: int = MAX_RETRIES
    retry_sleep: int = RETRY_SLEEP

    def execute(self, oenv: dict, dry: bool = False) -> None:

        if self.condition and not self.validate(oenv):
            logging.debug("Skipping action due to invalid condition: {0}".format(self.action))
            return

        for param, value in self.params.items():
            self.params[param] = parse_command(value, oenv)
        for header, value in self.headers.items():
            self.headers[header] = parse_command(value, oenv)
        for param, value in self.payload.items():
            self.payload[param] = parse_command(value, oenv)

        logging.debug(f"PARAMS: {self.params}")
        logging.debug(f"HEADERS: {self.headers}")
        logging.debug(f"PAYLOAD: {self.payload}")

        if dry:
            return

        r = requests.Request(
            method=self.method,
            url=self.url,
            headers=self.headers,
            params=self.params,
            json=self.payload,
        )
        r = r.prepare()

        retry = 0
        with requests.Session() as s:
            while retry < self.retries:
                try:
                    res = s.send(r)
                    if res.status_code == 200:
                        break
                    raise Exception(f"{res.status_code} {res.reason}")
                except:
                    logging.debug(f"FAILED: {retry}. Retrying in {self.retry_sleep} seconds...")
                    time.sleep(self.retry_sleep)
                    retry += 1

    def parse(data: dict):

        default_params = {}
        default_headers = {}
        default_payload = {}
        default_method = DEFAULT_METHOD
        default_max_retries = MAX_RETRIES
        default_retry_sleep = RETRY_SLEEP

        stage, action, condition = Action._parse_default_attr(data)

        if "url" not in data:
            raise ValueError("The data must contain the url.")
        url = data["url"]

        params = default_params
        headers = default_headers
        payload = default_payload
        method = default_method
        max_retries = default_max_retries
        retry_sleep = default_retry_sleep

        if "params" in data and isinstance(data["params"], dict):
            params = data["params"]
        if "headers" in data and isinstance(data["headers"], dict):
            headers = data["headers"]
        if "payload" in data and isinstance(data["payload"], dict):
            payload = data["payload"]
        if "method" in data and isinstance(data["method"], str):
            method = str(data["method"]).upper()
        if "retries" in data and isinstance(data["retries"], int):
            max_retries = data["retries"]
        if "retry_sleep" in data and isinstance(data["retry_sleep"], int):
            retry_sleep = data["retry_sleep"]

        return HTTP(
            stage=stage,
            action=action,
            condition=condition,
            url=url,
            params=params,
            headers=headers,
            payload=payload,
            method=method,
            retries=max_retries,
            retry_sleep=retry_sleep,
        )
