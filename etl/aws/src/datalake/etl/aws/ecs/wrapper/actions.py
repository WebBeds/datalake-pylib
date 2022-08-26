from dataclasses import dataclass
import requests
import logging
import json
import time

from .command import parse_command

# Stages
START = "start"
END = "end"

MAX_RETRIES = 3
RETRY_SLEEP = 5

@dataclass
class Action:
    stage: str
    action: str
    url: str
    params: dict
    headers: dict
    payload: dict
    method: str = "POST"

    def execute(self, oenv: dict = None, max_retries: int = MAX_RETRIES, dry: bool = False) -> None:

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
            json=self.payload
        )
        r = r.prepare()

        retry = 0
        with requests.Session() as s:
            while retry < max_retries:
                try:
                    res = s.send(r)
                    if res.status_code == 200:
                        break
                    raise Exception(f"{res.status_code} {res.reason}")
                except:
                    logging.debug(f"FAILED: {retry}. Retrying in {RETRY_SLEEP} seconds...")
                    time.sleep(RETRY_SLEEP)
                    retry += 1

def parse_action(action_data: dict) -> Action:

    if isinstance(action_data, str):
        action_data = json.loads(action_data)

    default_params = {}
    default_headers = {}
    default_payload = {}
    default_method = "POST"

    if "stage" not in action_data or \
        "action" not in action_data or \
        "url" not in action_data:
        return None

    stage = action_data["stage"]
    action = action_data["action"]
    url = action_data["url"]
    params = default_params
    headers = default_headers
    payload = default_payload
    method = default_method

    if "params" in action_data and isinstance(action_data["params"], dict):
        params = action_data["params"]
    if "headers" in action_data and isinstance(action_data["headers"], dict):
        headers = action_data["headers"]
    if "payload" in action_data and isinstance(action_data["payload"], dict):
        payload = action_data["payload"]
    if "method" in action_data and isinstance(action_data["method"], str):
        method = str(action_data["method"]).upper()

    return Action(
        stage=stage,
        action=action,
        url=url,
        params=params,
        headers=headers,
        payload=payload,
        method=method
    )

class WrapperActions:

    def __init__(self, actions: list, oenv: dict = None) -> None:
        self.actions = actions
        self.oenv = oenv

    def execute(self, stage: str, max_retries: int = MAX_RETRIES, dry: bool = False) -> None:
        logging.debug("Executing actions for stage: {0}".format(stage))
        for action in self.actions:
            if not isinstance(action, Action):
                continue
            if action.stage != stage:
                continue
            try:
                logging.debug("Executing action: {0}".format(action.action))
                action.execute(
                    oenv=self.oenv,
                    max_retries=max_retries,
                    dry=dry
                )
            except Exception as e:
                logging.error("Failed to execute action: {0}".format(action))
                logging.error(e)
        logging.debug("Finished executing actions")

    def __str__(self) -> str:
        return "WrapperActions(actions={0}, oenv={1})".format(self.actions, self.oenv)

    @staticmethod
    def parse(actions: list, oenv: dict = None):

        parsed_actions = []
        for action in actions:
            parsed_action = parse_action(action)
            if not parsed_action:
                continue
            parsed_actions.append(parsed_action)

        return WrapperActions(
            actions=parsed_actions,
            oenv=oenv,
        )