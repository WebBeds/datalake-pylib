from dataclasses import dataclass
import awswrangler as wr
import logging
import json
import re
import os

pattern = re.compile(r'\$\{\{(.*?)\}\}')

PLUGINS = [
    "secretsmanager",
    "oenv"
]

@dataclass
class ReplacedCommand:
    commands: list
    action: str = "join"

    # NOTE: Valid actions
    valid_actions = [
        "join"
    ]

    def parse(command):
        if not isinstance(command, dict):
            raise ValueError("The command must be a dict.")
        if "commands" not in command:
            raise ValueError("The command must contain the commands.")
        if not isinstance(command["commands"], list):
            raise ValueError("The commands must be a list.")
        action = "join"
        if "action" in command and command["action"] in ReplacedCommand.valid_actions:
            action = command["action"]
        return ReplacedCommand(
            commands=command["commands"],
            action=action
        )

def parse_plugin(
    plugin: str,
    value: str,
    oenv: dict = None
) -> str:

    # ==========================
    # secretsmanager
    # ==========================

    if plugin == "secretsmanager":

        secret_search_key = "value"
        secret_format = "plain"

        if len(value.split(";")) > 1:
            # value;type;search_key
            secret_format = value.split(";")[1]
            secret_search_key = value.split(";")[2]
            value = value.split(";")[0]

        logging.debug(f"SECRETSMANAGER: getting value {value}")
        try:
            s = wr.secretsmanager.get_secret(
                value
            )
            # Parse format
            if secret_format == "json":
                s = json.loads(s)                
            # Parse search key
            if secret_format == "json" and secret_search_key in s:
                return s[secret_search_key]
            return s
        except:
            logging.debug(f"SECRETSMANAGER: failed to get value {value}")
            raise Exception(f"SECRETSMANAGER: failed to get value {value}")

    if plugin == "oenv":
        logging.debug(f"OENV: getting value {value}")
        if value not in oenv:
            raise Exception(f"OENV: failed to get value {value}")
        return oenv[value]

    if plugin == "env":
        logging.debug(f"ENV: getting value {value}")
        if value not in os.environ:
            raise Exception(f"ENV: failed to get value {value}")
        return os.environ[value]

    raise Exception(f"Unknown plugin: {plugin}")

def parse_plugins(
    command: str,
    oenv: dict = None
) -> str:
    for plugin in PLUGINS:
        p = re.compile(r'\$\{\{' + plugin + r'.(.*?)\}\}')
        s = p.search(command)
        if not s:
            continue
        try:
            return parse_plugin(plugin, s.group(1), oenv)
        except:
            pass # Ignore if not found.
    raise Exception("No plugin found")

def parse_replaced_commands(
    command: str,
    oenv: dict = None
) -> str:
    rc = ReplacedCommand.parse(command)
    if rc.action == "join":
        cmd = []
        for c in rc.commands:
            try:
                cmd.append(parse_command(c, oenv))
            except:
                cmd.append(c)
        return " ".join(cmd)
    raise Exception(f"Unknown action: {rc.action}")

def parse_command(
    command: str,
    oenv: dict = None
):
    try:
        return parse_replaced_commands(command, oenv)
    except:
        pass # Ignore if not found.
    try:
        return parse_plugins(command, oenv)
    except:
        pass # NOTE: Ignore if not found.
    s = pattern.search(command)
    if not s or not oenv:
        return command
    if s.group(1) not in oenv:
        return command
    return pattern.sub(lambda x: oenv[x.group(1)], command)

def parse_commands(
    commands: list,
    oenv: dict
) -> list:
    if not oenv:
        return commands
    for idx, command in enumerate(commands):
        commands[idx] = parse_command(command, oenv)
    return commands

def get_command(
    entrypoint: list,
    command: list,
    oenv: dict = None
) -> list:
    cmd = []
    if entrypoint and len(entrypoint) > 0:
        cmd.extend(entrypoint)
    if command and len(command) > 0:
        cmd.extend(command)
    return parse_commands(cmd, oenv)