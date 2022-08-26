import awswrangler as wr
import logging
import json
import re

pattern = re.compile(r'\$\{\{(.*?)\}\}')

PLUGINS = [
    "secretsmanager"
]

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
            return None

    return None

def parse_plugins(
    command: str,
    oenv: dict = None
) -> str:
    for plugin in PLUGINS:
        p = re.compile(r'\$\{\{' + plugin + r'.(.*?)\}\}')
        s = p.search(command)
        if not s:
            continue
        o = parse_plugin(plugin, s.group(1), oenv)
        if not o:
            continue
        return o
    return None

def parse_command(
    command: str,
    oenv: dict = None
):
    if parse_plugins(command, oenv):
        return parse_plugins(command, oenv)
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