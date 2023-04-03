import json
import logging

from .actions import END, START, Action, WrapperActions

# PLUGINS
from .http import HTTP
from .s3 import S3
from .sns import SNSECSErrorTrace

DEFAULT_PLUGIN = "HTTP"
DEFAULT_PLUGIN_SEARCH_KEY = "plugin"


def _parse_plugin(plugin_type: str, data: dict) -> Action:
    """
    Parse the plugin.

    :param plugin_type: The plugin type.
    :param data: The data to parse.

    :return: The action instance.
    """

    if plugin_type == "HTTP":
        return HTTP.parse(data)
    elif plugin_type == "S3":
        return S3.parse(data)
    elif plugin_type == "ECS_SNSTRACE":
        return SNSECSErrorTrace.parse(data)

    return None


def _parse_action(data: dict) -> Action:
    if isinstance(data, str):
        data = json.loads(data)

    plugin_type = DEFAULT_PLUGIN
    for key in data.keys():
        key: str
        if key.lower() != DEFAULT_PLUGIN_SEARCH_KEY:
            continue
        plugin_type = data[key]
        break

    return _parse_plugin(plugin_type.upper(), data)


def parse_actions(actions: list, oenv: dict) -> WrapperActions:

    parsed_actions = []
    for action in actions:
        try:
            parsed_action = _parse_action(action)
        except:
            logging.warning(f"Failed to parse action: {action}")
            parsed_action = None
        if not parsed_action:
            continue
        parsed_actions.append(parsed_action)

    return WrapperActions(actions=parsed_actions, oenv=oenv)
