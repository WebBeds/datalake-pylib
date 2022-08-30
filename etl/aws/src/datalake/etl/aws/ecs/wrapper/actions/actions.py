from dataclasses import dataclass
from abc import abstractmethod
import logging

START = "start"
END = "end"

@dataclass
class Action:
    stage: str
    action: str

    def _parse_default_attr(data: dict):
        """
        Parse the default attributes of the action.

        :param data: The data to parse.
        :return: The stage and action.
        """
        if not isinstance(data, dict):
            raise ValueError("The data must be a dict.")
        if "stage" not in data and "action" not in data:
            raise ValueError("The data must contain the stage and action.")
        return data["stage"], data["action"]

    @abstractmethod
    def execute(self, oenv: dict, dry: bool = False) -> None:
        """
        Execute the action.

        :param oenv: The oenv to use.
        :param dry: Whether to dry run or not.

        :return: None.
        """
        pass

    @staticmethod
    def parse(data: dict):
        """
        Parse the action.

        :param data: The data to parse.
        :return: The action instance.
        """
        return Action(
            **data
        )

@dataclass
class WrapperActions:
    actions: list
    oenv: dict

    def execute(self, stage: str, dry: bool = False) -> None:
        """
        Execute the actions.

        :param stage: The stage to execute.
        :param dry: Whether to dry run or not.

        :return: None.
        """
        logging.debug("Executing actions for stage: {0}".format(stage))
        for action in self.actions:
            if not isinstance(action, Action) or not issubclass(action.__class__, Action):
                continue
            if action.stage != stage:
                continue
            try:
                logging.debug("Executing action: {0}".format(action.action))
                action.execute(
                    oenv=self.oenv,
                    dry=dry
                )
            except Exception as e:
                logging.error("Failed to execute action: {0}".format(action))
                logging.error(e)
        logging.debug("Finished executing actions")

    def __str__(self) -> str:
        return "WrapperActions(actions={0}, oenv={1})".format(self.actions, self.oenv)