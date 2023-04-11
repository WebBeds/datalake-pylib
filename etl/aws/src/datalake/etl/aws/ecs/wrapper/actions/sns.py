import base64
import json
import logging
import os
from dataclasses import dataclass

import boto3
import requests

from ..command import parse_command
from .actions import Action


@dataclass
class SNSECSErrorTrace(Action):
    topic: str

    name: str
    subject: str

    level: str = "ERROR"

    def obtain_context(self, oenv: dict):
        self.topic = parse_command(self.topic, oenv)

        cli: dict = oenv.get("_execution", {}).get("cli", {})
        if not self.name:
            self.name = cli.get("job", None)
        if not self.subject:
            self.subject = cli.get("job", None)

    def obtain_logstream(self) -> str:
        metadata_url: str = os.environ.get("ECS_CONTAINER_METADATA_URI_V4", None)
        if not metadata_url:
            raise ValueError("The ECS_CONTAINER_METADATA_URI_V4 is not set.")

        response = requests.get(url="{0}/task".format(metadata_url), timeout=5)

        if response.status_code != 200:
            raise ValueError("The response status code is not 200.")

        log_options: dict = response.json().get("Containers", [])[0].get("LogOptions", {})
        if not log_options:
            raise ValueError("The log options are empty.")

        log_url: str = "https://{0}.console.aws.amazon.com/cloudwatch/home?region={0}#logEventViewer:group={1};stream={2}".format(
            log_options.get("awslogs-region"),
            log_options.get("awslogs-group"),
            log_options.get("awslogs-stream"),
        )

        if not log_url:
            raise ValueError("The log url is empty.")

        return log_url

    def obtain_message(self, oenv: dict) -> str:
        # Obtain StdErr
        stderr = oenv.get("StdErr", None)
        if not stderr:
            return base64.b64encode(
                "StdErr is empty, maybe is from OutOfMemory error.".encode("utf-8")
            ).decode("utf-8")

        # Obtain the last 10 lines of the StdErr
        stderr = stderr.splitlines()[-10:]

        message = """\n**Error summary:**\n\n\n```{}""".format("\n".join(stderr))

        return base64.b64encode(message.encode("utf-8")).decode("utf-8")

    def execute(self, oenv: dict, dry: bool = False) -> None:
        if self.condition and not self.validate(oenv):
            logging.debug("Skipping action due to invalid condition: {0}".format(self.action))
            return

        self.obtain_context(oenv)
        logs_url = self.obtain_logstream()
        message = self.obtain_message(oenv)

        logging.debug("Logs URL: {0}".format(logs_url))
        logging.debug("Message: {0}".format(message))

        if dry:
            return

        client = boto3.client("sns")
        client.publish(
            TopicArn=self.topic,
            Subject=self.subject,
            # Message is the JSON payload of the notification message.
            Message=json.dumps(
                {
                    "name": self.name,
                    "subject": self.subject,
                    "message": message,
                    "link": logs_url,
                    "level": self.level,
                }
            ),
        )

    @staticmethod
    def parse(data: dict):
        stage, action, condition = Action._parse_default_attr(data)

        if not data.get("topic"):
            raise ValueError("The topic is required.")

        name: str = data.get("name", None)
        subject: str = data.get("subject", None)

        return SNSECSErrorTrace(
            stage=stage,
            action=action,
            condition=condition,
            topic=data.get("topic"),
            name=name,
            subject=subject,
        )
