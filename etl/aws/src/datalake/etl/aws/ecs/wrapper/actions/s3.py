import logging
import os
import tempfile
import time
from dataclasses import dataclass

import boto3

from ..command import parse_command
from .actions import Action

DEFAULT_AWS_REGION = "eu-west-1"
AVAILABLE_MODES = [
    "GET",
    "PUT",
]


@dataclass
class S3(Action):
    bucket: str
    key: str

    profile: str = None
    region: str = DEFAULT_AWS_REGION

    output: str = None
    mode: str = "GET"

    def _get_session(self, oenv: dict) -> boto3.Session:
        if self.profile:
            return boto3.Session(region_name=self.region, profile_name=self.profile)
        else:
            return boto3.Session(region_name=self.region)

    def _get(self, svc, oenv: dict) -> None:
        output: str = self.output
        if output:
            if os.path.isdir(output):
                output = os.path.join(output, self.key.split("/")[-1])
            with open(output, "wb") as f:
                svc.download_fileobj(self.bucket, self.key, f)
        else:
            delete = False
            suffix = None
            try:
                # Get the extension of the file
                suffix = self.key.split(".")[-1]
            except:
                pass
            with tempfile.NamedTemporaryFile(
                delete=delete, mode="wb", suffix=f".{suffix}" if suffix else None
            ) as f:
                svc.download_fileobj(self.bucket, self.key, f)
                output = f.name

        oenv.update({"S3_GET_OUTPUT": output})

    def _put(self, svc, oenv: dict) -> None:
        raise NotImplementedError("PUT is not implemented yet.")

    def execute(self, oenv: dict, dry: bool = False) -> None:

        if self.condition and not self.validate(oenv):
            logging.debug("Skipping action due to invalid condition: {0}".format(self.action))
            return

        self.bucket = parse_command(self.bucket, oenv)
        self.key = parse_command(self.key, oenv)
        self.output = parse_command(self.output, oenv)

        logging.debug(f"BUCKET: {self.bucket}")
        logging.debug(f"KEY: {self.key}")
        logging.debug(f"OUTPUT: {self.output}")
        logging.debug(f"PROFILE: {self.profile}")
        logging.debug(f"REGION: {self.region}")
        logging.debug(f"MODE: {self.mode}")

        if dry:
            return

        sess = self._get_session(oenv)
        svc = sess.client("s3")

        if self.mode == "GET":
            self._get(svc, oenv)
        elif self.mode == "PUT":
            self._put(svc, oenv)

    def parse(data: dict):

        bucket: str = None
        key: str = None
        profile: str = None
        region: str = DEFAULT_AWS_REGION
        output: str = None
        mode: str = "GET"  # Default to GET

        stage, action, condition = Action._parse_default_attr(data)

        if "bucket" not in data and "key" not in data:
            raise ValueError("The data must contain the bucket and key.")

        bucket = data["bucket"]
        key = data["key"]

        if "profile" in data and isinstance(data["profile"], str):
            profile = data["profile"]
        if "region" in data and isinstance(data["region"], str):
            region = data["region"]
        if "output" in data and isinstance(data["output"], str):
            output = data["output"]
        if "mode" in data and isinstance(data["mode"], str):
            if data["mode"].upper() not in AVAILABLE_MODES:
                raise ValueError(f"The mode must be one of {AVAILABLE_MODES}")
            mode = data["mode"].upper()

        return S3(
            stage=stage,
            action=action,
            condition=condition,
            bucket=bucket,
            key=key,
            profile=profile,
            region=region,
            output=output,
            mode=mode,
        )
