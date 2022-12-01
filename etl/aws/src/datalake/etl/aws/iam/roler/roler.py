import subprocess
import argparse
import boto3
import sys
import os

def parse_argv() -> argparse.Namespace:
    
    parser = argparse.ArgumentParser(
        description="Assume a role and run a command",
    )

    parser.add_argument(
        "-r", "--role",
        type=str,
        help="Role to assume",
        required=True,
    )

    parser.add_argument(
        "-o", "--operation",
        type=str,
        help="Operation to perform",
        choices=[
            "assume",
            "install",
        ],
        default="assume",
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )

    parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="Command to run",
    )

    args = parser.parse_args()

    return args

def get_account() -> dict:
    sts = boto3.client("sts")
    return sts.get_caller_identity()

def assume_role(role: str, assumed_by: dict) -> dict:
    arn: str = f"arn:aws:iam::{assumed_by['Account']}:role/{role}"
    sts = boto3.client("sts")
    return sts.assume_role(
        RoleArn=arn,
        RoleSessionName=f"{role},{assumed_by['Account']},roler",
        ExternalId=assumed_by["Account"],
    )

def _assume(args: argparse.Namespace) -> None:
    
    acc: dict = get_account()
    ass: dict = assume_role(args.role, acc)

    access_token, access_secret, access_session = (
        ass['Credentials']['AccessKeyId'],
        ass['Credentials']['SecretAccessKey'],
        ass['Credentials']['SessionToken'],
    )

    exit(subprocess.call(
        args=args.command,
        stdin=sys.stdin,
        stdout=sys.stdout,
        stderr=sys.stderr,
        shell=False,
        env={
            **os.environ,
            "AWS_ACCESS_KEY_ID": access_token,
            "AWS_SECRET_ACCESS_KEY": access_secret,
            "AWS_SESSION_TOKEN": access_session,
        }
    ))

def _install(args: argparse.Namespace) -> None:
    raise NotImplementedError

def main() -> None:

    args: argparse.Namespace = parse_argv()

    if args.operation == "assume":
        _assume(args)
    elif args.operation == "install":
        _install(args)
    else:
        raise ValueError(f"Unknown operation: {args.operation}")