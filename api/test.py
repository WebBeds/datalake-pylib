#!/usr/bin/env python3
# ./tests/test.py

import argparse

from unittest import TextTestRunner, TestSuite, TestLoader
from datalake.sdk import Session
from datalake.api import APIClient
from tests.infrastructure import (
    Master,
    MPA
)

SDK_TESTS = [
    Master,
    MPA
]

def init_tests(list: list, test_loader: TestLoader, client: APIClient) -> list:
    for idx, test in enumerate(list):
        # Setup client on class
        list[idx].client = client
        list[idx] = test()
        list[idx] = test_loader.loadTestsFromTestCase(test)
    return list

def main(args):

    suite = TestSuite()
    loader = TestLoader()

    # Create the session (Environment PROD/DEV)
    session = Session(environment=args.env, auth_token=args.token)

    # Create the client
    client = APIClient(session)

    suite.addTests(init_tests(SDK_TESTS, loader, client))

    runner = TextTestRunner(
        verbosity=2 if args.verbose else 1
    )

    runner.run(suite)

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(
        description='Run tests for the DataLake SDK.'
    )

    parser.add_argument(
        '--token',
        type=str,
        required=True,
        help='The bearer token to use for authentication.',
    )

    parser.add_argument(
        '--env',
        choices=['prod', 'dev'],
        default='prod',
        type=str,
        help='The environment to use for authentication.',
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Verbose output.'
    )

    args = parser.parse_args()

    main(args)