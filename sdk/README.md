# DataLake SDK for Python

**Work in progress**

Manage and obtain data from DataLake API.

## Install (pip3)

Using install command:

```bash
pip3 install "git+https://www.github.com/Webjet/datalake-pylib#egg=datalake-sdk&subdirectory=sdk"
```

Inside requirements:

```bash
git+https://www.github.com/Webjet/datalake-pylib#egg=datalake-sdk&subdirectory=sdk
```

## Import

```python
import datalake.sdk as sdk
```

## Basic Usage

Obtain 150 Hotels from DataLake API:
```python
# Import the SDK
import datalake.sdk as sdk

if __name__ == "__main__":

    token = "YOUR_TOKEN"

    # Create the session
    session = sdk.Session(environment="prod", auth_token=token)

    # Create the service
    svc = sdk.HotelService(session)

    # Obtain the collection of Hotels
    hotels = svc.get_paginated(platform="SH", page=0, page_size=150)

    # View hotels as dataframe
    print(hotels.to_dataframe())
```