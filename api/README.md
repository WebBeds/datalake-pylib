# DataLake API for Python

**Work in progress**

Manage and obtain data from DataLake API.

## Install (pip3)

Using install command:

```bash
pip3 install "git+ssh://github.com/Webjet/datalake-pylib#egg=datalake-api&subdirectory=api"
```

Inside requirements:

```bash
git+ssh://github.com/Webjet/datalake-pylib#egg=datalake-api&subdirectory=api
```

## Import

```python
import datalake.api as api
```

## Basic Usage

Obtain 150 Hotels from DataLake API:
```python
# Import SDK Generics
from datalake.sdk import Session, Collection

# Import the client
from datalake.api import APIClient

# Import the desired groups and fetchers.
from datalake.api.infrastructure import MasterFetcher, HotelFetcher

if __name__ == "__main__":

    token = "YOUR_TOKEN"

    # Create the session
    session = Session(environment="prod", auth_token=token)

    # Create the client (Custom API Client)
    client = APIClient(session)

    # Get the master service (Can be used to get the list of different master fetchers)
    # Such as HotelFetcher, SupplierFetcher, BookingFetcher, etc.
    fetcher: MasterFetcher = client.get_resource("master")
    
    # Get the hotels fetcher (To obtain the hotels in different ways)
    svc: HotelFetcher = fetcher.hotels()

    # Get the hotels
    hotels: Collection = svc.get_paginated(platform="SH", page=0, page_size=150)
    print(hotels.to_dataframe())
```