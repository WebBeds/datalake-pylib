from datalake.sdk import Client

class Fetcher:

    _client: Client = None
    _fetchers: dict = None

    def __init__(self, client: Client) -> None:
        self._client = client
        self._fetchers = {}