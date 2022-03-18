class SDKException(Exception):
    """
    Base exception class for all SDK exceptions.
    """
    def __init__(self, *args: object, status_code: int, content) -> None:
        super().__init__(*args)
        self.status_code = status_code
        self.content = content

    def get_message(self) -> str:
        return self.content
    
    def get_status_code(self) -> int:
        return self.status_code

    def __str__(self) -> str:
        return f"{self.status_code}: {self.content}"
    