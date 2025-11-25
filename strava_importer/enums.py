from enum import Enum, auto


class UploadResult(Enum):
    SUCCESS = auto()
    DUPLICATE = auto()
    RATE_LIMITED = auto()
    SERVER_ERROR = auto()
    FAILED = auto()