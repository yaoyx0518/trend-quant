from enum import Enum


class SignalAction(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


class SignalLevel(str, Enum):
    INFO = "INFO"
    WARN = "WARN"
    ACTION = "ACTION"
    ERROR = "ERROR"
