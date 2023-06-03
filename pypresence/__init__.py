"""
Python RPC Client for Discord
-----------------------------
By: qwertyquerty and LewdNeko
"""

from .baseclient import BaseClient
from .client import Client, AioClient
from .exceptions import (
    DiscordError,
    InvalidArgument,
    InvalidID,
    InvalidPipe,
    PipeClosed,
    PyPresenceException,
    ResponseTimeout,
    ArgumentError,
    ConnectionTimeout,
    DiscordNotFound,
    EventNotFound,
    ServerError,
)
from .presence import Presence, AioPresence


__title__ = "pypresence"
__author__ = "qwertyquerty"
__copyright__ = "Copyright 2018 - Current qwertyquerty"
__license__ = "MIT"
__version__ = "4.2.4"
__all__ = (
    "BaseClient",
    "Client",
    "AioClient",
    "Presence",
    "AioPresence",
    "DiscordError",
    "InvalidArgument",
    "InvalidID",
    "InvalidPipe",
    "PipeClosed",
    "PyPresenceException",
    "ResponseTimeout",
    "ArgumentError",
    "ConnectionTimeout",
    "DiscordNotFound",
    "EventNotFound",
    "ServerError",
)
