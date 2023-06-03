import asyncio
import inspect
import json
import struct
import sys
from typing import Any, Union, Optional
from logging import getLogger

from .exceptions import (
    DiscordNotFound,
    InvalidPipe,
    InvalidID,
    DiscordError,
    PyPresenceException,
    PipeClosed,
    ServerError,
    ResponseTimeout,
    ConnectionTimeout,
    InvalidArgument,
)
from .payloads import Payload
from .utils import get_ipc_path, get_event_loop

logger = getLogger("pypresence.client")


class BaseClient:
    def __init__(
        self,
        client_id: Union[str, int],
        loop: Optional[asyncio.AbstractEventLoop] = None,
        handler=None,
        pipe: Optional[Any] = None,
        isasync: bool = False,
        connection_timeout: int = 30,
        response_timeout: int = 15,
    ):
        self.pipe = pipe
        self.isasync = isasync
        self.connection_timeout = connection_timeout
        self.response_timeout = response_timeout

        client_id = str(client_id)

        if loop is not None:
            self.update_event_loop(loop)
        else:
            self.update_event_loop(get_event_loop())

        self.sock_reader: Optional[asyncio.StreamReader] = None
        self.sock_writer: Optional[asyncio.StreamWriter] = None

        self.client_id = client_id

        if handler is not None:
            if not inspect.isfunction(handler):
                raise PyPresenceException("Error handler must be a function.")
            args = inspect.getfullargspec(handler).args
            if args[0] == "self":
                args = args[1:]
            if len(args) != 2:
                raise PyPresenceException(
                    "Error handler should only accept two arguments."
                )

            if self.isasync:
                if not inspect.iscoroutinefunction(handler):
                    raise InvalidArgument(
                        "Coroutine",
                        "Subroutine",
                        "You are running async mode - "
                        "your error handler should be awaitable.",
                    )
                err_handler = self._async_err_handle
            else:
                err_handler = self._err_handle

            loop.set_exception_handler(err_handler)
            self.handler = handler
        self._events_on = hasattr(self, "on_event")

    def update_event_loop(self, loop):
        # noinspection PyAttributeOutsideInit
        self.loop = loop
        asyncio.set_event_loop(self.loop)

    def _err_handle(self, loop, context: dict):
        result = self.handler(context["exception"], context["future"])
        if inspect.iscoroutinefunction(self.handler):
            asyncio.create_task(result)

    # noinspection PyUnusedLocal
    async def _async_err_handle(self, loop, context: dict):
        await self.handler(context["exception"], context["future"])

    async def read_output(self):
        if not self.sock_reader:
            raise PipeClosed
        try:
            preamble = await asyncio.wait_for(
                self.sock_reader.read(8), self.response_timeout
            )
            status_code, length = struct.unpack("<II", preamble[:8])
            data = await asyncio.wait_for(
                self.sock_reader.read(length), self.response_timeout
            )
        except (BrokenPipeError, struct.error):
            raise PipeClosed
        except asyncio.TimeoutError:
            raise ResponseTimeout

        logger.debug(f"Received data: {data.decode('utf-8')}")

        payload = json.loads(data.decode("utf-8"))

        if payload["evt"] == "ERROR":
            raise ServerError(payload["data"]["message"])

        return payload

    def send_data(self, op: int, payload: Union[dict, Payload]):
        if isinstance(payload, Payload):
            payload = payload.data
        payload = json.dumps(payload)

        assert (
            self.sock_writer is not None
        ), "You must connect your client before sending events!"

        logger.debug(f"Sending data: {payload}")

        self.sock_writer.write(
            struct.pack("<II", op, len(payload)) + payload.encode("utf-8")
        )

    async def handshake(self):
        ipc_path = get_ipc_path(self.pipe)
        if not ipc_path:
            raise DiscordNotFound

        try:
            if sys.platform == "linux" or sys.platform == "darwin":
                self.sock_reader, self.sock_writer = await asyncio.wait_for(
                    asyncio.open_unix_connection(ipc_path), self.connection_timeout
                )
            elif sys.platform == "win32" or sys.platform == "win64":
                self.sock_reader = asyncio.StreamReader(loop=self.loop)
                reader_protocol = asyncio.StreamReaderProtocol(
                    self.sock_reader, loop=self.loop
                )
                self.sock_writer, _ = await asyncio.wait_for(
                    self.loop.create_pipe_connection(lambda: reader_protocol, ipc_path),
                    self.connection_timeout,
                )
        except FileNotFoundError:
            raise InvalidPipe
        except asyncio.TimeoutError:
            raise ConnectionTimeout

        self.send_data(0, {"v": 1, "client_id": self.client_id})

        preamble = await self.sock_reader.read(8)

        code, length = struct.unpack("<ii", preamble)

        data = json.loads(await self.sock_reader.read(length))

        if "code" in data:
            if data["message"] == "Invalid Client ID":
                raise InvalidID
            raise DiscordError(data["code"], data["message"])

        if self._events_on:
            self.sock_reader.feed_data = self.on_event
