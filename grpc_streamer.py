import asyncio
import logging

import grpc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class BaseStreamer:
    RECONNECT_DELAY = 3  # seconds

    def __init__(self, target_address: str, stub_class: type):
        self.target_address = target_address
        self.stub_class = stub_class
        self.channel: grpc.aio.Channel | None = None
        self.stub = None
        self.stream = None
        self._lock = asyncio.Lock()
        self._connecting = asyncio.Lock()

    async def connect(self):
        async with self._connecting:
            if self.channel:
                await self.channel.close()

            # Always recreate channel and stub
            self.channel = grpc.aio.insecure_channel(self.target_address)
            self.stub = self.stub_class(self.channel)
            self.stream = await self._create_stream()
            logger.info("Connected gRPC stream to %s", self.target_address)

    async def _create_stream(self):
        """Override in subclass to call correct stub method."""
        raise NotImplementedError

    async def _ensure_stream_alive(self):
        """Reconnect if stream is closed or broken."""
        if not self.stream or self.stream.done():
            logger.warning("Stream closed — reconnecting...")
            await self.connect()

    async def send_data(self, data):
        async with self._lock:  # prevent concurrent writes
            try:
                await self._ensure_stream_alive()
                await self.stream.write(data)
            except (grpc.aio.AioRpcError, asyncio.InvalidStateError) as e:
                logger.error(f"Stream error: {e}")
                await self._handle_stream_error()
                # Retry once after reconnect
                try:
                    await self.stream.write(data)
                except Exception as e:
                    logger.exception(f"Failed after reconnect: {e}")

    async def _handle_stream_error(self):
        """Reconnect with delay if possible."""
        try:
            if self.stream:
                await self.stream.done_writing()
        except Exception:
            pass

        await asyncio.sleep(self.RECONNECT_DELAY)
        await self.connect()

    async def close(self):
        try:
            if self.stream:
                await self.stream.done_writing()
        except Exception:
            pass

        if self.channel:
            await self.channel.close()
            logger.info("Stream closed.")
