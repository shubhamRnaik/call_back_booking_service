"""
Packet Scheduler: Regulates audio stream pacing and buffers chunks.
Eliminates network jitter by chunking audio into consistent time packets.
"""

import io
import asyncio
import logging
from typing import AsyncGenerator, Optional
from collections import deque
from datetime import datetime

from ..config import settings

logger = logging.getLogger(__name__)


class PacketScheduler:
    """
    Audio packet scheduler for jitter-free delivery.
    Buffers incoming audio chunks and emits them as consistent-sized packets.
    """

    def __init__(
        self,
        packet_duration_ms: Optional[int] = None,
        sample_rate: Optional[int] = None
    ) -> None:
        """
        Initialize packet scheduler.
        
        Args:
            packet_duration_ms: Duration of each packet in milliseconds (default: 20ms for 160 bytes)
            sample_rate: Audio sample rate in Hz (default: 8000)
        """
        self.packet_duration_ms = packet_duration_ms or 20  # 20ms standard for 8kHz streams
        self.sample_rate = sample_rate or settings.default_sample_rate
        self.bytes_per_sample = 2  # 16-bit audio
        self.channels = 1  # Mono
        
        # Calculate packet size in bytes
        # For 16-bit mono @ 8kHz: 20ms = 160 bytes, 8kHz * 2 bytes * 1 chan * 0.020sec
        # For 16-bit mono @ 8kHz: 20ms = (8000 * 2 * 1 * 20) / 1000 = 320 bytes (standard)
        self.packet_size_bytes = int(
            (self.sample_rate * self.channels * self.bytes_per_sample * self.packet_duration_ms) / 1000
        )

        # Internal buffer for accumulating audio chunks
        self._buffer: deque[bytes] = deque()
        self._buffer_size: int = 0
        self._packets_emitted: int = 0
        self._total_bytes_processed: int = 0
        self._start_time: Optional[datetime] = None
        
        # Barge-in / interruption support
        self._barge_in_triggered = False
        self._active_streams: set = set()
        self._stream_lock = asyncio.Lock()

        logger.info(
            f"Packet Scheduler initialized: "
            f"packet_size={self.packet_size_bytes} bytes, "
            f"duration={self.packet_duration_ms}ms, "
            f"sample_rate={self.sample_rate}Hz"
        )

    async def schedule_stream(
        self,
        incoming_stream: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[bytes, None]:
        """
        Schedule and regulate incoming audio stream.
        Buffers chunks and emits consistent-sized packets.
        Supports barge-in interruption.
        
        Args:
            incoming_stream: Async generator yielding audio chunks
            
        Yields:
            Regulated audio packets of consistent size
        """
        if self._start_time is None:
            self._start_time = datetime.now()

        try:
            # Register this stream task
            current_task = asyncio.current_task()
            if current_task:
                async with self._stream_lock:
                    self._active_streams.add(current_task)

            async for chunk in incoming_stream:
                # Check for barge-in interrupt
                if self._barge_in_triggered:
                    logger.warning("Stream interrupted by barge-in")
                    raise asyncio.CancelledError("Barge-in triggered")

                if not chunk:
                    continue

                self._buffer.append(chunk)
                self._buffer_size += len(chunk)
                self._total_bytes_processed += len(chunk)

                # Emit complete packets as they become available
                while self._buffer_size >= self.packet_size_bytes:
                    packet = self._extract_packet()
                    if packet:
                        self._packets_emitted += 1
                        yield packet

            # Emit remaining data as final packet
            if self._buffer_size > 0 and not self._barge_in_triggered:
                final_packet = self._flush_buffer()
                if final_packet:
                    self._packets_emitted += 1
                    yield final_packet

            logger.info(
                f"Packet Scheduler completed: "
                f"{self._packets_emitted} packets emitted, "
                f"{self._total_bytes_processed} bytes processed"
            )

        except asyncio.CancelledError:
            logger.info("Packet stream cancelled")
        except Exception as e:
            logger.error(f"Error in packet scheduling: {e}")
            raise
        finally:
            # Unregister this stream task
            current_task = asyncio.current_task()
            if current_task:
                async with self._stream_lock:
                    self._active_streams.discard(current_task)

    def _extract_packet(self) -> Optional[bytes]:
        """
        Extract a complete packet from buffer.
        
        Returns:
            Packet bytes or None if buffer insufficient
        """
        if self._buffer_size < self.packet_size_bytes:
            return None

        packet = bytearray()
        remaining_bytes = self.packet_size_bytes

        # Extract bytes from buffer queue
        while remaining_bytes > 0 and self._buffer:
            chunk = self._buffer[0]
            
            if len(chunk) <= remaining_bytes:
                # Entire chunk fits in packet
                self._buffer.popleft()
                packet.extend(chunk)
                remaining_bytes -= len(chunk)
                self._buffer_size -= len(chunk)
            else:
                # Partial chunk - split it
                part = chunk[:remaining_bytes]
                packet.extend(part)
                
                # Update the chunk in buffer with remainder
                remaining_chunk = chunk[remaining_bytes:]
                self._buffer[0] = remaining_chunk
                remaining_bytes = 0
                self._buffer_size -= len(part)

        return bytes(packet)

    def _flush_buffer(self) -> Optional[bytes]:
        """
        Flush all remaining buffer data as final packet.
        
        Returns:
            Remaining bytes or None if empty
        """
        if self._buffer_size == 0:
            return None

        packet = bytearray()
        while self._buffer:
            chunk = self._buffer.popleft()
            packet.extend(chunk)

        self._buffer_size = 0
        return bytes(packet)

    async def schedule_bytes_stream(
        self,
        audio_bytes: bytes,
        chunk_size: int = 8192
    ) -> AsyncGenerator[bytes, None]:
        """
        Convert static audio bytes to regulated stream.
        Useful for scheduling pre-buffered audio (e.g., cached content).
        
        Args:
            audio_bytes: Complete audio data as bytes
            chunk_size: Size of chunks to emit
            
        Yields:
            Regulated audio packets
        """
        async def chunk_generator() -> AsyncGenerator[bytes, None]:
            """Generate chunks from bytes."""
            for i in range(0, len(audio_bytes), chunk_size):
                yield audio_bytes[i:i + chunk_size]
                await asyncio.sleep(0)  # Yield control

        async for packet in self.schedule_stream(chunk_generator()):
            yield packet

    def get_scheduler_stats(self) -> dict:
        """
        Get current scheduler statistics.
        
        Returns:
            Dictionary with scheduler metrics
        """
        elapsed_seconds = 0
        if self._start_time:
            elapsed_seconds = (datetime.now() - self._start_time).total_seconds()

        return {
            "packet_duration_ms": self.packet_duration_ms,
            "packet_size_bytes": self.packet_size_bytes,
            "sample_rate": self.sample_rate,
            "packets_emitted": self._packets_emitted,
            "total_bytes_processed": self._total_bytes_processed,
            "buffer_size_bytes": self._buffer_size,
            "elapsed_seconds": round(elapsed_seconds, 2),
            "throughput_mbps": round(
                (self._total_bytes_processed * 8) / (elapsed_seconds * 1_000_000)
                if elapsed_seconds > 0 else 0,
                2
            )
        }

    def reset(self) -> None:
        """Reset scheduler state for new stream."""
        self._buffer.clear()
        self._buffer_size = 0
        self._packets_emitted = 0
        self._barge_in_triggered = False
        logger.debug("Scheduler reset")

    def clear_buffer(self) -> None:
        """
        Clear all buffered audio (used for barge-in interruption).
        Immediately discards any pending audio packets.
        """
        self._buffer.clear()
        self._buffer_size = 0
        logger.info("Audio buffer cleared (barge-in)")

    async def trigger_barge_in(self, sarvam_client=None) -> None:
        """
        Trigger barge-in: interrupt current audio playback and synthesis.
        Clears buffer, signals flush to Sarvam, and cancels active streams.
        
        Args:
            sarvam_client: Optional SarvamWebSocketClient to send flush command
        """
        logger.info("🔴 BARGE-IN TRIGGERED - Interrupting audio synthesis")
        
        async with self._stream_lock:
            self._barge_in_triggered = True
            
            # Clear local buffer immediately
            self.clear_buffer()
            
            # Send flush to Sarvam WebSocket to stop server-side synthesis
            if sarvam_client and hasattr(sarvam_client, 'send_flush'):
                success = await sarvam_client.send_flush()
                logger.info(f"Flush sent to Sarvam: {success}")
            
            # Cancel all active streaming tasks
            for task in self._active_streams.copy():
                if not task.done():
                    task.cancel()
                    logger.debug(f"Cancelled stream task {task.get_name()}")
            
            self._active_streams.clear()

    async def reset_barge_in(self) -> None:
        """Reset barge-in state for new synthesis."""
        self._barge_in_triggered = False
        logger.debug("Barge-in state reset")
        self._total_bytes_processed = 0
        self._start_time = None
        logger.debug("Packet Scheduler reset")

    async def flush(self) -> None:
        """Flush remaining buffer and reset state."""
        self.clear_buffer()
        await self.reset_barge_in()
        logger.info("Scheduler flushed")

    async def schedule_and_emit(self, audio_chunk: bytes) -> None:
        """
        Schedule a single audio chunk for emission.
        Buffers and emits as complete packets become available.
        
        Args:
            audio_chunk: Raw audio bytes
        """
        if not audio_chunk or self._barge_in_triggered:
            return

        self._buffer.append(audio_chunk)
        self._buffer_size += len(audio_chunk)
        self._total_bytes_processed += len(audio_chunk)

        # Emit complete packets as they become available
        while self._buffer_size >= self.packet_size_bytes:
            packet = self._extract_packet()
            if packet:
                self._packets_emitted += 1
                # Note: Actual emission handled by caller
