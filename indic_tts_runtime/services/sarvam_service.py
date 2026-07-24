"""
Sarvam AI WebSocket Service: Production-grade streaming client for Bulbul V3 TTS engine.
Uses official sarvamai library for robust WebSocket connection management.
"""

import asyncio
import base64
import json
import logging
import time
from typing import Optional, AsyncGenerator, Callable
from datetime import datetime
from collections import deque

from sarvamai import AsyncSarvamAI, AudioOutput, EventResponse

from ..config import settings

logger = logging.getLogger(__name__)


class SarvamWebSocketClient:
    """
    Production-grade WebSocket client for Sarvam AI's Bulbul V3 TTS engine.
    Uses official sarvamai library for reliable authentication and connection management.
    """

    # Configuration constants
    MAX_CONCURRENT_SESSIONS = 50
    DEFAULT_LANGUAGE = "hi-IN"
    DEFAULT_SPEAKER = "shubh"
    DEFAULT_PACE = 0.95
    MODEL = "bulbul:v3"
    DEFAULT_STREAM_IDLE_TIMEOUT_SEC = 4.0
    DEFAULT_STREAM_MAX_DURATION_SEC = 25.0

    def __init__(self):
        """Initialize Sarvam WebSocket client with configuration."""
        self.api_key = settings.sarvam_api_key
        self.sample_rate = settings.default_sample_rate
        self.audio_codec = settings.default_audio_codec
        
        # Connection state
        self._connected = False
        self._ws = None  # Context manager
        self._ws_connection = None  # Actual WebSocket connection
        self._client: Optional[AsyncSarvamAI] = None
        
        # Session management
        self._active_sessions = 0
        self._session_semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_SESSIONS)
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 5
        self._reconnect_delay_sec = 1.0
        self._connection_created_at: Optional[datetime] = None
        
        # Audio buffer
        self._audio_buffer: deque = deque(maxlen=10000)
        self._buffer_lock = asyncio.Lock()
        self._audio_stream_lock = asyncio.Lock()
        
        # Connection callbacks
        self._on_connected: Optional[Callable] = None
        self._on_disconnected: Optional[Callable] = None
        
        # Statistics
        self._stats = {
            "total_text_chunks_sent": 0,
            "total_audio_chunks_received": 0,
            "total_bytes_received": 0,
            "reconnect_count": 0,
            "errors": 0,
            "connected": False,
            "active_sessions": 0,
            "last_error": None,
        }

    async def connect(
        self,
        target_language_code: str = "hi-IN",
        speaker: str = "shubh",
        pace: float = 0.95
    ) -> bool:
        """
        Establish WebSocket connection to Sarvam AI using official library.
        
        Args:
            target_language_code: Target language (e.g., "hi-IN", "ta-IN")
            speaker: Voice profile (e.g., "shubh", "meera")
            pace: Speech pace (0.5-2.0, default 0.95)
            
        Returns:
            True if connected successfully, False otherwise
        """
        try:
            # Check session concurrency limit
            if self._active_sessions >= self.MAX_CONCURRENT_SESSIONS:
                logger.warning(
                    f"Cannot connect: Max concurrent sessions ({self.MAX_CONCURRENT_SESSIONS}) reached"
                )
                return False

            await self._session_semaphore.acquire()
            self._active_sessions += 1

            logger.info(f"Connecting to Sarvam AI (using sarvamai library)... (session {self._active_sessions})")
            
            # Create Sarvam client using official library
            self._client = AsyncSarvamAI(api_subscription_key=self.api_key)
            
            # Get the context manager for WebSocket connection with completion events enabled
            # ✅ send_completion_event=True tells Sarvam to signal when synthesis is done for current flush
            self._ws = self._client.text_to_speech_streaming.connect(
                model=self.MODEL,
                send_completion_event=True
            )
            
            # Enter the context manager to get the actual WebSocket
            ws_connection = await self._ws.__aenter__()
            
            # Configure the connection with all parameters
            config_params = {
                "target_language_code": target_language_code,
                "speaker": speaker,
                "pace": pace,
                "output_audio_codec": self.audio_codec,
                "min_buffer_size": 50,
                "max_chunk_length": 200,
            }
            
            # Add bitrate for MP3 codec if applicable
            if self.audio_codec != "linear16":
                config_params["output_audio_bitrate"] = "128k"
            
            logger.debug(f"Sarvam config: {config_params}")
            await ws_connection.configure(**config_params)
            
            self._connected = True
            self._connection_created_at = datetime.now()
            self._reconnect_attempts = 0
            
            # Store the actual connection for use in other methods
            self._ws_connection = ws_connection
            
            logger.info(f"✓ Connected to Sarvam AI TTS Streaming")
            logger.info(f"  Language: {target_language_code}, Speaker: {speaker}, Pace: {pace}x, Codec: {self.audio_codec}")
            
            if self._on_connected:
                await self._on_connected()
            
            return True
        
        except Exception as e:
            self._connected = False
            self._stats["errors"] += 1
            self._stats["last_error"] = str(e)
            logger.error(f"Failed to connect to Sarvam: {e}")
            
            # Cleanup on failure
            await self._cleanup_session()
            
            return False

    async def ensure_connected(
        self,
        target_language_code: str = "hi-IN",
        speaker: str = "shubh",
        pace: float = 0.95
    ) -> bool:
        """Verify connection health and transparently reconnect if idle-dropped."""
        if self._connected and self._ws_connection is not None:
            return True
        logger.info("🔄 TTS WebSocket idle or disconnected. Reconnecting...")
        return await self.connect(
            target_language_code=target_language_code,
            speaker=speaker,
            pace=pace
        )

    async def send_text_chunk(self, text: str) -> bool:
        """
        Send text chunk to Sarvam for synthesis.
        
        Args:
            text: Normalized text to synthesize
            
        Returns:
            True if sent successfully, False otherwise
        """
        if not self._connected or not self._ws_connection:
            logger.error("Not connected to Sarvam")
            return False
        
        try:
            await self._ws_connection.convert(text)
            self._stats["total_text_chunks_sent"] += 1
            logger.debug(f"Text sent: {text[:50]}...")
            return True
        
        except Exception as e:
            logger.error(f"Failed to send text: {e}")
            self._stats["errors"] += 1
            return False

    async def send_flush(self) -> bool:
        """
        Flush pending synthesis (force completion of current utterance).
        Used for barge-in interruption.
        
        Returns:
            True if flushed successfully
        """
        if not self._connected or not self._ws_connection:
            return False
        
        try:
            await self._ws_connection.flush()
            logger.info("Flush sent to Sarvam")
            return True
        except Exception as e:
            logger.error(f"Failed to flush: {e}")
            return False

    async def stream_audio_chunks(
        self,
        initial_timeout_sec: float = 2.0,
        post_audio_idle_timeout_sec: float = 0.3,
        max_duration_sec: float = 12.0,
        idle_timeout_sec: Optional[float] = None,  # Backward compatibility
    ) -> AsyncGenerator[bytes, None]:
        """
        Yields audio chunks for current clause/utterance from Sarvam.
        Breaks on Sarvam 'completion' event OR fast 300ms post-audio silence.
        """
        if not self._connected or not self._ws_connection:
            logger.debug("Not connected to Sarvam - skipping audio stream")
            return

        if idle_timeout_sec is not None:
            initial_timeout_sec = idle_timeout_sec

        try:
            async with self._audio_stream_lock:
                if self._ws_connection is None:
                    logger.debug("Connection closed during audio stream init")
                    return

                stream_started_at = time.perf_counter()
                iterator = self._ws_connection.__aiter__()
                audio_received = False
                
                # Pattern detection for buffer cycling (looped audio detection)
                chunk_sizes = []
                repeating_pair_hits = 0
                chunks_yielded = 0
                recent_hashes = deque(maxlen=8)
                identical_hash_hits = 0

                LARGE_CHUNK_THRESHOLD_BYTES = 8000
                large_chunk_seen = False
                large_chunk_hash = None
                startup_hashes = []
                startup_phase = True
                MAX_STARTUP_CHUNKS = 6
                signature_match_window = deque(maxlen=6)

                while True:
                    elapsed_sec = time.perf_counter() - stream_started_at
                    if elapsed_sec > max_duration_sec:
                        logger.warning(
                            "Audio stream hard timeout after %.2fs; stopping clause stream",
                            elapsed_sec,
                        )
                        break

                    current_timeout = post_audio_idle_timeout_sec if audio_received else initial_timeout_sec

                    try:
                        message = await asyncio.wait_for(
                            anext(iterator), timeout=current_timeout
                        )
                    except StopAsyncIteration:
                        logger.debug("Audio stream ended normally")
                        break
                    except asyncio.TimeoutError:
                        if audio_received:
                            logger.debug("✓ Clause audio finished (300ms post-audio silence detected)")
                        else:
                            logger.warning(
                                "⚠️ Clause initial audio timeout (no audio received in %.2fs)",
                                initial_timeout_sec,
                            )
                        break

                    # Audio frame received
                    if isinstance(message, AudioOutput):
                        audio_bytes = base64.b64decode(message.data.audio)
                        if not audio_bytes:
                            continue

                        audio_received = True
                        chunk_size = len(audio_bytes)
                        chunk_hash = hash(audio_bytes[:64])

                        # Build startup signature (first 6 chunk hashes = unique fingerprint of this utterance start)
                        if startup_phase and len(startup_hashes) < MAX_STARTUP_CHUNKS:
                            startup_hashes.append(chunk_hash)
                            if len(startup_hashes) == MAX_STARTUP_CHUNKS:
                                startup_phase = False
                                logger.debug(f"[Replay Guard v4] Startup signature captured after {MAX_STARTUP_CHUNKS} chunks")

                        # Guard 0a: full-utterance replay detection via lead-in chunk.
                        # A large (>=8000 byte) chunk is normal for BOTH the very
                        # first lead-in chunk of an utterance AND a legitimate
                        # end-of-stream flush chunk later on - size alone is not
                        # a reliable replay signal. Only treat it as a replay
                        # when a second large chunk arrives with the SAME
                        # content hash as a previously-seen large chunk (i.e.
                        # the buffer actually looped back and is resending the
                        # same bytes), matching the content-hash pattern Guard
                        # 0b already uses.
                        if chunk_size >= LARGE_CHUNK_THRESHOLD_BYTES:
                            if large_chunk_seen and chunk_hash == large_chunk_hash:
                                logger.info(
                                    "✓ Detected utterance restart (lead-in chunk content repeated "
                                    "after %d chunks, size=%d) - stopping stream before replay",
                                    chunks_yielded,
                                    chunk_size,
                                )
                                break
                            large_chunk_seen = True
                            large_chunk_hash = chunk_hash
                        
                        # Guard 0b: detect replay via signature match (content fingerprint).
                        # After startup phase, maintain a 6-chunk rolling window. If it matches
                        # the startup signature, the backend has restarted the utterance.
                        # This is CRITICAL for catching mid-utterance replays (where replay
                        # doesn't start with lead-in, so Guard 0a won't catch it until later).
                        # Fire on FIRST signature match after 18+ chunks to stop replay immediately.
                        if not startup_phase and chunks_yielded >= (MAX_STARTUP_CHUNKS * 3):
                            signature_match_window.append(chunk_hash)
                            if len(signature_match_window) == MAX_STARTUP_CHUNKS:
                                rolling_sig = tuple(signature_match_window)
                                startup_sig = tuple(startup_hashes)
                                if rolling_sig == startup_sig:
                                    logger.info(
                                        "✓ Detected utterance replay via signature match "
                                        "(content fingerprint repeated after %d chunks) "
                                        "- stopping before mid-utterance replay",
                                        chunks_yielded,
                                    )
                                    break
                        
                        self._audio_buffer.append(audio_bytes)
                        self._stats["total_audio_chunks_received"] += 1
                        self._stats["total_bytes_received"] += chunk_size
                        chunks_yielded += 1
                        yield audio_bytes

                        # Guard 1: exact same leading payload repeated too many times.
                        if recent_hashes and chunk_hash == recent_hashes[-1]:
                            identical_hash_hits += 1
                        else:
                            identical_hash_hits = 0
                        recent_hashes.append(chunk_hash)


                        if chunks_yielded > 20 and identical_hash_hits >= 6:
                            logger.info(
                                "✓ Detected repeated identical audio chunks; stopping stream"
                            )
                            break
                        
                        # ✅ Track last 4 chunk sizes to detect repeating pattern
                        chunk_sizes.append(chunk_size)
                        if len(chunk_sizes) > 4:
                            chunk_sizes.pop(0)
                        
                        # Detect repeated 2-chunk cycle only after enough audio has been streamed.
                        # We require multiple consecutive hits to avoid cutting valid speech early.
                        if len(chunk_sizes) == 4:
                            is_repeating_pair = (
                                chunk_sizes[0] == chunk_sizes[2]
                                and chunk_sizes[1] == chunk_sizes[3]
                            )
                            if is_repeating_pair:
                                repeating_pair_hits += 1
                            else:
                                repeating_pair_hits = 0

                            # Require >20 chunks and 3 consecutive pair matches before cutting.
                            if chunks_yielded > 20 and repeating_pair_hits >= 3:
                                logger.info(
                                    "✓ Detected sustained repeating buffer pattern %s - stopping audio stream (looped data)",
                                    chunk_sizes[:2],
                                )
                                break

                        # Guard 2: alternating chunk hash cycle ABABAB indicates buffer loop.
                        if len(recent_hashes) >= 6:
                            h = list(recent_hashes)
                            is_hash_pair_cycle = (
                                h[-1] == h[-3] == h[-5]
                                and h[-2] == h[-4] == h[-6]
                                and h[-1] != h[-2]
                            )
                            if chunks_yielded > 20 and is_hash_pair_cycle:
                                logger.info(
                                    "✓ Detected alternating audio hash cycle; stopping stream"
                                )
                                break
                    
                    # Try completion event as well (fallback for future Sarvam versions)
                    elif isinstance(message, EventResponse) or getattr(message, "event_type", None) == "completion":
                        logger.debug("✓ Received Sarvam completion event - stopping audio stream")
                        break
        
        except asyncio.CancelledError:
            logger.debug("Audio stream cancelled")
        except StopAsyncIteration:
            logger.debug("Audio stream ended normally")
        except Exception as e:
            logger.debug(f"Audio stream ended: {e}")
            self._stats["errors"] += 1
            self._stats["last_error"] = str(e)

    async def synthesize_stream(
        self,
        text: str,
        language_code: str = "hi-IN",
        speaker: str = "shubh",
        pace: float = 0.95,
    ) -> AsyncGenerator[bytes, None]:
        """
        Synthesize text and stream audio chunks.
        Convenience method combining send_text_chunk and stream_audio_chunks.
        
        Args:
            text: Text to synthesize
            language_code: Target language code
            speaker: Voice profile
            pace: Speech pace
            
        Yields:
            PCM audio bytes
        """
        try:
            # Send text to TTS
            sent = await self.send_text_chunk(text)
            if not sent:
                logger.error(f"Failed to send text for synthesis: {text}")
                return

            # Stream audio chunks back
            async for audio_chunk in self.stream_audio_chunks():
                yield audio_chunk

        except Exception as e:
            logger.error(f"Error in synthesize_stream: {e}")
            self._stats["errors"] += 1
            raise

    async def disconnect(self) -> None:
        """Gracefully close WebSocket connection."""
        try:
            if self._ws and self._ws_connection:
                # Exit the context manager
                await self._ws.__aexit__(None, None, None)
                self._ws = None
                self._ws_connection = None
            
            if self._client:
                self._client = None
            
            self._connected = False
            logger.info("Disconnected from Sarvam")
            
            if self._on_disconnected:
                await self._on_disconnected()
        
        except Exception as e:
            logger.error(f"Error during disconnect: {e}")
        
        finally:
            await self._cleanup_session()

    async def _cleanup_session(self) -> None:
        """Cleanup session resources and release semaphore."""
        try:
            self._active_sessions = max(0, self._active_sessions - 1)
            self._session_semaphore.release()
            self._stats["active_sessions"] = self._active_sessions
        except ValueError:
            logger.warning("Semaphore release without acquire")

    def health_check(self) -> bool:
        """
        Check if WebSocket connection is healthy.
        
        Returns:
            True if connected, False otherwise
        """
        return self._connected

    def get_connection_stats(self) -> dict:
        """
        Get connection and performance statistics.
        
        Returns:
            Dictionary of statistics
        """
        self._stats["connected"] = self._connected
        self._stats["active_sessions"] = self._active_sessions
        self._stats["uptime_seconds"] = (
            (datetime.now() - self._connection_created_at).total_seconds()
            if self._connection_created_at else 0
        )
        return self._stats.copy()


# For backward compatibility, alias to commonly used name
SarvamService = SarvamWebSocketClient
