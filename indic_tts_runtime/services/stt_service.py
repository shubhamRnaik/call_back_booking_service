"""
Sarvam Saaras V3 Streaming STT Service: Production-grade WebSocket-based speech recognition.
Streams 16kHz PCM audio and yields transcripts with language identification (LID).
"""

import asyncio
import base64
import json
import logging
import time
import wave
import io
import websockets
from typing import Optional, AsyncGenerator, Callable, Dict, Any
from datetime import datetime
from dataclasses import dataclass, field

from ..config import settings

logger = logging.getLogger(__name__)


@dataclass
class STTEvent:
    """Represents an event from the STT service."""
    event_type: str  # 'speech_started', 'speech_ended', 'final_transcript', 'error'
    transcript: Optional[str] = None
    language_code: Optional[str] = None
    confidence: Optional[float] = None
    timestamp: datetime = field(default_factory=datetime.now)
    error_message: Optional[str] = None


class SarvamSaarasSTTClient:
    """
    Production-grade WebSocket client for Sarvam Saaras V3 Speech-to-Text engine.
    Handles real-time streaming audio input and yields transcripts with language detection.
    """

    # Configuration constants
    WS_ENDPOINT = "wss://api.sarvam.ai/speech-to-text/ws"
    MODEL = "saaras:v3"
    MODE = "codemix"  # Support code-mixed Hindi/English
    SAMPLE_RATE = 16000  # STT expects 16kHz
    VAD_ENABLED = True

    def __init__(self, api_key: Optional[str] = None, high_vad_sensitivity: Optional[bool] = None):
        """
        Initialize Sarvam STT client.

        Args:
            api_key: Sarvam API key (default: from settings)
            high_vad_sensitivity: Override for Sarvam's VAD sensitivity query
                param. Defaults to settings.stt_high_vad_sensitivity (env-tunable)
                rather than being hardcoded - a too-sensitive VAD chops single
                long utterances into fragments.
        """
        self.api_key = api_key or settings.sarvam_api_key
        self.sample_rate = self.SAMPLE_RATE
        self.high_vad_sensitivity = (
            settings.stt_high_vad_sensitivity
            if high_vad_sensitivity is None
            else high_vad_sensitivity
        )

        # Tracks the timestamp of the last speech_ended signal so the gap to
        # the next speech_started can be logged (makes VAD fragment-splitting
        # visible in logs without guessing - see _parse_stt_message).
        self._last_speech_ended_at: Optional[float] = None

        # Connection state
        self._connected = False
        self._ws = None
        self._ws_connection = None

        # Session management
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 5
        self._reconnect_delay_sec = 1.0

        # Audio buffering
        self._audio_buffer = asyncio.Queue(maxsize=100)
        self._stream_active = False

        # Callbacks
        self._on_speech_started: Optional[Callable] = None
        self._on_speech_ended: Optional[Callable] = None
        self._on_transcript_received: Optional[Callable] = None
        self._on_error: Optional[Callable] = None

        # Statistics
        self._stats = {
            "connected": False,
            "total_audio_chunks_sent": 0,
            "total_transcripts_received": 0,
            "total_bytes_sent": 0,
            "reconnect_count": 0,
            "errors": 0,
            "last_error": None,
            "session_start_time": None,
        }

        logger.info(f"Sarvam STT client initialized with endpoint: {self.WS_ENDPOINT}")

    async def connect(
        self,
        language_code: str = "hi-IN",
        high_vad_sensitivity: Optional[bool] = None,
    ) -> bool:
        """
        Establish WebSocket connection to Sarvam STT service.

        Args:
            language_code: Target BCP-47 language code (default: "hi-IN")
            high_vad_sensitivity: Optional per-call override; falls back to
                self.high_vad_sensitivity (constructor/settings default) when
                not given.

        Returns:
            True if connected successfully, False otherwise
        """
        try:
            effective_high_vad = (
                self.high_vad_sensitivity
                if high_vad_sensitivity is None
                else high_vad_sensitivity
            )
            url = (
                f"{self.WS_ENDPOINT}?"
                f"model={self.MODEL}&"
                f"mode={self.MODE}&"
                f"language_code={language_code}&"
                f"sample_rate={self.SAMPLE_RATE}&"
                f"vad_signals={'true' if self.VAD_ENABLED else 'false'}&"
                f"high_vad_sensitivity={'true' if effective_high_vad else 'false'}"
            )

            logger.info(f"Connecting to Sarvam STT: {url}")

            # Create WebSocket connection with API key in header
            # Use additional_headers parameter for websockets library
            headers = [("api-subscription-key", self.api_key)]
            self._ws = await websockets.connect(
                url, 
                additional_headers=headers,
                ping_interval=30,
                close_timeout=10
            )

            self._connected = True
            self._reconnect_attempts = 0
            self._stats["connected"] = True
            self._stats["session_start_time"] = datetime.now()

            logger.info("Successfully connected to Sarvam STT service")
            return True

        except Exception as e:
            self._connected = False
            self._stats["connected"] = False
            self._stats["errors"] += 1
            self._stats["last_error"] = str(e)
            logger.error(f"Failed to connect to Sarvam STT: {e}")
            return False

    async def disconnect(self) -> None:
        """Close WebSocket connection gracefully."""
        try:
            if self._ws:
                await self._ws.close()
            self._connected = False
            self._stats["connected"] = False
            logger.info("Disconnected from Sarvam STT service")
        except Exception as e:
            logger.error(f"Error disconnecting: {e}")

    def _wrap_pcm_as_wav(self, audio_chunk: bytes) -> bytes:
        """Wrap PCM bytes into a minimal WAV container for Sarvam STT."""
        wav_buffer = io.BytesIO()
        with wave.open(wav_buffer, "wb") as wav_file:
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)
            wav_file.setframerate(self.SAMPLE_RATE)
            wav_file.writeframes(audio_chunk)
        return wav_buffer.getvalue()

    async def send_audio_chunk(self, audio_chunk: bytes) -> bool:
        """
        Send audio chunk to STT service.

        Args:
            audio_chunk: PCM audio data (16-bit, 16kHz, mono)

        Returns:
            True if sent successfully, False otherwise
        """
        if not self._connected or not self._ws:
            logger.warning("Cannot send audio: not connected")
            return False

        try:
            wav_payload = self._wrap_pcm_as_wav(audio_chunk)
            audio_b64 = base64.b64encode(wav_payload).decode("utf-8")
            message = {
                "audio": {
                    "data": audio_b64,
                    "sample_rate": str(self.SAMPLE_RATE),
                    "encoding": "audio/wav",
                }
            }

            await self._ws.send(json.dumps(message))
            self._stats["total_audio_chunks_sent"] += 1
            self._stats["total_bytes_sent"] += len(audio_chunk)

            return True

        except Exception as e:
            logger.error(f"Error sending audio chunk: {e}")
            self._stats["errors"] += 1
            self._stats["last_error"] = str(e)
            return False

    async def signal_end_of_stream(self) -> bool:
        """
        Signal end of audio stream to STT service.

        Returns:
            True if signal sent successfully, False otherwise
        """
        if not self._connected or not self._ws:
            return False

        try:
            message = {"audio": {"data": "", "sample_rate": str(self.SAMPLE_RATE), "encoding": "audio/wav"}}
            await self._ws.send(json.dumps(message))
            logger.info("Sent end-of-stream signal to STT service")
            return True

        except Exception as e:
            logger.error(f"Error sending end-of-stream signal: {e}")
            self._stats["errors"] += 1
            return False

    async def stream_transcripts(self) -> AsyncGenerator[STTEvent, None]:
        """
        Listen to WebSocket and yield transcripts as they arrive.

        Yields:
            STTEvent objects containing transcript, language, and metadata
        """
        if not self._connected or not self._ws:
            logger.error("Cannot stream transcripts: not connected")
            return

        try:
            async for message_str in self._ws:
                try:
                    message = json.loads(message_str)
                    event = self._parse_stt_message(message)

                    if event:
                        # Fire callbacks
                        if event.event_type == "speech_started" and self._on_speech_started:
                            await self._invoke_callback(self._on_speech_started, event)
                        elif event.event_type == "speech_ended" and self._on_speech_ended:
                            await self._invoke_callback(self._on_speech_ended, event)
                        elif event.event_type == "final_transcript" and self._on_transcript_received:
                            await self._invoke_callback(self._on_transcript_received, event)

                        yield event
                        self._stats["total_transcripts_received"] += 1

                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON received: {e}")
                    continue

        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"WebSocket connection closed: {e}")
            error_event = STTEvent(
                event_type="error",
                error_message=f"WebSocket closed: {e}",
            )
            yield error_event

        except Exception as e:
            logger.error(f"Error in stream_transcripts: {e}")
            error_event = STTEvent(
                event_type="error",
                error_message=str(e),
            )
            yield error_event

    def _parse_stt_message(self, message: Dict[str, Any]) -> Optional[STTEvent]:
        """
        Parse WebSocket message from Sarvam STT service.

        Args:
            message: JSON message from STT service

        Returns:
            STTEvent object or None if unparseable
        """
        try:
            if not isinstance(message, dict):
                logger.debug(f"STT payload is not a dict: {message}")
                return None

            logger.debug(f"STT raw payload: {message}")

            msg_type = message.get("type")
            data = message.get("data", {}) or {}

            # 1. Parse VAD Events
            if msg_type == "events":
                signal = data.get("signal_type")
                if signal == "START_SPEECH":
                    if self._last_speech_ended_at is not None:
                        gap_ms = (time.monotonic() - self._last_speech_ended_at) * 1000
                        logger.info(
                            "🎙️ Sarvam STT: fragment gap %.0fms since last speech_ended "
                            "(small gaps may indicate VAD splitting one utterance)",
                            gap_ms,
                        )
                    logger.info("🎙️ Sarvam STT: Speech started signal detected")
                    return STTEvent(event_type="speech_started")
                if signal == "END_SPEECH":
                    self._last_speech_ended_at = time.monotonic()
                    logger.info("🎙️ Sarvam STT: Speech ended signal detected")
                    return STTEvent(event_type="speech_ended")
                return None

            # 2. Parse Transcripts from event structure
            if msg_type == "data":
                transcript = data.get("transcript")
                if transcript and transcript.strip():
                    logger.info(f"🎙️ Sarvam STT parsed transcript: '{transcript.strip()}'")
                    return STTEvent(
                        event_type="final_transcript",
                        transcript=transcript.strip(),
                        language_code=data.get("language_code") or data.get("language"),
                        confidence=data.get("confidence", 0.0),
                    )
                return None

            # 3. Check direct legacy/fallback VAD keys
            if message.get("speech_started") or message.get("signal_type") == "START_SPEECH":
                return STTEvent(event_type="speech_started")

            if message.get("speech_ended") or message.get("signal_type") == "END_SPEECH":
                return STTEvent(event_type="speech_ended")

            # 4. Fallback transcript extraction
            transcript = self._extract_text_from_payload(message, keys=["transcript", "text", "message", "result"])
            if transcript:
                language = message.get("language") or message.get("lang") or "hi-IN"
                confidence = message.get("confidence", 0.0)
                logger.info(f"🎙️ Sarvam STT parsed transcript (fallback): '{transcript.strip()}'")
                return STTEvent(
                    event_type="final_transcript",
                    transcript=transcript.strip(),
                    language_code=language,
                    confidence=confidence,
                )

            return None

        except Exception as e:
            logger.error(f"Error parsing STT message: {e}")
            return None

    def _extract_text_from_payload(self, payload: Any, keys: list[str]) -> Optional[str]:
        """Recursively extract a text transcript from nested payload structures."""
        if isinstance(payload, str):
            return payload.strip() or None

        if isinstance(payload, dict):
            for key in keys:
                if key in payload:
                    value = payload[key]
                    if isinstance(value, str) and value.strip():
                        return value.strip()
                    if isinstance(value, (dict, list)):
                        nested_value = self._extract_text_from_payload(value, keys)
                        if nested_value:
                            return nested_value

            for value in payload.values():
                if isinstance(value, (dict, list)):
                    nested_value = self._extract_text_from_payload(value, keys)
                    if nested_value:
                        return nested_value

        elif isinstance(payload, list):
            for item in payload:
                nested_value = self._extract_text_from_payload(item, keys)
                if nested_value:
                    return nested_value

        return None

    async def _invoke_callback(self, callback: Callable, event: STTEvent) -> None:
        """Invoke callback safely (supports both sync and async)."""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(event)
            else:
                callback(event)
        except Exception as e:
            logger.error(f"Error invoking callback: {e}")

    def set_speech_started_callback(self, callback: Callable) -> None:
        """Set callback for when user starts speaking."""
        self._on_speech_started = callback

    def set_speech_ended_callback(self, callback: Callable) -> None:
        """Set callback for when user stops speaking."""
        self._on_speech_ended = callback

    def set_transcript_callback(self, callback: Callable) -> None:
        """Set callback for when final transcript received."""
        self._on_transcript_received = callback

    def set_error_callback(self, callback: Callable) -> None:
        """Set callback for errors."""
        self._on_error = callback

    def get_stats(self) -> Dict[str, Any]:
        """Get connection and streaming statistics."""
        return {
            **self._stats,
            "session_duration_sec": (
                (datetime.now() - self._stats["session_start_time"]).total_seconds()
                if self._stats["session_start_time"]
                else None
            ),
        }

    async def health_check(self) -> bool:
        """Check if connection is healthy."""
        return self._connected and self._ws is not None
