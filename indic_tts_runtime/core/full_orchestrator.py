"""
Full Voice Orchestrator: Orchestrates end-to-end voice bot with STT ──► LLM ──► Chunker ──► Normalizer ──► TTS ──► Scheduler.
Handles instant interruption, latency tracking, and real-time metrics.
"""

import asyncio
import logging
import time
from typing import Optional, Callable, Dict, Any
from datetime import datetime
from dataclasses import dataclass
import threading

from ..services.stt_service import SarvamSaarasSTTClient, STTEvent
from ..brain.llm_service import StreamingBrain
from ..chunker import StreamTextChunker
from ..normalizer import MultilingualTextNormalizer
from ..services.sarvam_service import SarvamWebSocketClient
from ..core.scheduler import PacketScheduler
from ..config import settings

logger = logging.getLogger(__name__)


@dataclass
class VoiceMetrics:
    """Real-time metrics for voice loop."""
    stt_latency_ms: Optional[float] = None  # Speech End ➔ Transcript
    brain_ttft_ms: Optional[float] = None  # Transcript ➔ First Gemini Token
    brain_total_ms: Optional[float] = None  # Transcript ➔ Last Gemini Token
    tts_latency_ms: Optional[float] = None  # Text ➔ First Audio Packet
    e2e_ttfb_ms: Optional[float] = None  # Speech End ➔ First Audio to Speaker
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class FullVoiceOrchestrator:
    """
    Orchestrates complete end-to-end voice bot loop:
    Mic (16kHz) ──► STT ──► LLM ──► Chunker ──► Normalizer ──► TTS ──► Scheduler ──► Speaker (8kHz)
    """

    def __init__(
        self,
        stt_api_key: Optional[str] = None,
        llm_api_key: Optional[str] = None,
        tts_api_key: Optional[str] = None,
        default_language_code: str = "hi-IN",
    ):
        """
        Initialize voice orchestrator.

        Args:
            stt_api_key: Sarvam STT API key
            llm_api_key: Gemini API key
            tts_api_key: Sarvam TTS API key
            default_language_code: Default language for TTS/Normalizer
        """
        # Initialize services
        self.stt_client = SarvamSaarasSTTClient(api_key=stt_api_key)
        self.brain = StreamingBrain(api_key=llm_api_key)
        self.chunker = StreamTextChunker(
            min_word_threshold=5, max_word_threshold=7, buffer_timeout_sec=0.5
        )
        self.normalizer = MultilingualTextNormalizer(default_language=default_language_code[:2])
        self.tts_client = SarvamWebSocketClient()
        self.scheduler = PacketScheduler()

        # Configuration
        self.default_language_code = default_language_code
        self.stt_sample_rate = 16000
        self.tts_sample_rate = 8000

        # State management
        self._is_running = False
        self._is_agent_speaking = False
        self._user_started_speaking = False
        self._current_turn_id = None
        self._interrupt_event = asyncio.Event()

        # Task management
        self._active_tasks: set = set()

        # Callbacks
        self._on_status_change: Optional[Callable] = None
        self._on_metrics_update: Optional[Callable] = None
        self._on_transcript: Optional[Callable] = None
        self._on_response: Optional[Callable] = None

        # Metrics tracking
        self._metrics_history = []
        self._current_metrics: Dict[str, Any] = {}

        # Transcript guard filter threshold
        self.min_transcript_length = 3  # Ignore < 3 alphanumeric chars

        # STT callbacks
        self.stt_client.set_speech_started_callback(self._on_speech_started)
        self.stt_client.set_speech_ended_callback(self._on_speech_ended)
        self.stt_client.set_transcript_callback(self._on_transcript_received)

        logger.info("Full Voice Orchestrator initialized")

    async def start(self) -> bool:
        """
        Start the voice orchestrator and establish connections.

        Returns:
            True if all services connected successfully
        """
        try:
            logger.info("Starting Voice Orchestrator...")

            # Connect STT
            stt_connected = await self.stt_client.connect()
            if not stt_connected:
                logger.error("Failed to connect STT service")
                return False

            # Connect TTS (with language)
            tts_connected = await self.tts_client.connect(
                target_language_code=self.default_language_code,
                speaker="shubh",
                pace=0.95,
            )
            if not tts_connected:
                logger.error("Failed to connect TTS service")
                return False

            self._is_running = True
            self._update_status("READY")
            logger.info("Voice Orchestrator started successfully")
            return True

        except Exception as e:
            logger.error(f"Error starting orchestrator: {e}")
            self._update_status("ERROR")
            return False

    async def stop(self) -> None:
        """Stop the orchestrator and close all connections."""
        try:
            logger.info("Stopping Voice Orchestrator...")
            self._is_running = False

            # Cancel active tasks
            for task in self._active_tasks:
                if not task.done():
                    task.cancel()

            # Wait for tasks to complete
            if self._active_tasks:
                await asyncio.gather(*self._active_tasks, return_exceptions=True)

            # Disconnect services
            await self.stt_client.disconnect()
            await self.tts_client.disconnect()

            self._update_status("STOPPED")
            logger.info("Voice Orchestrator stopped")

        except Exception as e:
            logger.error(f"Error stopping orchestrator: {e}")

    async def process_audio_stream(
        self, audio_stream_generator
    ) -> None:
        """
        Main loop: Process incoming audio and generate voice responses.

        Args:
            audio_stream_generator: Async generator yielding audio chunks (16kHz PCM)
        """
        try:
            async for audio_chunk in audio_stream_generator:
                if not self._is_running:
                    break

                # Send audio to STT
                await self.stt_client.send_audio_chunk(audio_chunk)

                # Process incoming transcripts in parallel
                await self._process_pending_transcripts()

        except asyncio.CancelledError:
            logger.info("Audio stream processing cancelled")
        except Exception as e:
            logger.error(f"Error in process_audio_stream: {e}")

    async def _process_pending_transcripts(self) -> None:
        """Process any pending transcripts from STT stream."""
        try:
            # Non-blocking check for transcript
            # This is called during audio streaming
            pass
        except Exception as e:
            logger.error(f"Error processing transcripts: {e}")

    async def _on_speech_started(self, event: STTEvent) -> None:
        """Handle speech started event."""
        self._user_started_speaking = True
        self._update_status("USER_SPEAKING")
        logger.debug("User started speaking")

    async def _on_speech_ended(self, event: STTEvent) -> None:
        """Handle speech ended event."""
        self._user_started_speaking = False
        logger.debug("User stopped speaking")

    async def _on_transcript_received(self, event: STTEvent) -> None:
        """
        Handle final transcript received from STT.
        Triggers LLM response generation.
        """
        try:
            transcript = event.transcript.strip()
            language_code = event.language_code or self.default_language_code

            # Guard filter: ignore short/empty transcripts
            if len(transcript) < self.min_transcript_length:
                logger.debug(f"Ignoring short transcript: '{transcript}'")
                return

            logger.info(f"[STT] Transcript: {transcript} (Language: {language_code})")

            if self._on_transcript:
                self._on_transcript(transcript, language_code)

            # Start brain task
            brain_task = asyncio.create_task(
                self._generate_and_stream_response(transcript, language_code)
            )
            self._active_tasks.add(brain_task)
            brain_task.add_done_callback(self._active_tasks.discard)

        except Exception as e:
            logger.error(f"Error handling transcript: {e}")

    async def _generate_and_stream_response(
        self, user_text: str, language_code: str
    ) -> None:
        """
        Generate LLM response and stream it to TTS pipeline.

        Args:
            user_text: User's spoken input
            language_code: Detected language code
        """
        try:
            self._update_status("THINKING")
            self._is_agent_speaking = True

            # Start timing
            brain_start = time.time()
            chunk_count = 0

            # Stream tokens from brain
            token_stream = self.brain.stream_response(user_text)

            # Chunk the token stream
            chunk_stream = self.chunker.chunk_stream(token_stream)

            # Record TTFT
            brain_ttft_start = time.time()
            first_token_received = False

            async for chunk_text in chunk_stream:
                if not self._is_running:
                    break

                # Record first token latency
                if not first_token_received:
                    brain_ttft_ms = (time.time() - brain_ttft_start) * 1000
                    self._current_metrics["brain_ttft_ms"] = brain_ttft_ms
                    logger.debug(f"Brain TTFT: {brain_ttft_ms:.0f}ms")
                    first_token_received = True

                chunk_count += 1

                # Normalize text for TTS
                normalized_text = self.normalizer.normalize(
                    chunk_text, target_language_code=language_code
                )

                logger.debug(f"[Chunk {chunk_count}] {normalized_text[:80]}")

                # Send to TTS
                await self._stream_to_tts(normalized_text, language_code)

                # Check for interruption
                if self._user_started_speaking:
                    logger.info("Interruption detected - cancelling response")
                    await self._handle_interruption()
                    break

            # Record total brain time
            brain_total_ms = (time.time() - brain_start) * 1000
            self._current_metrics["brain_total_ms"] = brain_total_ms
            logger.info(f"Brain response complete ({brain_total_ms:.0f}ms, {chunk_count} chunks)")

            # Update response callback
            if self._on_response:
                self._on_response(
                    self.brain.get_last_response(),
                    language_code,
                    self._current_metrics.copy(),
                )

            self._update_status("LISTENING")
            self._is_agent_speaking = False

        except asyncio.CancelledError:
            logger.info("Brain task cancelled")
            self._is_agent_speaking = False
        except Exception as e:
            logger.error(f"Error generating response: {e}")
            self._update_status("ERROR")
            self._is_agent_speaking = False

    async def _stream_to_tts(self, text: str, language_code: str) -> None:
        """
        Stream normalized text to TTS service.

        Args:
            text: Normalized text
            language_code: Target language code
        """
        try:
            tts_start = time.time()

            # Send to TTS and stream audio
            audio_stream = self.tts_client.synthesize_stream(
                text=text,
                language_code=language_code,
                speaker="shubh",
                pace=0.95,
            )

            # Schedule audio packets
            first_packet_sent = False
            async for audio_chunk in audio_stream:
                if not self._is_running or self._user_started_speaking:
                    break

                if not first_packet_sent:
                    tts_latency_ms = (time.time() - tts_start) * 1000
                    self._current_metrics["tts_latency_ms"] = tts_latency_ms
                    logger.debug(f"TTS Latency: {tts_latency_ms:.0f}ms")
                    first_packet_sent = True
                    self._update_status("SPEAKING")

                # Schedule packet
                await self.scheduler.schedule_and_emit(audio_chunk)

        except Exception as e:
            logger.error(f"Error in TTS streaming: {e}")

    async def _handle_interruption(self) -> None:
        """
        Handle user interruption while agent is speaking.
        Flushes scheduler, cancels TTS, and cleans up tasks.
        """
        try:
            logger.warning("Handling user interruption...")

            # Flush scheduler output queue
            await self.scheduler.flush()

            # Send flush signal to TTS WebSocket
            await self.tts_client.send_flush()

            # Cancel ongoing tasks
            for task in list(self._active_tasks):
                if not task.done():
                    task.cancel()

            self._update_status("USER_INTERRUPTED")
            self._is_agent_speaking = False

        except Exception as e:
            logger.error(f"Error handling interruption: {e}")

    def set_status_callback(self, callback: Callable) -> None:
        """Set callback for status updates."""
        self._on_status_change = callback

    def set_metrics_callback(self, callback: Callable) -> None:
        """Set callback for metrics updates."""
        self._on_metrics_update = callback

    def set_transcript_callback(self, callback: Callable) -> None:
        """Set callback for transcript reception."""
        self._on_transcript = callback

    def set_response_callback(self, callback: Callable) -> None:
        """Set callback for response generation."""
        self._on_response = callback

    def _update_status(self, status: str) -> None:
        """Update and broadcast status."""
        logger.debug(f"Status: {status}")
        if self._on_status_change:
            self._on_status_change(status)

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics."""
        return {
            "current": self._current_metrics,
            "stt": self.stt_client.get_stats(),
            "brain": self.brain.get_stats(),
            "is_agent_speaking": self._is_agent_speaking,
            "is_user_speaking": self._user_started_speaking,
        }

    def get_status(self) -> Dict[str, Any]:
        """Get orchestrator status."""
        return {
            "running": self._is_running,
            "agent_speaking": self._is_agent_speaking,
            "user_speaking": self._user_started_speaking,
            "language": self.default_language_code,
        }
