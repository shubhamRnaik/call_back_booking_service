#!/usr/bin/env python3
"""
Unified End-to-End Live Voice Bot Test: Full microphone to speaker loop.
Uses PyAudio for real-time audio I/O with live CLI status indicator.

Usage:
    python test_end_to_end_voice_bot.py
    
    # Press Ctrl+C to exit
"""

import asyncio
import pyaudio
import numpy as np
import logging
import sys
import time
from datetime import datetime
from typing import Optional, AsyncGenerator
from threading import Thread
from queue import Queue

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("VoiceBot")

# Import orchestrator
from indic_tts_runtime.core.full_orchestrator import FullVoiceOrchestrator, VoiceMetrics
from indic_tts_runtime.config import settings


class AudioIOManager:
    """Manages real-time audio input/output using PyAudio."""

    def __init__(
        self,
        input_sample_rate: int = 16000,
        output_sample_rate: int = 8000,
        chunk_size: int = 4096,
    ):
        """
        Initialize audio I/O manager.

        Args:
            input_sample_rate: Microphone sample rate (16kHz for STT)
            output_sample_rate: Speaker sample rate (8kHz for TTS)
            chunk_size: Bytes per audio chunk
        """
        self.input_sample_rate = input_sample_rate
        self.output_sample_rate = output_sample_rate
        self.chunk_size = chunk_size

        # PyAudio instance
        self._pa = pyaudio.PyAudio()
        self._input_stream = None
        self._output_stream = None

        # Audio queues
        self._audio_queue = Queue(maxsize=100)
        self._speaker_queue = Queue(maxsize=100)

        # State
        self._recording = False
        self._is_agent_speaking = False
        self._barge_in_threshold = 500  # Volume threshold to trigger barge-in

        # Statistics
        self._total_input_frames = 0
        self._total_output_frames = 0

        logger.info(f"Audio I/O Manager initialized (Input: {input_sample_rate}Hz, Output: {output_sample_rate}Hz)")

    async def initialize_streams(self) -> bool:
        """
        Initialize input and output audio streams.

        Returns:
            True if successful
        """
        try:
            # Input stream (microphone, 16kHz, mono)
            self._input_stream = self._pa.open(
                format=pyaudio.paFloat32,
                channels=1,
                rate=self.input_sample_rate,
                input=True,
                frames_per_buffer=self.chunk_size,
                stream_callback=None,  # We'll read manually
            )

            # Output stream (speaker, 8kHz, mono)
            self._output_stream = self._pa.open(
                format=pyaudio.paFloat32,
                channels=1,
                rate=self.output_sample_rate,
                output=True,
                frames_per_buffer=self.chunk_size,
                stream_callback=None,
            )

            self._recording = True
            logger.info("✓ Audio streams initialized")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize audio streams: {e}")
            return False

    async def start_input_thread(self) -> None:
        """Start background thread for microphone input."""
        def input_thread_worker():
            try:
                while self._recording:
                    # Read from microphone
                    audio_data = self._input_stream.read(
                        self.chunk_size, exception_on_overflow=False
                    )
                    audio_array = np.frombuffer(audio_data, dtype=np.float32)

                    # Software echo gate: if agent is speaking, check for barge-in
                    if self._is_agent_speaking:
                        volume = np.abs(audio_array).mean()
                        if volume < self._barge_in_threshold:
                            # Mute/ignore input while agent speaking (unless loud barge-in)
                            continue

                    # Queue audio for STT
                    try:
                        self._audio_queue.put(
                            (audio_array * 32767).astype(np.int16).tobytes(),
                            timeout=0.1,
                        )
                    except:
                        pass  # Queue full, skip

            except Exception as e:
                logger.error(f"Input thread error: {e}")
            finally:
                logger.debug("Input thread stopped")

        # Start in background thread
        input_thread = Thread(target=input_thread_worker, daemon=True)
        input_thread.start()
        logger.info("Input thread started")

    async def start_output_thread(self) -> None:
        """Start background thread for speaker output."""
        def output_thread_worker():
            try:
                while self._recording:
                    try:
                        # Get audio from speaker queue
                        audio_data = self._speaker_queue.get(timeout=0.1)
                        if audio_data is None:
                            break

                        # Convert to float32 for PyAudio
                        audio_array = np.frombuffer(audio_data, dtype=np.int16).astype(np.float32) / 32767

                        # Write to speaker
                        self._output_stream.write(audio_array.tobytes())
                        self._total_output_frames += len(audio_array)

                    except:
                        pass

            except Exception as e:
                logger.error(f"Output thread error: {e}")
            finally:
                logger.debug("Output thread stopped")

        # Start in background thread
        output_thread = Thread(target=output_thread_worker, daemon=True)
        output_thread.start()
        logger.info("Output thread started")

    async def get_audio_stream(self) -> AsyncGenerator[bytes, None]:
        """
        Async generator for microphone audio.

        Yields:
            PCM audio chunks (16bit mono @ 16kHz)
        """
        while self._recording:
            try:
                audio_chunk = self._audio_queue.get(timeout=0.1)
                yield audio_chunk
            except:
                await asyncio.sleep(0.01)

    def queue_speaker_audio(self, audio_data: bytes) -> None:
        """Queue audio for speaker output."""
        try:
            self._speaker_queue.put(audio_data, timeout=0.1)
        except:
            pass  # Queue full

    def set_agent_speaking(self, is_speaking: bool) -> None:
        """Update agent speaking state for echo gate."""
        self._is_agent_speaking = is_speaking

    async def cleanup(self) -> None:
        """Cleanup audio streams."""
        self._recording = False
        if self._input_stream:
            self._input_stream.stop_stream()
            self._input_stream.close()
        if self._output_stream:
            self._output_stream.stop_stream()
            self._output_stream.close()
        self._pa.terminate()
        logger.info("Audio streams closed")


class VoiceBotUI:
    """CLI UI for voice bot with real-time status indicator."""

    def __init__(self):
        self.current_status = "INITIALIZING"
        self.current_metrics: Optional[VoiceMetrics] = None
        self.last_update_time = time.time()

    def display_status(self, status: str) -> None:
        """Display current status."""
        self.current_status = status
        self._render()

    def display_metrics(self, metrics: dict) -> None:
        """Display current metrics."""
        self.current_metrics = metrics
        self._render()

    def _render(self) -> None:
        """Render CLI status indicator."""
        # Status emoji mapping
        status_icons = {
            "INITIALIZING": "⏳",
            "READY": "🟢",
            "LISTENING": "👂",
            "USER_SPEAKING": "🗣️ ",
            "THINKING": "🧠",
            "SPEAKING": "🔊",
            "ERROR": "❌",
            "USER_INTERRUPTED": "⏹️ ",
        }

        icon = status_icons.get(self.current_status, "❓")

        # Build status line
        status_line = f"\r[{icon}] [{self.current_status:20s}]"

        if self.current_metrics and isinstance(self.current_metrics, dict):
            current = self.current_metrics.get("current", {})
            if current:
                ttfb = current.get("e2e_ttfb_ms")
                if ttfb:
                    status_line += f" TTFB: {ttfb:.0f}ms"

        sys.stdout.write(status_line)
        sys.stdout.flush()


async def main():
    """Main async entrypoint for voice bot."""
    logger.info("=" * 80)
    logger.info("🎤 INDIC VOICE BOT - End-to-End Test")
    logger.info("=" * 80)

    ui = VoiceBotUI()
    audio_manager = AudioIOManager()
    orchestrator = None

    try:
        # Initialize audio
        ui.display_status("INITIALIZING")
        audio_ok = await audio_manager.initialize_streams()
        if not audio_ok:
            ui.display_status("ERROR")
            return

        # Initialize orchestrator
        logger.info("Initializing Voice Orchestrator...")
        orchestrator = FullVoiceOrchestrator(
            default_language_code=settings.default_language_code,
        )

        # Setup callbacks
        orchestrator.set_status_callback(ui.display_status)
        orchestrator.set_metrics_callback(ui.display_metrics)

        def on_transcript(transcript: str, language_code: str) -> None:
            logger.info(f"📝 [STT] {transcript} ({language_code})")

        def on_response(response: str, language_code: str, metrics: dict) -> None:
            logger.info(f"🤖 [BRAIN] {response}")
            if metrics:
                ui.display_metrics(metrics)

        orchestrator.set_transcript_callback(on_transcript)
        orchestrator.set_response_callback(on_response)

        # Start orchestrator
        orch_ok = await orchestrator.start()
        if not orch_ok:
            ui.display_status("ERROR")
            logger.error("Failed to start orchestrator")
            return

        # Start audio I/O
        await audio_manager.start_input_thread()
        await audio_manager.start_output_thread()

        # Main status display
        ui.display_status("READY")
        logger.info("\n✨ Voice bot ready! Speak into your microphone...")
        logger.info("Press Ctrl+C to exit\n")

        # Process audio stream
        audio_stream = audio_manager.get_audio_stream()

        # Create audio processing task
        audio_task = asyncio.create_task(orchestrator.process_audio_stream(audio_stream))

        # Monitor STT stream
        stt_stream_task = asyncio.create_task(_monitor_stt_stream(orchestrator, audio_manager))

        # Wait for user interrupt
        try:
            while True:
                await asyncio.sleep(0.1)
        except KeyboardInterrupt:
            logger.info("\n\n👋 Shutting down...")

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        ui.display_status("ERROR")

    finally:
        # Cleanup
        if orchestrator:
            await orchestrator.stop()
        await audio_manager.cleanup()
        logger.info("\n✓ Voice bot shutdown complete")


async def _monitor_stt_stream(orchestrator: FullVoiceOrchestrator, audio_manager: AudioIOManager) -> None:
    """Monitor STT stream and handle transcripts."""
    try:
        async for event in orchestrator.stt_client.stream_transcripts():
            if event.event_type == "speech_started":
                logger.debug("User started speaking")
                audio_manager.set_agent_speaking(False)

            elif event.event_type == "speech_ended":
                logger.debug("User stopped speaking")

            elif event.event_type == "final_transcript":
                # Trigger brain response (handled by callback)
                pass

    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Error monitoring STT stream: {e}")


if __name__ == "__main__":
    # Run async main
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
