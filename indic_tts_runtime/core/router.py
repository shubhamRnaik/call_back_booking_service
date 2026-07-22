"""
Voice Router: Intelligent routing logic for TTS synthesis requests.
Implements sequential routing: Cache -> Primary (Sarvam) -> Fallback
"""

import io
import logging
from typing import Optional, Tuple, AsyncGenerator
from enum import Enum

from ..services.cache_service import CacheService
from ..services.sarvam_service import SarvamWebSocketClient
from ..schemas import LanguageCode, SpeakerProfile

logger = logging.getLogger(__name__)


class RoutingStrategy(str, Enum):
    """Available routing strategies."""
    CACHE_FIRST = "cache_first"
    PRIMARY_ONLY = "primary_only"
    FALLBACK_ENABLED = "fallback_enabled"


class VoiceRouter:
    """
    Intelligent voice routing orchestrator.
    Routes TTS requests through cache, primary, and fallback engines
    based on availability and configuration.
    """

    def __init__(
        self,
        cache_service: CacheService,
        sarvam_service: SarvamWebSocketClient,
        strategy: RoutingStrategy = RoutingStrategy.CACHE_FIRST
    ) -> None:
        """
        Initialize Voice Router with engine instances.
        
        Args:
            cache_service: Cache lookup service instance
            sarvam_service: Sarvam AI synthesis service instance
            strategy: Routing strategy to apply
        """
        self.cache_service = cache_service
        self.sarvam_service = sarvam_service
        self.strategy = strategy

    async def route_and_synthesize(
        self,
        text: str,
        language: LanguageCode = LanguageCode.HINDI,
        speaker: SpeakerProfile = SpeakerProfile.SHUBH,
        pace: float = 0.95
    ) -> Tuple[io.BytesIO, dict]:
        """
        Route synthesis request through cache and primary engine.
        Implements fallback error handling with explicit reporting.
        
        Args:
            text: Text to synthesize
            language: Target language
            speaker: Voice profile
            pace: Speech pace multiplier
            
        Returns:
            Tuple of (audio_stream, metadata)
            
        Raises:
            RuntimeError: If all routing paths fail
        """
        routing_log = {
            "text": text[:50],
            "language": language.value,
            "speaker": speaker.value,
            "attempted_sources": [],
            "success": False,
            "final_source": None
        }

        # Step 1: Try Cache
        if self.strategy in [RoutingStrategy.CACHE_FIRST, RoutingStrategy.FALLBACK_ENABLED]:
            try:
                logger.info(f"Router: Attempting cache lookup for: {text[:50]}...")
                cache_result = self.cache_service.lookup_phrase(text)
                
                if cache_result:
                    audio_stream, metadata = cache_result
                    routing_log["attempted_sources"].append("cache")
                    routing_log["success"] = True
                    routing_log["final_source"] = "cache"
                    
                    logger.info(f"Router: Cache HIT - serving from cache")
                    return audio_stream, metadata
                else:
                    routing_log["attempted_sources"].append("cache_miss")
                    logger.debug(f"Router: Cache MISS - proceeding to primary")
                    
            except Exception as e:
                logger.warning(f"Router: Cache lookup error: {e}")
                routing_log["attempted_sources"].append(f"cache_error:{str(e)}")

        # Step 2: Try Primary Engine (Sarvam)
        try:
            logger.info(f"Router: Attempting Sarvam synthesis for: {text[:50]}...")
            audio_stream, metadata = await self.sarvam_service.synthesize_full(
                text=text,
                language=language,
                speaker=speaker,
                pace=pace
            )
            
            routing_log["attempted_sources"].append("sarvam")
            routing_log["success"] = True
            routing_log["final_source"] = "sarvam"
            
            logger.info(f"Router: Sarvam synthesis SUCCESS")
            return audio_stream, metadata

        except Exception as e:
            routing_log["attempted_sources"].append(f"sarvam_error:{str(e)}")
            logger.warning(f"Router: Sarvam synthesis failed: {e}")

            # Step 3: Fallback Handler
            if self.strategy == RoutingStrategy.FALLBACK_ENABLED:
                try:
                    logger.warning(
                        f"Router: Primary engine failed, attempting fallback for: {text[:50]}..."
                    )
                    audio_stream = self._generate_fallback_audio(text)
                    metadata = {
                        "source": "fallback",
                        "warning": "Primary engine unavailable, serving fallback audio"
                    }
                    
                    routing_log["attempted_sources"].append("fallback")
                    routing_log["success"] = True
                    routing_log["final_source"] = "fallback"
                    
                    logger.info(f"Router: Fallback audio generated")
                    return audio_stream, metadata

                except Exception as fallback_error:
                    routing_log["attempted_sources"].append(f"fallback_error:{str(fallback_error)}")
                    logger.error(f"Router: Fallback generation failed: {fallback_error}")

        # All routing paths exhausted
        error_msg = (
            f"All routing paths exhausted. "
            f"Attempted: {', '.join(routing_log['attempted_sources'])}"
        )
        logger.error(f"Router: {error_msg}")
        raise RuntimeError(error_msg)

    async def route_and_stream(
        self,
        text: str,
        language: LanguageCode = LanguageCode.HINDI,
        speaker: SpeakerProfile = SpeakerProfile.SHUBH,
        pace: float = 0.95
    ) -> AsyncGenerator[bytes, None]:
        """
        Route and stream audio synthesis (streaming mode).
        Useful for large texts or real-time applications.
        
        Args:
            text: Text to synthesize
            language: Target language
            speaker: Voice profile
            pace: Speech pace multiplier
            
        Yields:
            Audio chunks as they arrive
        """
        # Try cache first (buffered)
        if self.strategy in [RoutingStrategy.CACHE_FIRST, RoutingStrategy.FALLBACK_ENABLED]:
            try:
                cache_result = self.cache_service.lookup_phrase(text)
                if cache_result:
                    audio_stream, _ = cache_result
                    # Yield cached data in chunks
                    while True:
                        chunk = audio_stream.read(8192)
                        if not chunk:
                            break
                        yield chunk
                    return
            except Exception as e:
                logger.warning(f"Router: Cache streaming failed: {e}")

        # Stream from primary engine
        try:
            async for chunk in self.sarvam_service.synthesize_stream(
                text=text,
                language=language,
                speaker=speaker,
                pace=pace
            ):
                yield chunk
        except Exception as e:
            logger.error(f"Router: Primary streaming failed: {e}")
            # Yield fallback silence or error indicator
            if self.strategy == RoutingStrategy.FALLBACK_ENABLED:
                fallback_audio = self._generate_fallback_audio(text)
                while True:
                    chunk = fallback_audio.read(8192)
                    if not chunk:
                        break
                    yield chunk

    def _generate_fallback_audio(self, text: str) -> io.BytesIO:
        """
        Generate fallback audio when primary engines unavailable.
        Provides graceful degradation with simple silence or click track.
        
        Args:
            text: Text (used for logging, not synthesis)
            
        Returns:
            BytesIO stream with fallback audio
        """
        import struct
        import wave

        logger.info(f"Generating fallback audio for: {text[:50]}...")
        
        # Create simple silent audio (3 seconds)
        sample_rate = 8000
        duration_seconds = 3
        num_samples = sample_rate * duration_seconds
        
        audio_data = bytearray()
        for _ in range(num_samples):
            # Silent PCM (zero samples)
            audio_data.extend(struct.pack('<h', 0))

        # Create WAV file
        wav_stream = io.BytesIO()
        with wave.open(wav_stream, 'wb') as wav_file:
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)
            wav_file.setframerate(sample_rate)
            wav_file.writeframes(bytes(audio_data))

        wav_stream.seek(0)
        return wav_stream

    def get_routing_config(self) -> dict:
        """
        Get current routing configuration.
        
        Returns:
            Dictionary with router configuration
        """
        return {
            "routing_strategy": self.strategy.value,
            "cache_enabled": self.cache_service.cache_enabled,
            "primary_engine": "sarvam_bulbul_v3",
            "fallback_available": True,
            "cache_stats": self.cache_service.get_cache_stats(),
            "sarvam_stats": self.sarvam_service.get_connection_stats()
        }
