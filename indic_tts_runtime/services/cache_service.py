"""
Cache Service: Fast disk lookup for pre-rendered static phrases.
Provides O(1) lookup for common Indian phrases with instant audio delivery.
"""

import os
import io
import wave
import struct
import logging
from typing import Optional, Tuple
from pathlib import Path

from ..config import settings

logger = logging.getLogger(__name__)


class CacheService:
    """
    Fast cache lookup engine for static TTS phrases.
    Maintains in-memory dictionary of common phrases and disk-based WAV files.
    """

    def __init__(self) -> None:
        """Initialize cache service with mock phrase mappings and cache directory."""
        self.cache_enabled: bool = settings.cache_enabled
        self.cache_dir: str = settings.cache_directory_path
        self.sample_rate: int = settings.default_sample_rate
        
        # Mock dictionary: text -> cached audio filename
        # In production, this would be populated from a database or configuration
        self._phrase_map: dict[str, str] = {
            "haanji": "haanji.wav",
            "hello": "hello.wav",
            "namaste": "namaste.wav",
            "shukriya": "shukriya.wav",
            "phir milenge": "phir_milenge.wav",
            "accha": "accha.wav",
            "theek hai": "theek_hai.wav",
            "nahi": "nahi.wav",
            "haan": "haan.wav",
            "ok": "ok.wav",
        }
        
        # **TASK 2**: Greeting audio files for instant playback
        self._greeting_cache_map: dict[str, str] = {
            "hi-IN": "greeting_hi_IN.wav",
            "ta-IN": "greeting_ta_IN.wav",
            "te-IN": "greeting_te_IN.wav",
            "kn-IN": "greeting_kn_IN.wav",
        }

        # Ensure cache directory exists
        if self.cache_enabled:
            Path(self.cache_dir).mkdir(parents=True, exist_ok=True)
            self._initialize_mock_cache_files()

    def _initialize_mock_cache_files(self) -> None:
        """
        Create mock WAV files for testing purposes.
        Generates simple sine wave audio for each phrase and greeting in the cache.
        """
        try:
            for phrase, filename in self._phrase_map.items():
                filepath = os.path.join(self.cache_dir, filename)
                
                # Only create if doesn't exist
                if not os.path.exists(filepath):
                    self._create_mock_wav_file(filepath, phrase)
                    logger.info(f"Created mock cache file: {filename}")
            
            # Do not auto-generate mock greeting files.
            # Synthetic tones sound like long beeps and degrade call UX.
                    
        except Exception as e:
            logger.warning(f"Failed to initialize mock cache files: {e}")

    def _create_mock_wav_file(
        self, 
        filepath: str, 
        phrase: str,
        duration_ms: int = 1500
    ) -> None:
        """
        Create a mock WAV file with synthesized audio data.
        
        Args:
            filepath: Output file path
            phrase: Text phrase for the file
            duration_ms: Duration of generated audio in milliseconds
        """
        # Calculate parameters
        num_samples = int(self.sample_rate * duration_ms / 1000)
        
        # Generate simple sine wave tone
        frequency = 440  # A4 note
        audio_data = bytearray()
        
        for i in range(num_samples):
            # Generate sine wave sample
            sample = int(
                32767 * 0.3 * 
                __import__('math').sin(2 * __import__('math').pi * frequency * i / self.sample_rate)
            )
            # Pack as 16-bit signed little-endian
            audio_data.extend(struct.pack('<h', sample))

        # Create WAV file with proper headers
        with wave.open(filepath, 'wb') as wav_file:
            wav_file.setnchannels(1)  # Mono
            wav_file.setsampwidth(2)  # 16-bit
            wav_file.setframerate(self.sample_rate)
            wav_file.writeframes(bytes(audio_data))

    def lookup_phrase(self, text: str) -> Optional[Tuple[io.BytesIO, dict]]:
        """
        Fast lookup for cached phrase.
        Returns audio stream and metadata if found, None otherwise.
        
        Args:
            text: Input text to lookup
            
        Returns:
            Tuple of (audio_stream, metadata) or None if not cached
        """
        if not self.cache_enabled:
            return None

        # Normalize text for lookup
        normalized_text = text.strip().lower()
        
        # Direct lookup in phrase map
        if normalized_text not in self._phrase_map:
            logger.debug(f"Cache miss for phrase: {normalized_text}")
            return None

        filename = self._phrase_map[normalized_text]
        filepath = os.path.join(self.cache_dir, filename)

        try:
            # Read cached WAV file
            if not os.path.exists(filepath):
                logger.warning(f"Cache file not found: {filepath}")
                return None

            with open(filepath, 'rb') as f:
                audio_bytes = f.read()

            # Create in-memory stream
            audio_stream = io.BytesIO(audio_bytes)
            
            # Extract metadata from WAV file
            audio_stream.seek(0)
            with wave.open(audio_stream, 'rb') as wav_file:
                n_channels = wav_file.getnchannels()
                sample_width = wav_file.getsampwidth()
                framerate = wav_file.getframerate()
                n_frames = wav_file.getnframes()
                duration_ms = (n_frames / framerate) * 1000

            metadata = {
                "source": "cache",
                "filename": filename,
                "channels": n_channels,
                "sample_width": sample_width,
                "sample_rate": framerate,
                "duration_ms": duration_ms,
                "size_bytes": len(audio_bytes)
            }

            audio_stream.seek(0)
            logger.info(f"Cache hit for phrase: {normalized_text} ({len(audio_bytes)} bytes)")
            
            return audio_stream, metadata

        except Exception as e:
            logger.error(f"Error reading cached phrase '{normalized_text}': {e}")
            return None

    # **TASK 2**: Get cached greeting audio for instant playback
    def get_cached_greeting_audio(self, language_code: str) -> Optional[Tuple[bytes, dict]]:
        """
        Retrieve pre-rendered greeting audio for a language.
        Returns raw PCM bytes for instant streaming to client.
        
        Args:
            language_code: Language code (e.g., "hi-IN", "ta-IN")
            
        Returns:
            Tuple of (pcm_bytes, metadata) or None if not cached
        """
        if not self.cache_enabled:
            return None
        
        normalized_lang = (language_code or "hi-IN").strip().lower()
        filename = None
        
        # Match language code to greeting filename
        for supported_code, cached_filename in self._greeting_cache_map.items():
            if supported_code.lower() == normalized_lang:
                filename = cached_filename
                break
        
        # Fallback to Hindi if no match
        if not filename:
            filename = self._greeting_cache_map.get("hi-IN")
        
        if not filename:
            return None
        
        filepath = os.path.join(self.cache_dir, filename)
        if not os.path.exists(filepath):
            logger.debug(f"Greeting cache miss for language: {language_code}")
            return None
        
        try:
            # Read WAV file
            with open(filepath, 'rb') as f:
                wav_bytes = f.read()
            
            # Extract PCM data from WAV
            wav_stream = io.BytesIO(wav_bytes)
            with wave.open(wav_stream, 'rb') as wav_file:
                n_channels = wav_file.getnchannels()
                sample_width = wav_file.getsampwidth()
                framerate = wav_file.getframerate()
                n_frames = wav_file.getnframes()
                pcm_bytes = wav_file.readframes(n_frames)
                duration_ms = (n_frames / framerate) * 1000
            
            metadata = {
                "source": "cache",
                "filename": filename,
                "language_code": language_code,
                "channels": n_channels,
                "sample_width": sample_width,
                "sample_rate": framerate,
                "duration_ms": duration_ms,
                "size_bytes": len(pcm_bytes),
            }

            # Skip known synthetic placeholder greeting tone files.
            # Legacy mock greeting assets are 1.5s @ 22050Hz tone (~66150 PCM bytes).
            if (
                filename.startswith("greeting_")
                and metadata["sample_rate"] == 22050
                and metadata["channels"] == 1
                and metadata["size_bytes"] == 66150
            ):
                logger.warning(
                    "Ignoring synthetic greeting tone cache file: %s",
                    filename,
                )
                return None
            
            logger.info(f"Greeting audio cache hit for: {language_code} ({len(pcm_bytes)} bytes)")
            return pcm_bytes, metadata
            
        except Exception as e:
            logger.error(f"Error reading greeting cache '{filename}': {e}")
            return None

    def register_phrase(self, text: str, audio_bytes: bytes) -> bool:
        """
        Register a new phrase and cache its audio.
        Useful for caching dynamically generated responses.
        
        Args:
            text: Text phrase to cache
            audio_bytes: WAV audio bytes to store
            
        Returns:
            True if successful, False otherwise
        """
        if not self.cache_enabled:
            return False

        try:
            normalized_text = text.strip().lower()
            filename = f"{normalized_text}.wav"
            filepath = os.path.join(self.cache_dir, filename)

            # Write audio file
            with open(filepath, 'wb') as f:
                f.write(audio_bytes)

            # Update phrase map
            self._phrase_map[normalized_text] = filename

            logger.info(f"Registered cached phrase: {normalized_text}")
            return True

        except Exception as e:
            logger.error(f"Failed to register phrase '{text}': {e}")
            return False

    def clear_cache(self) -> bool:
        """
        Clear all cached files.
        Use with caution in production.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            if os.path.exists(self.cache_dir):
                for filename in os.listdir(self.cache_dir):
                    filepath = os.path.join(self.cache_dir, filename)
                    if os.path.isfile(filepath):
                        os.remove(filepath)
                logger.info("Cache cleared")
            return True
        except Exception as e:
            logger.error(f"Failed to clear cache: {e}")
            return False

    def get_cache_stats(self) -> dict:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache statistics
        """
        total_size = 0
        file_count = 0

        if os.path.exists(self.cache_dir):
            for filename in os.listdir(self.cache_dir):
                filepath = os.path.join(self.cache_dir, filename)
                if os.path.isfile(filepath):
                    total_size += os.path.getsize(filepath)
                    file_count += 1

        return {
            "cache_enabled": self.cache_enabled,
            "cache_dir": self.cache_dir,
            "total_phrases": len(self._phrase_map),
            "cached_files": file_count,
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2)
        }
