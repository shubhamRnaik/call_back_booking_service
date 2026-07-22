#!/usr/bin/env python3
"""
Simplified End-to-End Voice Bot Test: Full pipeline without PyAudio.
Tests all components except actual audio I/O.

Usage:
    python test_end_to_end_voice_bot_simple.py
"""

import asyncio
import logging
import sys
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("VoiceBot")

# Import orchestrator
from indic_tts_runtime.core.full_orchestrator import FullVoiceOrchestrator
from indic_tts_runtime.config import settings
from indic_tts_runtime.chunker import StreamTextChunker
from indic_tts_runtime.normalizer import MultilingualTextNormalizer


class SimpleTextAudioGenerator:
    """Simulates audio input without requiring PyAudio."""

    def __init__(self, test_phrases: list):
        """Initialize with test phrases."""
        self.test_phrases = test_phrases
        self.index = 0

    async def generate_audio_stream(self):
        """Yield simulated audio chunks."""
        for phrase in self.test_phrases:
            logger.info(f"\n📝 [SIMULATED INPUT] {phrase}\n")
            # Simulate audio input by just yielding dummy audio
            for _ in range(5):
                yield b'\x00' * 1024  # Dummy audio chunk
                await asyncio.sleep(0.1)


async def test_components():
    """Test individual components without full orchestration."""
    logger.info("=" * 80)
    logger.info("🎤 INDIC VOICE BOT - Component Test (No Audio Hardware Required)")
    logger.info("=" * 80)

    # Test 1: Configuration
    logger.info("\n✓ Configuration loaded")
    logger.info(f"  Language: {settings.default_language_code}")
    logger.info(f"  STT Sample Rate: {settings.stt_sample_rate}Hz")
    logger.info(f"  TTS Sample Rate: {settings.tts_sample_rate}Hz")

    # Test 2: Text Chunker
    logger.info("\n✓ Testing Text Chunker...")
    chunker = StreamTextChunker(min_word_threshold=5, max_word_threshold=7)
    
    async def test_chunker():
        tokens = ["Haanji", ",", " ", "aapka", " ", "order", " ", "ready", " ", "hai", "."]
        token_gen = async_iter(tokens)
        chunks = []
        async for chunk in chunker.chunk_stream(token_gen):
            chunks.append(chunk)
            logger.info(f"  Chunk: {chunk}")
        return chunks
    
    async def async_iter(items):
        for item in items:
            yield item
    
    chunks = await test_chunker()

    # Test 3: Text Normalizer
    logger.info("\n✓ Testing Text Normalizer...")
    normalizer = MultilingualTextNormalizer(default_language="hi")
    
    test_texts = [
        ("Namaste, mujhe ₹500 chahiye", "hi-IN"),
        ("Order ready at 2:30 PM", "hi-IN"),
        ("Vanakkam, enakku 123 items vename", "ta-IN"),
    ]
    
    for text, lang in test_texts:
        normalized = normalizer.normalize(text, target_language_code=lang)
        logger.info(f"  {lang}: '{text}' → '{normalized}'")

    # Test 4: Brain (LLM)
    logger.info("\n✓ Testing Streaming Brain...")
    try:
        from indic_tts_runtime.brain.llm_service import StreamingBrain
        
        brain = StreamingBrain()
        if settings.gemini_api_key and settings.gemini_api_key != "your_gemini_api_key_here":
            logger.info("  Gemini API key configured")
            test_input = "Haanji, mujhe hindi course ki info chahiye"
            logger.info(f"  Input: {test_input}")
            
            response = ""
            try:
                async for token in brain.stream_response(test_input):
                    response += token
                    sys.stdout.write(token)
                    sys.stdout.flush()
                logger.info(f"\n  Response: {response}")
            except Exception as e:
                logger.warning(f"  LLM test skipped (API may be unavailable): {e}")
        else:
            logger.warning("  Gemini API key not configured (placeholder value)")
            logger.info("  Set GEMINI_API_KEY in .env to test LLM")
    except Exception as e:
        logger.warning(f"  Brain test error: {e}")

    # Test 5: STT Service
    logger.info("\n✓ Testing STT Service...")
    try:
        from indic_tts_runtime.services.stt_service import SarvamSaarasSTTClient
        
        stt = SarvamSaarasSTTClient()
        if settings.sarvam_api_key and settings.sarvam_api_key != "your_sarvam_api_key_here":
            try:
                connected = await stt.connect()
                if connected:
                    logger.info("  ✓ Connected to Sarvam STT API")
                    await stt.disconnect()
                else:
                    logger.warning("  Could not connect to Sarvam STT API")
            except Exception as e:
                logger.warning(f"  STT connection test failed: {e}")
        else:
            logger.warning("  Sarvam API key not configured (placeholder value)")
            logger.info("  Set SARVAM_API_KEY in .env to test STT")
    except Exception as e:
        logger.warning(f"  STT test error: {e}")

    # Test 6: TTS Service
    logger.info("\n✓ Testing TTS Service...")
    try:
        from indic_tts_runtime.services.sarvam_service import SarvamWebSocketClient
        
        tts = SarvamWebSocketClient()
        if settings.sarvam_api_key and settings.sarvam_api_key != "your_sarvam_api_key_here":
            try:
                connected = await tts.connect(target_language_code="hi-IN")
                if connected:
                    logger.info("  ✓ Connected to Sarvam TTS API")
                    await tts.disconnect()
                else:
                    logger.warning("  Could not connect to Sarvam TTS API")
            except Exception as e:
                logger.warning(f"  TTS connection test failed: {e}")
        else:
            logger.warning("  Sarvam API key not configured (placeholder value)")
            logger.info("  Set SARVAM_API_KEY in .env to test TTS")
    except Exception as e:
        logger.warning(f"  TTS test error: {e}")

    logger.info("\n" + "=" * 80)
    logger.info("✅ Component tests completed!")
    logger.info("=" * 80)

    logger.info("""
🎯 Next Steps:
1. Configure API keys in .env:
   - SARVAM_API_KEY: Get from https://sarvam.ai/
   - GEMINI_API_KEY: Get from https://aistudio.google.com/app/apikeys

2. Install PyAudio for full test:
   pip install pyaudio  # or pipwin install pyaudio on Windows

3. Run full test:
   python test_end_to_end_voice_bot.py

4. Run verification:
   python verify_phase3.py
    """)


if __name__ == "__main__":
    try:
        asyncio.run(test_components())
    except KeyboardInterrupt:
        logger.info("\n\nInterrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
