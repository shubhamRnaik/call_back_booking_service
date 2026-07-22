#!/usr/bin/env python3
"""
Verification Script: Checks that all Phase 3 components are properly implemented.
Run this to validate the installation before running the live test.
"""

import sys
import os
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("Verify")

def check_imports():
    """Verify all required imports work."""
    logger.info("Checking imports...")
    
    imports = [
        ("google.genai", "google-genai"),
        ("pyaudio", "pyaudio"),
        ("numpy", "numpy"),
        ("websockets", "websockets"),
        ("fastapi", "fastapi"),
        ("uvicorn", "uvicorn"),
        ("pydantic_settings", "pydantic-settings"),
        ("python_dotenv", "python-dotenv"),
        ("indic_numtowords", "indic-numtowords"),
    ]
    
    failed = []
    for module_name, package_name in imports:
        try:
            __import__(module_name)
            logger.info(f"  ✓ {package_name}")
        except ImportError as e:
            logger.error(f"  ✗ {package_name} - {e}")
            failed.append(package_name)
    
    return len(failed) == 0, failed


def check_files():
    """Verify all required files exist."""
    logger.info("Checking files...")
    
    files = [
        "indic_tts_runtime/config.py",
        "indic_tts_runtime/chunker.py",
        "indic_tts_runtime/normalizer.py",
        "indic_tts_runtime/services/stt_service.py",
        "indic_tts_runtime/services/sarvam_service.py",
        "indic_tts_runtime/brain/__init__.py",
        "indic_tts_runtime/brain/prompts.py",
        "indic_tts_runtime/brain/llm_service.py",
        "indic_tts_runtime/core/scheduler.py",
        "indic_tts_runtime/core/full_orchestrator.py",
        "test_end_to_end_voice_bot.py",
        ".env",
        "requirements.txt",
    ]
    
    failed = []
    for file_path in files:
        full_path = Path(file_path)
        if full_path.exists():
            logger.info(f"  ✓ {file_path}")
        else:
            logger.error(f"  ✗ {file_path} - NOT FOUND")
            failed.append(file_path)
    
    return len(failed) == 0, failed


def check_env():
    """Verify .env has required keys."""
    logger.info("Checking .env configuration...")
    
    from indic_tts_runtime.config import settings
    
    required_keys = [
        "sarvam_api_key",
        "gemini_api_key",
        "default_language_code",
        "stt_sample_rate",
        "tts_sample_rate",
    ]
    
    failed = []
    for key in required_keys:
        try:
            value = getattr(settings, key)
            if value and value != "your_sarvam_api_key_here" and value != "your_gemini_api_key_here":
                logger.info(f"  ✓ {key}")
            else:
                logger.error(f"  ⚠ {key} - placeholder value (set in .env)")
                if "api_key" in key:
                    failed.append(key)
        except AttributeError as e:
            logger.error(f"  ✗ {key} - {e}")
            failed.append(key)
    
    return len(failed) == 0, failed


def check_classes():
    """Verify key classes can be instantiated."""
    logger.info("Checking classes...")
    
    try:
        from indic_tts_runtime.services.stt_service import SarvamSaarasSTTClient
        logger.info("  ✓ SarvamSaarasSTTClient")
    except Exception as e:
        logger.error(f"  ✗ SarvamSaarasSTTClient - {e}")
    
    try:
        from indic_tts_runtime.brain.llm_service import StreamingBrain
        logger.info("  ✓ StreamingBrain")
    except Exception as e:
        logger.error(f"  ✗ StreamingBrain - {e}")
    
    try:
        from indic_tts_runtime.services.sarvam_service import SarvamWebSocketClient
        logger.info("  ✓ SarvamWebSocketClient")
    except Exception as e:
        logger.error(f"  ✗ SarvamWebSocketClient - {e}")
    
    try:
        from indic_tts_runtime.core.full_orchestrator import FullVoiceOrchestrator
        logger.info("  ✓ FullVoiceOrchestrator")
    except Exception as e:
        logger.error(f"  ✗ FullVoiceOrchestrator - {e}")
    
    try:
        from indic_tts_runtime.chunker import StreamTextChunker
        logger.info("  ✓ StreamTextChunker")
    except Exception as e:
        logger.error(f"  ✗ StreamTextChunker - {e}")
    
    try:
        from indic_tts_runtime.normalizer import MultilingualTextNormalizer
        logger.info("  ✓ MultilingualTextNormalizer")
    except Exception as e:
        logger.error(f"  ✗ MultilingualTextNormalizer - {e}")
    
    try:
        from indic_tts_runtime.core.scheduler import PacketScheduler
        logger.info("  ✓ PacketScheduler")
    except Exception as e:
        logger.error(f"  ✗ PacketScheduler - {e}")


def main():
    """Run all checks."""
    logger.info("=" * 80)
    logger.info("Phase 3 Implementation Verification")
    logger.info("=" * 80)
    
    print()
    imports_ok, import_failures = check_imports()
    
    print()
    files_ok, file_failures = check_files()
    
    print()
    check_classes()
    
    print()
    env_ok, env_failures = check_env()
    
    print()
    logger.info("=" * 80)
    
    if imports_ok and files_ok and env_ok:
        logger.info("✓ ALL CHECKS PASSED!")
        logger.info("\nYou can now run: python test_end_to_end_voice_bot.py")
        return 0
    else:
        logger.error("✗ SOME CHECKS FAILED")
        
        if import_failures:
            logger.error(f"\nMissing packages: {', '.join(import_failures)}")
            logger.error("Run: pip install " + " ".join(import_failures))
        
        if file_failures:
            logger.error(f"\nMissing files: {', '.join(file_failures)}")
        
        if env_failures:
            logger.error(f"\nMissing .env configuration: {', '.join(env_failures)}")
            logger.error("Edit .env and set your API keys")
        
        return 1


if __name__ == "__main__":
    sys.exit(main())
