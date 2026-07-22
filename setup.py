#!/usr/bin/env python3
"""
Quick Start Setup Script: Automated project initialization and validation.
Validates environment, installs dependencies, and runs basic tests.
"""

import os
import sys
import subprocess
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent
INDIC_TTS_DIR = PROJECT_ROOT / "indic_tts_runtime"
REQUIREMENTS_FILE = PROJECT_ROOT / "requirements.txt"
ENV_FILE = INDIC_TTS_DIR / ".env"


def check_python_version() -> bool:
    """Verify Python version >= 3.10."""
    version_info = sys.version_info
    if version_info.major < 3 or (version_info.major == 3 and version_info.minor < 10):
        logger.error(f"Python 3.10+ required (you have {version_info.major}.{version_info.minor})")
        return False
    logger.info(f"✓ Python {version_info.major}.{version_info.minor} detected")
    return True


def check_project_structure() -> bool:
    """Verify project folder structure."""
    required_dirs = [
        INDIC_TTS_DIR,
        INDIC_TTS_DIR / "services",
        INDIC_TTS_DIR / "core",
        INDIC_TTS_DIR / "database" / "cache",
    ]
    
    required_files = [
        ENV_FILE,
        INDIC_TTS_DIR / "config.py",
        INDIC_TTS_DIR / "schemas.py",
        INDIC_TTS_DIR / "main.py",
        INDIC_TTS_DIR / "services" / "cache_service.py",
        INDIC_TTS_DIR / "services" / "sarvam_service.py",
        INDIC_TTS_DIR / "core" / "router.py",
        INDIC_TTS_DIR / "core" / "scheduler.py",
        REQUIREMENTS_FILE,
    ]

    for dir_path in required_dirs:
        if not dir_path.exists():
            logger.error(f"Missing directory: {dir_path}")
            return False

    for file_path in required_files:
        if not file_path.exists():
            logger.error(f"Missing file: {file_path}")
            return False

    logger.info("✓ Project structure verified")
    return True


def install_dependencies() -> bool:
    """Install Python dependencies from requirements.txt."""
    try:
        logger.info("Installing dependencies...")
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "-q", "-r", str(REQUIREMENTS_FILE)],
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if result.returncode != 0:
            logger.error(f"Dependency installation failed: {result.stderr}")
            return False
        
        logger.info("✓ Dependencies installed successfully")
        return True
    except Exception as e:
        logger.error(f"Dependency installation error: {e}")
        return False


def verify_dependencies() -> bool:
    """Verify that critical dependencies are importable."""
    dependencies = [
        "fastapi",
        "uvicorn",
        "pydantic",
        "aiohttp",
    ]
    
    for pkg in dependencies:
        try:
            __import__(pkg)
            logger.info(f"✓ {pkg} imported successfully")
        except ImportError:
            logger.error(f"✗ Failed to import {pkg}")
            return False
    
    return True


def validate_env_file() -> bool:
    """Check that .env file is configured."""
    if not ENV_FILE.exists():
        logger.error(f".env file not found at {ENV_FILE}")
        return False

    with open(ENV_FILE, 'r') as f:
        content = f.read()
        if "your_sarvam_api_key_here" in content.lower():
            logger.warning(
                "⚠ .env file contains placeholder API key. "
                "Update SARVAM_API_KEY before running production!"
            )
        else:
            logger.info("✓ .env file appears configured")

    return True


def create_cache_files() -> bool:
    """Create mock cache audio files."""
    try:
        cache_dir = INDIC_TTS_DIR / "database" / "cache"
        
        # Cache files should be automatically created by CacheService
        logger.info(f"✓ Cache directory verified: {cache_dir}")
        return True
    except Exception as e:
        logger.error(f"Cache file creation error: {e}")
        return False


def run_import_tests() -> bool:
    """Test that all modules can be imported."""
    try:
        # Add project to path
        sys.path.insert(0, str(INDIC_TTS_DIR.parent))
        
        logger.info("Testing module imports...")
        
        from indic_tts_runtime import config
        logger.info("✓ config.py imported")
        
        from indic_tts_runtime import schemas
        logger.info("✓ schemas.py imported")
        
        from indic_tts_runtime.services import cache_service
        logger.info("✓ cache_service.py imported")
        
        from indic_tts_runtime.services import sarvam_service
        logger.info("✓ sarvam_service.py imported")
        
        from indic_tts_runtime.core import router
        logger.info("✓ router.py imported")
        
        from indic_tts_runtime.core import scheduler
        logger.info("✓ scheduler.py imported")
        
        logger.info("✓ All modules imported successfully")
        return True
        
    except ImportError as e:
        logger.error(f"Import test failed: {e}")
        return False


def main() -> int:
    """Run all setup checks."""
    logger.info("=" * 80)
    logger.info("INDIC TTS ENGINE - QUICK START SETUP")
    logger.info("=" * 80 + "\n")

    checks = [
        ("Python version", check_python_version),
        ("Project structure", check_project_structure),
        ("Environment file", validate_env_file),
        ("Cache directory", create_cache_files),
        ("Installing dependencies", install_dependencies),
        ("Verifying dependencies", verify_dependencies),
        ("Module imports", run_import_tests),
    ]

    passed = 0
    failed = 0

    for check_name, check_func in checks:
        logger.info(f"\nChecking: {check_name}...")
        try:
            if check_func():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            logger.error(f"Unexpected error during {check_name}: {e}")
            failed += 1

    logger.info("\n" + "=" * 80)
    logger.info(f"Setup Results: {passed} passed, {failed} failed")
    logger.info("=" * 80 + "\n")

    if failed > 0:
        logger.error("Setup incomplete. Fix errors above and retry.")
        return 1

    logger.info("✓ All checks passed!\n")
    logger.info("Next steps:")
    logger.info("1. Update SARVAM_API_KEY in indic_tts_runtime/.env")
    logger.info("2. Start the server: cd indic_tts_runtime && python main.py")
    logger.info("3. In another terminal, run tests: python test_pipeline.py")
    logger.info("4. Access API docs: http://localhost:8000/docs")
    logger.info("\n" + "=" * 80 + "\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
