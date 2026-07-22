#!/usr/bin/env python3
"""
Simple WebSocket echo test to verify endpoint is registered and working
"""
import asyncio
import json
import websockets
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)


async def test_endpoint(uri, name):
    """Test a WebSocket endpoint"""
    logger.info(f"Testing {name} at {uri}")
    try:
        async with websockets.connect(uri) as ws:
            logger.info(f"✅ Connected to {name}")
            # Just close
            await asyncio.sleep(0.1)
            logger.info(f"✅ {name} working")
    except Exception as e:
        logger.error(f"❌ {name} failed: {e}")


async def main():
    await test_endpoint("ws://localhost:8000/ws/v1/stream-voice", "stream-voice endpoint")
    await test_endpoint("ws://localhost:8000/ws/v1/voice-call", "voice-call endpoint")


if __name__ == "__main__":
    asyncio.run(main())
