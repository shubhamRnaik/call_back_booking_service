#!/usr/bin/env python3
"""
Diagnostic trace test - connects to WebSocket voice-call endpoint and monitors logs
"""
import asyncio
import json
import websockets
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def test_voice_call_websocket():
    """Test WebSocket voice call endpoint with diagnostic logging"""
    
    uri = "ws://localhost:8000/ws/v1/voice-call"
    logger.info(f"🔌 Connecting to {uri}...")
    
    try:
        async with websockets.connect(uri) as websocket:
            logger.info("✅ WebSocket connected!")
            
            # Step 1: Send config
            logger.info("📤 Sending config message...")
            config = {
                "language": "hi-IN",
                "speaker": "shubh"
            }
            await websocket.send(json.dumps(config))
            logger.info(f"✅ Config sent: {config}")
            
            # Step 2: Wait for greeting
            logger.info("⏳ Waiting for greeting message...")
            greeting_timeout = time.time() + 15  # 15 second timeout
            
            while time.time() < greeting_timeout:
                try:
                    msg = await asyncio.wait_for(websocket.recv(), timeout=2)
                    
                    # Try parsing as JSON (transcript/status messages)
                    try:
                        data = json.loads(msg)
                        msg_type = data.get("type")
                        logger.info(f"📨 Server message: type={msg_type}, data={json.dumps(data)[:100]}")
                    except:
                        # Binary audio data
                        logger.info(f"🔊 Audio chunk received: {len(msg)} bytes")
                        
                except asyncio.TimeoutError:
                    logger.warning("⏱️  No message received in 2s")
                except Exception as e:
                    logger.error(f"❌ Error receiving: {e}")
                    break
                    
            # Step 3: Simulate speech - send dummy audio
            logger.info("🎤 Sending dummy audio chunk (simulating speech)...")
            dummy_audio = b'\x00' * 4096  # 4KB of silence
            await websocket.send(dummy_audio)
            logger.info("✅ Audio chunk sent")
            
            # Step 4: Wait for response
            logger.info("⏳ Waiting for response (30 seconds)...")
            response_timeout = time.time() + 30
            response_started = False
            
            while time.time() < response_timeout:
                try:
                    msg = await asyncio.wait_for(websocket.recv(), timeout=2)
                    
                    try:
                        data = json.loads(msg)
                        msg_type = data.get("type")
                        logger.info(f"📨 Response: type={msg_type}, data={json.dumps(data)[:100]}")
                        if msg_type == "transcript" and data.get("role") == "assistant":
                            response_started = True
                    except:
                        # Audio chunk
                        if len(msg) > 100:
                            logger.info(f"🔊 RESPONSE AUDIO RECEIVED: {len(msg)} bytes")
                            response_started = True
                        
                except asyncio.TimeoutError:
                    if response_started:
                        logger.info("✅ Response received! Waiting for more...")
                    else:
                        logger.warning("⏱️  Still waiting for response...")
                except Exception as e:
                    logger.error(f"Error: {e}")
                    break
            
            if response_started:
                logger.info("✅ SUCCESS: Response received from server!")
            else:
                logger.warning("❌ NO RESPONSE: Server did not send any assistant response")
                
    except Exception as e:
        logger.error(f"❌ Connection error: {e}")


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("DIAGNOSTIC TRACE TEST")
    logger.info("=" * 60)
    asyncio.run(test_voice_call_websocket())
