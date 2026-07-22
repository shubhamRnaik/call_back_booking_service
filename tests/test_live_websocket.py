import asyncio
import json
import base64
import websockets
import wave
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_live_streaming():
    """Test WebSocket streaming with real Sarvam API."""
    
    uri = "ws://localhost:8000/ws/v1/stream-voice"
    
    logger.info("🔌 Connecting to WebSocket server...")
    
    try:
        async with websockets.connect(uri) as ws:
            logger.info("✓ Connected!")
            
            # Test 1: Hindi with currency
            logger.info("\n📢 Test 1: Hindi (₹450)")
            config = {
                "language": "hi-IN",
                "speaker": "shubh",
                "pace": 0.95
            }
            await ws.send(json.dumps(config))
            logger.info("  → Config sent")
            
            # Give server time to process config
            await asyncio.sleep(0.5)
            
            text_msg = {
                "type": "text",
                "data": {"text": "मेरे पास ₹450 हैं"}
            }
            await ws.send(json.dumps(text_msg))
            logger.info("  → Text sent: 'मेरे पास ₹450 हैं'")
            
            # Collect audio chunks
            audio_frames = []
            ttfb_ms = None
            
            try:
                while True:
                    response = await asyncio.wait_for(ws.recv(), timeout=30)
                    msg = json.loads(response)
                    
                    if msg["type"] == "ttfb":
                        ttfb_ms = msg["data"]["ttfb_ms"]
                        logger.info(f"  ⏱️  TTFB: {ttfb_ms:.2f}ms ✓")
                    
                    elif msg["type"] == "audio":
                        audio_data = base64.b64decode(msg["data"]["audio"])
                        audio_frames.append(audio_data)
                        logger.info(f"  📦 Received {len(audio_data)} bytes")
                    
                    elif msg["type"] == "error":
                        error_msg = msg["data"].get("error", "Unknown error")
                        logger.error(f"  ✗ Server error: {error_msg}")
                        break
                    
                    elif msg["type"] == "done":
                        logger.info("  ✓ Synthesis complete")
                        break
            
            except asyncio.TimeoutError:
                logger.error("  ✗ Timeout waiting for response (30s)")
                logger.info("  → Check server logs for errors")
            
            # Save audio to WAV file
            if audio_frames:
                audio_bytes = b"".join(audio_frames)
                save_wav("test_hi_live.wav", audio_bytes)
                logger.info(f"  💾 Saved to test_hi_live.wav ({len(audio_bytes)} bytes)")
            else:
                logger.warning("  ⚠️  No audio received")
    
    except Exception as e:
        logger.error(f"✗ Connection failed: {e}")
        logger.info("→ Make sure server is running: python -m uvicorn indic_tts_runtime.main:app --host 0.0.0.0 --port 8000")

def save_wav(filename, audio_bytes):
    """Save PCM audio to WAV file."""
    with wave.open(filename, 'wb') as wav_file:
        wav_file.setnchannels(1)  # Mono
        wav_file.setsampwidth(2)  # 16-bit
        wav_file.setframerate(8000)  # 8kHz
        wav_file.writeframes(audio_bytes)

if __name__ == "__main__":
    logger.info("🎤 Live WebSocket TTS Test\n")
    asyncio.run(test_live_streaming())
    logger.info("\n✓ Test complete!")