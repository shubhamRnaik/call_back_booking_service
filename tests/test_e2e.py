"""
Comprehensive end-to-end test of the voice streaming pipeline.
Tests with new API key, multiple languages, and TTFB measurement.
"""

import asyncio
import websockets
import json
import base64
import time
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))

from indic_tts_runtime.config import settings


async def test_websocker_pipeline():
    """Test complete WebSocket streaming pipeline."""
    
    print("=" * 70)
    print("COMPREHENSIVE E2E TEST - VOICE STREAMING PIPELINE")
    print("=" * 70)
    print(f"\n🔑 API Key: {settings.sarvam_api_key[:20]}...{settings.sarvam_api_key[-10:]}")
    print(f"🌐 Server: ws://localhost:8000/ws/v1/stream-voice")
    
    # Test cases
    test_cases = [
        {
            "name": "Hindi: Currency (₹450)",
            "language": "hi-IN",
            "speaker": "shubh",
            "text": "मेरे पास चार सौ पचास रुपये हैं।",
            "output": "test_hi_currency.wav"
        },
        {
            "name": "Tamil: Simple Text",
            "language": "ta-IN",
            "speaker": "shubh",
            "text": "வணக்கம், இது ஒரு சோதனை.",
            "output": "test_ta_simple.wav"
        },
        {
            "name": "Hindi: Long Text",
            "language": "hi-IN",
            "speaker": "shubh",
            "text": "भारत की संस्कृति विश्व की सबसे प्राचीन और समृद्ध संस्कृतियों में से एक है।",
            "output": "test_hi_long.wav"
        }
    ]
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n{'=' * 70}")
        print(f"TEST {i}: {test_case['name']}")
        print(f"{'=' * 70}")
        
        try:
            uri = "ws://localhost:8000/ws/v1/stream-voice"
            
            print(f"🔌 Connecting to {uri}...")
            start_connect = time.time()
            
            async with websockets.connect(uri) as websocket:
                connect_time = (time.time() - start_connect) * 1000
                print(f"✓ Connected in {connect_time:.2f}ms")
                
                # Step 1: Send config
                print(f"\n📝 Sending configuration...")
                config = {
                    "type": "config",
                    "data": {
                        "language": test_case["language"],
                        "speaker": test_case["speaker"],
                        "pace": 0.95
                    }
                }
                await websocket.send(json.dumps(config))
                print(f"✓ Config sent: lang={test_case['language']}, speaker={test_case['speaker']}")
                
                # Step 2: Send text
                print(f"\n📤 Sending text...")
                text_msg = {
                    "type": "text",
                    "data": {
                        "text": test_case["text"]
                    }
                }
                start_text = time.time()
                await websocket.send(json.dumps(text_msg))
                print(f"✓ Text sent: {test_case['text'][:60]}...")
                
                # Step 3: Receive audio and measure TTFB
                print(f"\n🎵 Receiving audio...")
                chunk_count = 0
                total_bytes = 0
                ttfb_ms = None
                audio_data = bytearray()
                
                output_path = Path(__file__).parent / "test_outputs" / test_case["output"]
                output_path.parent.mkdir(exist_ok=True)
                
                try:
                    while True:
                        msg = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                        data = json.loads(msg)
                        
                        if data.get("type") == "audio":
                            if ttfb_ms is None:
                                ttfb_ms = (time.time() - start_text) * 1000
                                print(f"⏱️  TTFB: {ttfb_ms:.2f}ms ✓" if ttfb_ms < 220 else f"⏱️  TTFB: {ttfb_ms:.2f}ms ⚠️")
                            
                            chunk_count += 1
                            audio_b64 = data["data"]["audio"]
                            audio_chunk = base64.b64decode(audio_b64)
                            audio_data.extend(audio_chunk)
                            total_bytes += len(audio_chunk)
                            
                            if chunk_count <= 3 or chunk_count % 5 == 0:
                                print(f"  ✓ Chunk {chunk_count}: {len(audio_chunk)} bytes (total: {total_bytes})")
                        
                        elif data.get("type") == "ttfb":
                            ttfb_from_server = data["data"]["ttfb_ms"]
                            print(f"⏱️  Server TTFB: {ttfb_from_server:.2f}ms")
                
                except asyncio.TimeoutError:
                    print(f"✓ Stream complete (timeout reached)")
                
                # Save audio file
                if audio_data:
                    # Simple WAV header (22050Hz, 16-bit, mono, PCM)
                    # FIXED: Changed from 8000 Hz to 22050 Hz (Sarvam's actual output)
                    sample_rate = 22050
                    channels = 1
                    bit_depth = 16
                    
                    wav_header = bytearray(44)
                    wav_header[0:4] = b'RIFF'
                    wav_header[4:8] = (len(audio_data) + 36).to_bytes(4, 'little')
                    wav_header[8:12] = b'WAVE'
                    wav_header[12:16] = b'fmt '
                    wav_header[16:20] = (16).to_bytes(4, 'little')  # Subchunk1Size
                    wav_header[20:22] = (1).to_bytes(2, 'little')   # PCM format
                    wav_header[22:24] = channels.to_bytes(2, 'little')
                    wav_header[24:28] = sample_rate.to_bytes(4, 'little')
                    wav_header[28:32] = (sample_rate * channels * bit_depth // 8).to_bytes(4, 'little')
                    wav_header[32:34] = (channels * bit_depth // 8).to_bytes(2, 'little')
                    wav_header[34:36] = bit_depth.to_bytes(2, 'little')
                    wav_header[36:40] = b'data'
                    wav_header[40:44] = len(audio_data).to_bytes(4, 'little')
                    
                    with open(output_path, "wb") as f:
                        f.write(wav_header)
                        f.write(audio_data)
                    
                    print(f"\n✓ RESULTS:")
                    print(f"  • Chunks received: {chunk_count}")
                    print(f"  • Total audio bytes: {total_bytes}")
                    print(f"  • Audio duration: {(total_bytes / 2) / sample_rate:.2f}s")
                    print(f"  • TTFB: {ttfb_ms:.2f}ms {'✓' if ttfb_ms < 220 else '⚠️'}")
                    print(f"  • Output file: {output_path}")
        
        except ConnectionRefusedError:
            print(f"✗ Failed to connect - Server not running?")
            print(f"  Start server: python -m uvicorn indic_tts_runtime.main:app --host 0.0.0.0 --port 8000")
            break
        except Exception as e:
            print(f"✗ Error: {e}")


if __name__ == "__main__":
    print("\n🚀 Make sure the server is running:")
    print("   python -m uvicorn indic_tts_runtime.main:app --host 0.0.0.0 --port 8000")
    print("\n⏳ Waiting 2 seconds... (press Ctrl+C to cancel)\n")
    
    try:
        time.sleep(2)
        asyncio.run(test_websocker_pipeline())
    except KeyboardInterrupt:
        print("\n❌ Test cancelled")
