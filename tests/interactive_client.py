"""
Interactive WebSocket client for testing the voice streaming pipeline.
Connect to server, send text, receive audio, save to WAV, and play it back.
"""

import asyncio
import websockets
import json
import base64
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))


async def interactive_client():
    """Interactive WebSocket client."""
    
    uri = "ws://localhost:8000/ws/v1/stream-voice"
    
    print("=" * 70)
    print("INTERACTIVE WEBSOCKET CLIENT - VOICE STREAMING TEST")
    print("=" * 70)
    print(f"\n🔌 Connecting to {uri}...")
    
    try:
        async with websockets.connect(uri) as websocket:
            print("✓ Connected!")
            
            # Get user input for configuration
            print("\n📝 Configuration:")
            language = input("  Language code (default: hi-IN): ").strip() or "hi-IN"
            speaker = input("  Speaker name (default: shubh): ").strip() or "shubh"
            pace = float(input("  Speech pace 0.5-2.0 (default: 1.2): ").strip() or "1.2")
            
            # Send config
            print(f"\n📤 Sending config: lang={language}, speaker={speaker}, pace={pace}")
            config = {
                "type": "config",
                "data": {
                    "language": language,
                    "speaker": speaker,
                    "pace": pace
                }
            }
            await websocket.send(json.dumps(config))
            print("✓ Config sent")
            
            # Main loop for multiple utterances
            while True:
                print("\n" + "=" * 70)
                text = input("\n📝 Enter text to synthesize (or 'quit' to exit): ").strip()
                
                if text.lower() in ["quit", "exit", "q"]:
                    print("\n👋 Goodbye!")
                    break
                
                if not text:
                    print("⚠️  Empty text, skipping")
                    continue
                
                # Send text
                print(f"\n📤 Sending text: {text[:60]}...")
                text_msg = {
                    "type": "text",
                    "data": {"text": text}
                }
                await websocket.send(json.dumps(text_msg))
                
                # Receive audio
                print("🎵 Receiving audio...\n")
                chunk_count = 0
                total_bytes = 0
                ttfb_ms = None
                audio_data = bytearray()
                
                start_time = asyncio.get_event_loop().time()
                
                try:
                    while True:
                        # Set timeout for receiving message
                        msg = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                        data = json.loads(msg)
                        msg_type = data.get("type")
                        
                        if msg_type == "ttfb":
                            ttfb_ms = data["data"]["ttfb_ms"]
                            status = "✓" if ttfb_ms < 220 else "⚠️"
                            print(f"  ⏱️  TTFB: {ttfb_ms:.2f}ms {status}")
                        
                        elif msg_type == "audio":
                            chunk_count += 1
                            audio_b64 = data["data"]["audio"]
                            audio_chunk = base64.b64decode(audio_b64)
                            audio_data.extend(audio_chunk)
                            total_bytes += len(audio_chunk)
                            
                            # Show progress every 5 chunks
                            if chunk_count % 5 == 0 or chunk_count == 1:
                                elapsed = asyncio.get_event_loop().time() - start_time
                                duration_sec = total_bytes / (2 * 8000)  # 16-bit, 8kHz
                                print(f"  ✓ Chunk {chunk_count}: {len(audio_chunk):6d} bytes | "
                                      f"Total: {total_bytes:7d} bytes | Duration: {duration_sec:.2f}s | Elapsed: {elapsed:.1f}s")
                        
                        elif msg_type == "error":
                            error = data["data"]["error"]
                            print(f"  ✗ Server error: {error}")
                            break
                        
                        elif msg_type == "done":
                            print(f"  ✓ Stream complete")
                            break
                
                except asyncio.TimeoutError:
                    print(f"  ✓ Stream complete (timeout reached after {chunk_count} chunks)")
                
                # Save audio to WAV file
                if audio_data:
                    output_file = Path(__file__).parent / "test_outputs" / "interactive_output.wav"
                    output_file.parent.mkdir(exist_ok=True)
                    
                    # Create WAV header
                    # FIXED: Changed from 8000 Hz to 22050 Hz (Sarvam's actual output)
                    sample_rate = 22050
                    channels = 1
                    bit_depth = 16
                    
                    wav_header = bytearray(44)
                    wav_header[0:4] = b'RIFF'
                    wav_header[4:8] = (len(audio_data) + 36).to_bytes(4, 'little')
                    wav_header[8:12] = b'WAVE'
                    wav_header[12:16] = b'fmt '
                    wav_header[16:20] = (16).to_bytes(4, 'little')
                    wav_header[20:22] = (1).to_bytes(2, 'little')
                    wav_header[22:24] = channels.to_bytes(2, 'little')
                    wav_header[24:28] = sample_rate.to_bytes(4, 'little')
                    wav_header[28:32] = (sample_rate * channels * bit_depth // 8).to_bytes(4, 'little')
                    wav_header[32:34] = (channels * bit_depth // 8).to_bytes(2, 'little')
                    wav_header[34:36] = bit_depth.to_bytes(2, 'little')
                    wav_header[36:40] = b'data'
                    wav_header[40:44] = len(audio_data).to_bytes(4, 'little')
                    
                    with open(output_file, "wb") as f:
                        f.write(wav_header)
                        f.write(audio_data)
                    
                    print(f"\n📁 Audio saved to: {output_file}")
                    print(f"   • Chunks: {chunk_count}")
                    print(f"   • Total bytes: {total_bytes}")
                    print(f"   • Duration: {(total_bytes / 2) / sample_rate:.2f}s")
                    if ttfb_ms:
                        print(f"   • TTFB: {ttfb_ms:.2f}ms")
                    
                    # Try to play the audio
                    try:
                        import subprocess
                        print(f"\n🎧 Playing audio...")
                        subprocess.run(["powershell", "-Command", f"(New-Object System.Media.SoundPlayer '{output_file}').PlaySync()"], timeout=30)
                    except Exception as e:
                        print(f"   ℹ️  Could not play audio automatically: {e}")
                        print(f"   📝 File saved at: {output_file}")
                        print(f"   💡 You can open it manually to listen")
    
    except ConnectionRefusedError:
        print(f"\n✗ Could not connect to server at {uri}")
        print(f"\n🚀 Make sure the server is running:")
        print(f"   python -m uvicorn indic_tts_runtime.main:app --host 0.0.0.0 --port 8000")
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    print("\n💡 Interactive WebSocket Client for Voice Streaming\n")
    print("Make sure server is running:")
    print("  python -m uvicorn indic_tts_runtime.main:app --host 0.0.0.0 --port 8000\n")
    
    asyncio.run(interactive_client())
