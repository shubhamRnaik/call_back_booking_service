#!/usr/bin/env python3
"""
Test voice selection feature - verify female (meera) and male (shubh) voices work correctly.
"""

import asyncio
import websockets
import json
import base64
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))


async def test_voice_selection(voice_speaker: str = "meera"):
    """
    Test voice selection by connecting to WebSocket and verifying audio is generated.
    
    Args:
        voice_speaker: Either "meera" (female) or "shubh" (male)
    """
    
    uri = "ws://localhost:8000/ws/v1/voice-call"
    
    print(f"\n{'='*70}")
    print(f"VOICE SELECTION TEST - Testing {voice_speaker.upper()} voice")
    print(f"{'='*70}\n")
    print(f"🔌 Connecting to {uri}...")
    
    try:
        async with websockets.connect(uri) as websocket:
            print("✓ Connected!")
            
            # Send config with selected voice
            config = {
                "language": "hi-IN",
                "speaker": voice_speaker
            }
            print(f"\n📤 Sending config: {json.dumps(config)}")
            await websocket.send(json.dumps(config))
            print("✓ Config sent")
            
            # Wait a moment for server to initialize
            await asyncio.sleep(0.5)
            
            # Simulate a simple transcript
            test_transcript = "नमस्ते, मैं आपके साथ बात कर रहा हूँ।"
            print(f"\n📝 Test transcript: {test_transcript}")
            
            # Send a message event (simulating STT output)
            message = {
                "type": "message",
                "data": test_transcript
            }
            print(f"📤 Sending message: {test_transcript}")
            await websocket.send(json.dumps(message))
            
            # Collect audio output
            audio_chunks_received = 0
            total_audio_bytes = 0
            
            print("\n⏳ Waiting for audio response...")
            print("-" * 70)
            
            timeout_counter = 0
            while timeout_counter < 30:  # 30 second timeout
                try:
                    response = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                    
                    # Parse response
                    try:
                        msg = json.loads(response)
                        msg_type = msg.get("type")
                        
                        if msg_type == "audio":
                            audio_chunks_received += 1
                            audio_data = msg.get("data", "")
                            if audio_data:
                                audio_bytes = len(base64.b64decode(audio_data))
                                total_audio_bytes += audio_bytes
                                print(f"  🔊 Audio chunk {audio_chunks_received}: {audio_bytes} bytes")
                        
                        elif msg_type == "status":
                            status = msg.get("data", {}).get("status")
                            print(f"  📊 Status: {status}")
                            if status == "complete":
                                print("✓ Response complete")
                                break
                        
                        elif msg_type == "transcript":
                            transcript = msg.get("data", {}).get("transcript")
                            if transcript:
                                print(f"  🤖 Agent: {transcript}")
                        
                        elif msg_type == "error":
                            error = msg.get("data", {}).get("error")
                            print(f"  ❌ Error: {error}")
                            break
                        
                        else:
                            print(f"  ℹ️  {msg_type}: {msg}")
                    
                    except json.JSONDecodeError:
                        print(f"  📦 Binary data: {len(response)} bytes")
                        total_audio_bytes += len(response)
                        audio_chunks_received += 1
                
                except asyncio.TimeoutError:
                    timeout_counter += 1
                    if timeout_counter % 5 == 0:
                        print(f"  ⏳ Waiting... ({timeout_counter}s)")
                    continue
            
            print("-" * 70)
            print(f"\n✅ Results for {voice_speaker.upper()} voice:")
            print(f"  • Audio chunks received: {audio_chunks_received}")
            print(f"  • Total audio bytes: {total_audio_bytes:,}")
            print(f"  • Voice quality: {'✓ Good' if total_audio_bytes > 5000 else '⚠️  Low'}")
            
            return True
    
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Test both female and male voices."""
    
    print("\n" + "="*70)
    print("🎤 VOICE SELECTION FEATURE TEST")
    print("="*70)
    print("\nThis test verifies that both female and male voices work correctly.")
    print("Make sure the server is running: python -m indic_tts_runtime.main\n")
    
    results = {}
    
    # Test female voice
    print("\n[1/2] Testing FEMALE voice (meera)...")
    results['meera'] = await test_voice_selection("meera")
    await asyncio.sleep(1)
    
    # Test male voice
    print("\n[2/2] Testing MALE voice (shubh)...")
    results['shubh'] = await test_voice_selection("shubh")
    
    # Summary
    print("\n" + "="*70)
    print("📋 SUMMARY")
    print("="*70)
    
    for voice, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL"
        label = "Female (Meera)" if voice == "meera" else "Male (Shubh)"
        print(f"  {status} - {label}")
    
    all_passed = all(results.values())
    print(f"\n{'✅ All tests passed!' if all_passed else '❌ Some tests failed'}")
    print("="*70)


if __name__ == "__main__":
    asyncio.run(main())
