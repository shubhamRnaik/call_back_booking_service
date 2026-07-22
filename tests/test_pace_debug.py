#!/usr/bin/env python3
"""Debug script to test pace parameter directly against Sarvam API"""
import asyncio
import json
import sys
import struct
import wave
from pathlib import Path

# Add package to path
sys.path.insert(0, str(Path(__file__).parent))

from indic_tts_runtime.services.sarvam_service import SarvamWebSocketClient
from indic_tts_runtime.config import Settings

async def test_pace_values():
    """Test 3 extreme pace values to verify Sarvam respects pace parameter"""
    
    config = Settings()
    test_text = "नमस्ते स्वागत है"  # Slightly longer for better comparison
    
    # Create output directory
    output_dir = Path("test_outputs")
    output_dir.mkdir(exist_ok=True)
    
    pace_values = [0.5, 1.0, 2.0]  # Very slow, normal, very fast
    results = []
    
    for pace in pace_values:
        print(f"\n{'='*60}")
        print(f"🎤 Testing PACE: {pace}x")
        print(f"{'='*60}")
        
        client = SarvamWebSocketClient()
        
        try:
            # Connect with specific pace
            print(f"✓ Connecting with pace={pace}...")
            success = await client.connect(
                target_language_code="hi-IN",
                speaker="shubh",
                pace=pace
            )
            
            if not success:
                print(f"✗ Failed to connect")
                continue
            
            print(f"✓ Connected successfully")
            
            # Send text
            print(f"→ Sending text: '{test_text}'")
            await client.send_text_chunk(test_text)
            await client.send_flush()
            
            # Collect audio
            chunk_count = 0
            total_bytes = 0
            audio_data = b""
            
            print(f"📡 Receiving audio chunks...")
            async for audio_bytes in client.stream_audio_chunks():
                chunk_count += 1
                total_bytes += len(audio_bytes)
                audio_data += audio_bytes
            
            print(f"✓ Received {chunk_count} chunks, {total_bytes} bytes")
            
            # Save as WAV file
            filename = output_dir / f"pace_{pace}x.wav"
            
            # WAV header for 22050Hz, 16-bit, mono (FIXED from 8000 Hz)
            sample_rate = 22050
            channels = 1
            sample_width = 2
            
            with wave.open(str(filename), 'wb') as wav_file:
                wav_file.setnchannels(channels)
                wav_file.setsampwidth(sample_width)
                wav_file.setframerate(sample_rate)
                wav_file.writeframes(audio_data)
            
            file_size_kb = len(audio_data) / 1024
            duration_sec = len(audio_data) / (sample_rate * sample_width)
            
            print(f"💾 Saved: {filename}")
            print(f"   Size: {file_size_kb:.1f} KB")
            print(f"   Duration: {duration_sec:.2f}s")
            
            results.append({
                'pace': pace,
                'chunks': chunk_count,
                'bytes': total_bytes,
                'size_kb': file_size_kb,
                'duration_sec': duration_sec,
                'filename': str(filename)
            })
            
            # Disconnect
            await client.disconnect()
            print(f"✓ Disconnected")
            
        except Exception as e:
            print(f"✗ Error: {e}")
            import traceback
            traceback.print_exc()
        
        # Small delay between tests
        await asyncio.sleep(1)
    
    print(f"\n{'='*60}")
    print("📊 COMPARISON RESULTS")
    print(f"{'='*60}")
    
    for r in results:
        print(f"\n🎵 Pace {r['pace']}x:")
        print(f"   Chunks: {r['chunks']}")
        print(f"   Bytes: {r['bytes']}")
        print(f"   Size: {r['size_kb']:.1f} KB")
        print(f"   Duration: {r['duration_sec']:.2f}s")
        print(f"   File: {r['filename']}")
    
    if results:
        print(f"\n📌 ANALYSIS:")
        if len(results) >= 2:
            slow_chunks = results[0]['chunks']
            fast_chunks = results[2]['chunks']
            ratio = slow_chunks / fast_chunks if fast_chunks > 0 else 0
            print(f"   0.5x vs 2.0x chunk ratio: {ratio:.2f}x")
            if ratio > 1.5:
                print(f"   ✅ PACE IS WORKING! (2.0x has fewer chunks)")
            else:
                print(f"   ⚠️ PACE MIGHT NOT BE WORKING (similar chunk counts)")
    
    print(f"\n{'='*60}")
    print("📂 FILES SAVED TO: test_outputs/")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    asyncio.run(test_pace_values())
