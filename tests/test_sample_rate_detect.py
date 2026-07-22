#!/usr/bin/env python3
"""Detect actual sample rate from Sarvam API"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from sarvamai import AsyncSarvamAI
from indic_tts_runtime.config import Settings

async def detect_sample_rate():
    """Test to determine Sarvam's actual output sample rate"""
    
    config = Settings()
    
    print("\n" + "="*80)
    print("DETECTING SARVAM AUDIO SAMPLE RATE")
    print("="*80 + "\n")
    
    async with AsyncSarvamAI(api_subscription_key=config.sarvam_api_key) as client:
        
        await client.configure(
            target_language_code="hi-IN",
            speaker="shubh",
            pace=1.0,
            output_audio_codec="linear16"
        )
        
        print("📤 Sending test text: 'नमस्ते'")
        await client.convert("नमस्ते")
        await client.flush()
        
        print("📥 Receiving audio...\n")
        
        total_bytes = 0
        chunk_count = 0
        
        async for audio_bytes in client.stream():
            if isinstance(audio_bytes, bytes):
                chunk_count += 1
                total_bytes += len(audio_bytes)
        
        print(f"✓ Received {chunk_count} chunks, {total_bytes} bytes total")
        
        # Try different sample rates
        test_sample_rates = [8000, 16000, 22050, 24000, 44100, 48000]
        
        print(f"\n📊 Testing possible sample rates:\n")
        
        for sr in test_sample_rates:
            duration = total_bytes / (sr * 2)  # 16-bit = 2 bytes per sample
            print(f"  If sample rate is {sr:5d} Hz → Duration: {duration:.3f}s")
        
        print(f"\n🎯 Most likely: 22050 Hz or 24000 Hz (typical TTS rates)")
        print(f"   Your current config: 8000 Hz")
        print(f"   That's {22050/8000:.2f}x mismatch!")

if __name__ == "__main__":
    asyncio.run(detect_sample_rate())
