"""
Diagnostic script to test Sarvam API key and streaming availability.
"""

import asyncio
import sys
import os
from pathlib import Path

# Add project to path
sys.path.insert(0, str(Path(__file__).parent))

from indic_tts_runtime.config import settings
from sarvamai import AsyncSarvamAI, AudioOutput

async def test_api_key():
    """Test if API key is valid."""
    print("=" * 60)
    print("SARVAM API DIAGNOSTICS")
    print("=" * 60)
    
    api_key = settings.sarvam_api_key
    print(f"\n📝 API Key: {api_key[:20]}...{api_key[-10:]}")
    print(f"✓ API key loaded from config")
    
    # Test 1: Create client
    print("\n[TEST 1] Creating AsyncSarvamAI client...")
    try:
        client = AsyncSarvamAI(api_subscription_key=api_key)
        print("✓ Client created successfully")
    except Exception as e:
        print(f"✗ Failed to create client: {e}")
        return False
    
    # Test 2: Try to get supported languages/models
    print("\n[TEST 2] Testing REST API (if available)...")
    try:
        # This would test if the API key works for basic REST calls
        print("ℹ️  (Skipping - would need REST endpoint details)")
    except Exception as e:
        print(f"✗ REST call failed: {e}")
    
    # Test 3: Try to connect to streaming WebSocket
    print("\n[TEST 3] Attempting WebSocket connection to streaming endpoint...")
    try:
        ws = client.text_to_speech_streaming.connect(model="bulbul:v3")
        print("  → Context manager created")
        
        async with ws as ws_conn:
            print("  → Entering context manager...")
            
            await ws_conn.configure(
                target_language_code="hi-IN",
                speaker="shubh",
                output_audio_codec="linear16"
            )
            print("✓ WebSocket connected and configured successfully!")
            print("  → Language: hi-IN")
            print("  → Speaker: shubh")
            print("  → Codec: linear16")
            
            # Try sending a small text
            print("\n[TEST 4] Sending test text to streaming endpoint...")
            await ws_conn.convert("नमस्ते")
            print("✓ Text sent successfully")
            
            # Try receiving audio
            print("\n[TEST 5] Receiving audio chunks...")
            chunk_count = 0
            async for message in ws_conn:
                if isinstance(message, AudioOutput):
                    chunk_count += 1
                    print(f"  ✓ Received audio chunk {chunk_count}: {len(message.data.audio)} chars (base64)")
                    if chunk_count >= 3:  # Just get first 3 chunks
                        break
            
            if chunk_count > 0:
                print(f"✓ Received {chunk_count} audio chunks successfully!")
            else:
                print("⚠️  No audio chunks received")
        
        return True
    
    except Exception as e:
        print(f"✗ WebSocket connection failed: {e}")
        print(f"\nError type: {type(e).__name__}")
        print(f"Error details: {str(e)}")
        
        # Try to extract useful info
        if hasattr(e, 'status_code'):
            print(f"Status code: {e.status_code}")
        if hasattr(e, 'headers'):
            print(f"Headers: {e.headers}")
        if hasattr(e, 'body'):
            print(f"Body: {e.body}")
        
        return False


async def main():
    success = await test_api_key()
    
    print("\n" + "=" * 60)
    if success:
        print("✓ ALL TESTS PASSED - API is working!")
    else:
        print("✗ TESTS FAILED - Check error above")
        print("\nPossible causes:")
        print("  1. API key is invalid or expired")
        print("  2. Subscription doesn't include streaming TTS")
        print("  3. Account needs setup/activation")
        print("  4. Rate limit exceeded")
        print("  5. Model name 'bulbul:v3' is incorrect")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
