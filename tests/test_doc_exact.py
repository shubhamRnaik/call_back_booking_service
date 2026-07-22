"""
Test using EXACT code from Sarvam official documentation.
This should reveal if the issue is with our parameters or the API key itself.
"""

import asyncio
import base64
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))

from indic_tts_runtime.config import settings
from sarvamai import AsyncSarvamAI, AudioOutput


async def test_exact_doc_code():
    """Run the exact example from Sarvam docs."""
    print("=" * 70)
    print("TESTING EXACT CODE FROM SARVAM DOCUMENTATION")
    print("=" * 70)
    
    api_key = settings.sarvam_api_key
    print(f"\nUsing API Key: {api_key[:20]}...{api_key[-10:]}\n")
    
    try:
        # EXACT code from Sarvam docs
        client = AsyncSarvamAI(api_subscription_key=api_key)
        
        async with client.text_to_speech_streaming.connect(model="bulbul:v3") as ws:
            print("✓ WebSocket connected!")
            
            await ws.configure(target_language_code="hi-IN", speaker="shubh")
            print("✓ Configuration sent")
            
            long_text = (
                "भारत की संस्कृति विश्व की सबसे प्राचीन और समृद्ध संस्कृतियों में से एक है।"
                "यह विविधता, सहिष्णुता और परंपराओं का अद्भुत संगम है, "
                "जिसमें विभिन्न धर्म, भाषाएं, त्योहार, संगीत, नृत्य, वास्तुकला और जीवनशैली शामिल हैं।"
            )
            
            await ws.convert(long_text)
            print("✓ Text sent to convert")
            
            await ws.flush()
            print("✓ Flushed buffer")
            
            # Receive audio
            chunk_count = 0
            total_bytes = 0
            output_file = Path(__file__).parent / "test_doc_code.wav"
            
            print(f"\nReceiving audio chunks...")
            
            with open(output_file, "wb") as f:
                try:
                    while True:
                        message = await asyncio.wait_for(ws.recv(), timeout=3.0)
                        
                        if isinstance(message, AudioOutput):
                            chunk_count += 1
                            audio_bytes = base64.b64decode(message.data.audio)
                            total_bytes += len(audio_bytes)
                            f.write(audio_bytes)
                            f.flush()
                            print(f"  ✓ Chunk {chunk_count}: {len(audio_bytes)} bytes")
                
                except asyncio.TimeoutError:
                    print("\n  ✓ Stream complete (timeout reached)")
            
            print(f"\n✓ SUCCESS!")
            print(f"  • Total chunks: {chunk_count}")
            print(f"  • Total bytes: {total_bytes}")
            print(f"  • Output file: {output_file}")
            
            return True
    
    except Exception as e:
        print(f"\n✗ FAILED")
        print(f"Error: {e}")
        print(f"\nError type: {type(e).__name__}")
        
        # Try to extract status code
        error_str = str(e)
        if "403" in error_str:
            print("\n⚠️  HTTP 403 Forbidden")
            print("\nPossible causes:")
            print("  1. API key is invalid or revoked")
            print("  2. Account doesn't have streaming TTS enabled")
            print("  3. Regional restrictions on your account")
            print("  4. Quota exceeded")
            print("\nNext steps:")
            print("  • Log into https://dashboard.sarvam.ai/")
            print("  • Check if streaming TTS is enabled")
            print("  • Verify API key is active")
            print("  • Check usage/quota limits")
            print("  • Contact Sarvam support if needed")
        elif "404" in error_str:
            print("\n⚠️  HTTP 404 Not Found")
            print("  → The model or endpoint doesn't exist")
            print("  → Try different model names")
        elif "401" in error_str:
            print("\n⚠️  HTTP 401 Unauthorized")
            print("  → API key is missing or invalid")
        
        return False


if __name__ == "__main__":
    success = asyncio.run(test_exact_doc_code())
    print("\n" + "=" * 70)
    if not success:
        print("⚠️  API key or account issue detected")
        print("\nOPTIONS:")
        print("1. Generate a new API key from https://dashboard.sarvam.ai/")
        print("2. Verify account status and streaming TTS subscription")
        print("3. Check Sarvam documentation or contact support")
    print("=" * 70)
