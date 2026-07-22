"""
Test with ALL configuration parameters from Sarvam documentation.
Some parameters might be required for streaming to work.
"""

import asyncio
import base64
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))

from indic_tts_runtime.config import settings
from sarvamai import AsyncSarvamAI, AudioOutput


async def test_with_full_config():
    """Test with all available config parameters from docs."""
    print("=" * 70)
    print("TESTING WITH FULL CONFIGURATION PARAMETERS")
    print("=" * 70)
    
    api_key = settings.sarvam_api_key
    print(f"\nUsing API Key: {api_key[:20]}...{api_key[-10:]}\n")
    
    try:
        client = AsyncSarvamAI(api_subscription_key=api_key)
        
        async with client.text_to_speech_streaming.connect(model="bulbul:v3") as ws:
            print("✓ WebSocket connected!")
            
            # Try with full config parameters mentioned in docs
            print("\nSending full configuration...")
            config_params = {
                "target_language_code": "hi-IN",
                "speaker": "shubh",
                "pace": 0.95,
                "min_buffer_size": 50,      # From docs
                "max_chunk_length": 200,    # From docs
                "output_audio_codec": "linear16",
                "output_audio_bitrate": "128k"  # From docs
            }
            
            print(f"Config: {config_params}")
            await ws.configure(**config_params)
            print("✓ Full configuration sent")
            
            # Send test text
            test_text = "नमस्ते, यह एक परीक्षण है।"
            await ws.convert(test_text)
            print(f"✓ Text sent: {test_text}")
            
            await ws.flush()
            print("✓ Buffer flushed")
            
            # Receive audio
            chunk_count = 0
            total_bytes = 0
            
            print(f"\nReceiving audio...")
            
            try:
                async for message in ws:
                    if isinstance(message, AudioOutput):
                        chunk_count += 1
                        audio_bytes = base64.b64decode(message.data.audio)
                        total_bytes += len(audio_bytes)
                        print(f"  ✓ Chunk {chunk_count}: {len(audio_bytes)} bytes")
                    else:
                        print(f"  Other message: {type(message).__name__}")
            
            except asyncio.TimeoutError:
                pass
            
            print(f"\n✓ SUCCESS!")
            print(f"  • Total chunks: {chunk_count}")
            print(f"  • Total bytes: {total_bytes}")
            
            return True
    
    except TypeError as e:
        print(f"\n✗ TypeError (parameter issue)")
        print(f"Error: {e}")
        print("\nThis might mean the configure() method doesn't accept these parameters.")
        print("Try removing: min_buffer_size, max_chunk_length, output_audio_bitrate")
        return False
    
    except Exception as e:
        print(f"\n✗ FAILED")
        print(f"Error: {e}")
        return False


if __name__ == "__main__":
    asyncio.run(test_with_full_config())
