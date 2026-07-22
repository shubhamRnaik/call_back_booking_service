"""
Test different Sarvam models and configurations to find working streaming endpoint.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from indic_tts_runtime.config import settings
from sarvamai import AsyncSarvamAI

async def test_model(model_name: str):
    """Test a specific model."""
    print(f"\n  Testing model: '{model_name}'")
    api_key = settings.sarvam_api_key
    
    try:
        client = AsyncSarvamAI(api_subscription_key=api_key)
        ws = client.text_to_speech_streaming.connect(model=model_name)
        
        async with ws as ws_conn:
            await ws_conn.configure(
                target_language_code="hi-IN",
                speaker="shubh",
                output_audio_codec="linear16"
            )
            print(f"    ✓ Connected to '{model_name}'!")
            return True
    
    except Exception as e:
        error_msg = str(e)
        if "403" in error_msg:
            print(f"    ✗ 403 Forbidden")
        elif "404" in error_msg:
            print(f"    ✗ 404 Not Found")
        else:
            print(f"    ✗ Error: {error_msg[:80]}...")
        return False


async def main():
    print("=" * 60)
    print("SARVAM MODEL DISCOVERY TEST")
    print("=" * 60)
    
    # Different model name variations to test
    models_to_test = [
        "bulbul:v3",           # Original attempt
        "bulbul:v2",           # Earlier version
        "bulbul",              # Without version
        "bulbul:latest",       # Latest tag
        "tts_bulbul_v3",       # Alternative naming
        "bulbul_v3",           # Underscore variant
        "bulbul:v3:en",        # With language
        "bulbul-v3",           # Hyphenated
        "sarvam-tts",          # Generic TTS name
        "tts",                 # Simple TTS
    ]
    
    print(f"\nAPI Key: {settings.sarvam_api_key[:20]}...{settings.sarvam_api_key[-10:]}")
    print(f"Trying {len(models_to_test)} model variations...\n")
    
    results = {}
    for model in models_to_test:
        success = await test_model(model)
        results[model] = success
    
    print("\n" + "=" * 60)
    print("RESULTS:")
    print("=" * 60)
    
    working = [m for m, s in results.items() if s]
    failed = [m for m, s in results.items() if not s]
    
    if working:
        print(f"\n✓ WORKING MODELS ({len(working)}):")
        for model in working:
            print(f"  • {model}")
    else:
        print("\n✗ No models worked")
    
    print(f"\n✗ FAILED MODELS ({len(failed)}):")
    for model in failed[:5]:  # Show first 5
        print(f"  • {model}")
    if len(failed) > 5:
        print(f"  ... and {len(failed) - 5} more")
    
    print("\n" + "=" * 60)
    
    if working:
        print(f"\n✓ SUCCESS! Use model: '{working[0]}'")
        return True
    else:
        print("\n✗ FAILURE: No working models found")
        print("\nPossible issues:")
        print("  1. API key not authorized for streaming TTS")
        print("  2. Account/subscription needs activation")
        print("  3. Region/geographic restriction")
        print("  4. Different API endpoint required")
        print("\nNext steps:")
        print("  • Check Sarvam dashboard: https://console.sarvam.ai")
        print("  • Verify streaming TTS is enabled")
        print("  • Check API key permissions")
        print("  • Contact Sarvam support")
        return False


if __name__ == "__main__":
    asyncio.run(main())
