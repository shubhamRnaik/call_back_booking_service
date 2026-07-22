"""
Diagnostic script to test Sarvam AI API connectivity.
Helps identify authentication and network issues.
"""

import asyncio
import aiohttp
import sys
import os
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent))

from indic_tts_runtime.config import settings


async def diagnose_sarvam_api():
    """Run comprehensive Sarvam API diagnostics."""
    
    print("=" * 80)
    print("SARVAM AI API DIAGNOSTICS")
    print("=" * 80 + "\n")
    
    # Test 1: Configuration Check
    print("1. Configuration Check")
    print("-" * 80)
    print(f"✓ API Key: {settings.sarvam_api_key[:10]}...{settings.sarvam_api_key[-10:]}")
    print(f"✓ API URL: {settings.sarvam_api_url}")
    print(f"✓ Sample Rate: {settings.default_sample_rate} Hz")
    print(f"✓ Codec: {settings.default_audio_codec}\n")
    
    # Test 2: Network Connectivity
    print("2. Network Connectivity Test")
    print("-" * 80)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://api.sarvam.ai/text-to-speech",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                print(f"✓ Can reach api.sarvam.ai (status: {response.status})\n")
    except asyncio.TimeoutError:
        print("✗ Timeout reaching api.sarvam.ai\n")
    except Exception as e:
        print(f"✗ Cannot reach api.sarvam.ai: {e}\n")
    
    # Test 3: Simple GET Request
    print("3. Simple API GET Request")
    print("-" * 80)
    try:
        async with aiohttp.ClientSession() as session:
            headers = {
                "Authorization": f"Bearer {settings.sarvam_api_key}",
                "Content-Type": "application/json"
            }
            
            async with session.get(
                settings.sarvam_api_url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                text = await response.text()
                print(f"Status: {response.status}")
                print(f"Response: {text[:200]}\n")
                
    except Exception as e:
        print(f"✗ Error: {e}\n")
    
    # Test 4: Full TTS Request
    print("4. Full TTS Synthesis Request")
    print("-" * 80)
    
    payload = {
        "text": "Namaste, aap kaisa ho?",
        "language": "hi-IN",
        "speaker": "shubh",
        "pace": 0.95,
        "audio_format": "wav",
        "sample_rate": 8000,
        "codec": "linear16"
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            headers = {
                "Authorization": f"Bearer {settings.sarvam_api_key}",
                "Content-Type": "application/json"
            }
            
            print(f"Sending request to: {settings.sarvam_api_url}")
            print(f"Payload: {payload}\n")
            
            async with session.post(
                settings.sarvam_api_url,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30),
                ssl=True
            ) as response:
                print(f"Status: {response.status}")
                print(f"Content-Type: {response.headers.get('Content-Type')}")
                
                if response.status == 200:
                    audio_data = await response.read()
                    print(f"✓ SUCCESS! Received {len(audio_data)} bytes of audio data\n")
                else:
                    text = await response.text()
                    print(f"✗ API returned error: {text}\n")
                    
    except asyncio.TimeoutError:
        print("✗ Request timed out (30s)\n")
    except Exception as e:
        print(f"✗ Error: {type(e).__name__}: {e}\n")
    
    # Test 5: Recommendations
    print("5. Troubleshooting Recommendations")
    print("-" * 80)
    print("""
COMMON ISSUES & SOLUTIONS:

1. 401 Unauthorized Error:
   → API key is invalid or expired
   → Get a new API key from: https://console.sarvam.ai/api-keys
   → Update SARVAM_API_KEY in .env

2. 404 Not Found Error:
   → API endpoint URL might be wrong
   → Check: https://api.sarvam.ai/api/v1/text-to-speech
   → Or try: https://api.sarvam.ai/v1/text-to-speech

3. Timeout or Connection Refused:
   → Check your internet connection
   → Verify firewall/proxy settings
   → Try: curl https://api.sarvam.ai

4. SSL Certificate Error:
   → Disable SSL verification (NOT for production):
     In sarvam_service.py, change: ssl=True → ssl=False

5. Empty API Key:
   → Ensure SARVAM_API_KEY is set in .env
   → Format: sk_xxxxxxxxxxxx (should start with sk_)

ACTION ITEMS:
□ Verify API key is correct (copy-paste from Sarvam console)
□ Check internet connection
□ Try with curl: 
  curl -X POST https://api.sarvam.ai/api/v1/text-to-speech \\
    -H "Authorization: Bearer YOUR_KEY" \\
    -H "Content-Type: application/json" \\
    -d '{"text":"test","language":"hi-IN","speaker":"shubh"}'
□ Check if API endpoint URL changed
□ Contact Sarvam support if key is valid but still fails
    """)


def main():
    """Run diagnostics."""
    try:
        asyncio.run(diagnose_sarvam_api())
    except KeyboardInterrupt:
        print("\n✗ Diagnostics interrupted")
    except Exception as e:
        print(f"\n✗ Diagnostic error: {e}")


if __name__ == "__main__":
    main()
