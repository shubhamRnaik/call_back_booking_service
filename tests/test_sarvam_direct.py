"""Direct Sarvam WebSocket connection test - minimal diagnostic."""

import asyncio
import websockets
import json
import logging

logging.basicConfig(level=logging.DEBUG, format='%(message)s')
logger = logging.getLogger(__name__)

async def test_sarvam_direct():
    """Test direct WebSocket connection to Sarvam."""
    
    # Your API key
    api_key = "k_2f1kjzhf_QKr0O0hT8do8xBIPSnsqpm6H"
    
    # Sarvam WebSocket URL
    base_url = "wss://api.sarvam.ai/text-to-speech/ws?model=bulbul:v3"
    
    # Try 3 different authentication approaches
    attempts = [
        {
            "name": "API key as URL parameter",
            "url": f"{base_url}&api-subscription-key={api_key}",
            "headers": None
        },
        {
            "name": "API key as extra_headers",
            "url": base_url,
            "headers": {"api-subscription-key": api_key}
        },
        {
            "name": "API key as Authorization header",
            "url": base_url,
            "headers": {"Authorization": f"Bearer {api_key}"}
        }
    ]
    
    for attempt in attempts:
        logger.info(f"\n{'='*60}")
        logger.info(f"Attempt: {attempt['name']}")
        logger.info(f"URL: {attempt['url']}")
        if attempt['headers']:
            logger.info(f"Headers: {attempt['headers']}")
        logger.info(f"{'='*60}")
        
        try:
            if attempt['headers']:
                ws = await websockets.connect(
                    attempt['url'],
                    extra_headers=attempt['headers'],
                    ping_interval=20,
                    ping_timeout=10
                )
            else:
                ws = await websockets.connect(
                    attempt['url'],
                    ping_interval=20,
                    ping_timeout=10
                )
            
            logger.info("✓ Connected successfully!")
            
            # Try sending config
            config = {
                "type": "config",
                "data": {
                    "target_language_code": "hi-IN",
                    "speaker": "shubh",
                    "pace": 0.95,
                    "speech_sample_rate": 8000,
                    "output_audio_codec": "linear16"
                }
            }
            
            await ws.send(json.dumps(config))
            logger.info("✓ Config sent")
            
            # Send test text
            text_msg = {
                "type": "text",
                "data": {"text": "नमस्ते"}
            }
            
            await ws.send(json.dumps(text_msg))
            logger.info("✓ Text sent")
            
            # Receive first message
            response = await asyncio.wait_for(ws.recv(), timeout=5)
            msg = json.loads(response)
            logger.info(f"✓ Received: {msg['type']}")
            
            await ws.close()
            logger.info("\n✓ SUCCESS - This authentication method works!")
            return True
            
        except asyncio.TimeoutError:
            logger.warning("✗ Timeout - no response from server")
        except websockets.exceptions.InvalidStatusException as e:
            logger.error(f"✗ Invalid status: {e.status} {e.headers}")
        except Exception as e:
            logger.error(f"✗ Error: {type(e).__name__}: {e}")
    
    logger.info(f"\n{'='*60}")
    logger.error("✗ All attempts failed. Check:")
    logger.error("  1. API key validity: https://console.sarvam.ai")
    logger.error("  2. Network connectivity to api.sarvam.ai")
    logger.error("  3. Sarvam API documentation for current auth method")
    logger.info(f"{'='*60}\n")

if __name__ == "__main__":
    asyncio.run(test_sarvam_direct())
