"""Diagnostic script to test Sarvam WebSocket connection directly."""

import asyncio
import json
import logging
from indic_tts_runtime.services.sarvam_service import SarvamWebSocketClient
from indic_tts_runtime.config import settings

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_sarvam_connection():
    """Test direct connection to Sarvam API."""
    
    logger.info("=" * 60)
    logger.info("SARVAM CONNECTION DIAGNOSTIC")
    logger.info("=" * 60)
    
    # Check API key
    try:
        api_key = settings.sarvam_api_key
        if api_key.startswith("demo_") or api_key == "k_2f1kjzhf_QKr0O0hT8do8xBIPSnsqpm6H":
            logger.warning(f"⚠️  Using demo/placeholder API key: {api_key}")
            logger.info("→ Get real key from: https://console.sarvam.ai")
        else:
            logger.info(f"✓ API key configured: {api_key[:20]}...")
    except Exception as e:
        logger.error(f"✗ Failed to load API key: {e}")
        return
    
    # Try to create and connect client
    logger.info("\n📡 Attempting Sarvam WebSocket connection...")
    
    try:
        client = SarvamWebSocketClient()
        logger.info(f"✓ SarvamWebSocketClient created")
        logger.info(f"  URL: {client.WS_URL}")
        logger.info(f"  Max sessions: {client.MAX_CONCURRENT_SESSIONS}")
        
        # Try to connect
        logger.info("\n🔗 Connecting...")
        success = await client.connect(
            target_language_code="hi-IN",
            speaker="shubh",
            pace=0.95
        )
        
        if success:
            logger.info("✓ Connected to Sarvam!")
            
            # Try to send text
            logger.info("\n📝 Sending test text...")
            text_success = await client.send_text_chunk("नमस्ते")
            if text_success:
                logger.info("✓ Text sent successfully")
            else:
                logger.error("✗ Failed to send text")
            
            # Check stats
            stats = client.get_connection_stats()
            logger.info(f"\n📊 Connection stats:")
            for key, value in stats.items():
                logger.info(f"  {key}: {value}")
            
            # Disconnect
            await client.disconnect()
            logger.info("\n✓ Disconnected cleanly")
        else:
            logger.error("✗ Failed to connect to Sarvam")
            logger.info("→ Check your API key and network connectivity")
            
    except Exception as e:
        logger.error(f"✗ Error: {e}")
        import traceback
        logger.debug(traceback.format_exc())

if __name__ == "__main__":
    asyncio.run(test_sarvam_connection())
