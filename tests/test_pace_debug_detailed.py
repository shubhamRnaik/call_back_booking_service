#!/usr/bin/env python3
"""Detailed debug script with full request/response logging for support"""
import asyncio
import json
import sys
import logging
from pathlib import Path
from datetime import datetime

# Add package to path
sys.path.insert(0, str(Path(__file__).parent))

from indic_tts_runtime.services.sarvam_service import SarvamWebSocketClient
from indic_tts_runtime.config import Settings

# Setup detailed logging
log_file = Path("test_outputs") / f"pace_debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_file.parent.mkdir(exist_ok=True)

# Configure logging to both file and console
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Enable debug logging for sarvamai library
logging.getLogger('sarvamai').setLevel(logging.DEBUG)
logging.getLogger('asyncio').setLevel(logging.WARNING)

async def test_with_full_logging():
    """Test with comprehensive logging"""
    
    logger.info("="*80)
    logger.info("PACE PARAMETER DEBUG TEST - FULL LOGGING")
    logger.info("="*80)
    
    config = Settings()
    logger.info(f"✓ Config loaded:")
    logger.info(f"  API Key: {config.sarvam_api_key[:20]}...{config.sarvam_api_key[-10:]}")
    logger.info(f"  Audio Codec: {config.default_audio_codec}")
    logger.info(f"  Sample Rate: {config.default_sample_rate}")
    
    test_text = "नमस्ते स्वागत है आपको"
    test_cases = [
        {"pace": 0.5, "speaker": "shubh", "language": "hi-IN"},
        {"pace": 2.0, "speaker": "shubh", "language": "hi-IN"},
    ]
    
    results = []
    
    for idx, test_case in enumerate(test_cases, 1):
        pace = test_case["pace"]
        speaker = test_case["speaker"]
        language = test_case["language"]
        
        logger.info("")
        logger.info("="*80)
        logger.info(f"TEST CASE {idx}: Pace={pace}x, Speaker={speaker}, Language={language}")
        logger.info("="*80)
        
        client = SarvamWebSocketClient()
        start_time = datetime.now()
        
        try:
            # Log connection parameters
            logger.info(f"→ Initiating connection with parameters:")
            logger.info(f"  target_language_code={language}")
            logger.info(f"  speaker={speaker}")
            logger.info(f"  pace={pace}")
            logger.info(f"  output_audio_codec=linear16")
            logger.info(f"  min_buffer_size=50")
            logger.info(f"  max_chunk_length=200")
            
            # Connect
            connect_start = datetime.now()
            success = await client.connect(
                target_language_code=language,
                speaker=speaker,
                pace=pace
            )
            connect_time = (datetime.now() - connect_start).total_seconds() * 1000
            
            if not success:
                logger.error(f"✗ Connection failed!")
                continue
            
            logger.info(f"✓ Connection successful (took {connect_time:.2f}ms)")
            logger.info(f"  Connection state: {client._connected}")
            logger.info(f"  Connection object type: {type(client._ws_connection)}")
            
            # Log text being sent
            logger.info(f"→ Sending text: '{test_text}'")
            logger.info(f"  Text length: {len(test_text)} characters")
            
            send_start = datetime.now()
            await client.send_text_chunk(test_text)
            logger.info(f"✓ Text sent (took {(datetime.now() - send_start).total_seconds() * 1000:.2f}ms)")
            
            # Send flush
            flush_start = datetime.now()
            await client.send_flush()
            logger.info(f"✓ Flush signal sent (took {(datetime.now() - flush_start).total_seconds() * 1000:.2f}ms)")
            
            # Stream audio and log chunks
            logger.info(f"📡 Streaming audio chunks...")
            chunk_count = 0
            total_bytes = 0
            chunk_times = []
            chunk_start = datetime.now()
            
            async for audio_bytes in client.stream_audio_chunks():
                chunk_count += 1
                total_bytes += len(audio_bytes)
                chunk_time = (datetime.now() - chunk_start).total_seconds() * 1000
                chunk_times.append(chunk_time)
                
                logger.debug(f"  Chunk {chunk_count}: {len(audio_bytes)} bytes (arrived after {chunk_time:.2f}ms)")
                
                chunk_start = datetime.now()
            
            logger.info(f"✓ Audio streaming complete!")
            logger.info(f"  Total chunks: {chunk_count}")
            logger.info(f"  Total bytes: {total_bytes}")
            
            # Calculate statistics
            if chunk_times:
                avg_chunk_time = sum(chunk_times) / len(chunk_times)
                first_chunk_time = chunk_times[0]
                logger.info(f"  First chunk TTFB: {first_chunk_time:.2f}ms")
                logger.info(f"  Avg chunk interval: {avg_chunk_time:.2f}ms")
            
            # Calculate duration
            # FIXED: Changed from 8000 Hz to 22050 Hz (Sarvam's actual output)
            sample_rate = 22050
            sample_width = 2
            duration_sec = total_bytes / (sample_rate * sample_width)
            
            logger.info(f"  Audio duration: {duration_sec:.2f}s")
            logger.info(f"  Audio size: {total_bytes / 1024:.1f} KB")
            
            # Disconnect
            logger.info(f"→ Disconnecting...")
            await client.disconnect()
            logger.info(f"✓ Disconnected")
            
            total_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"✓ Total test time: {total_time:.2f}s")
            
            results.append({
                "pace": pace,
                "chunks": chunk_count,
                "bytes": total_bytes,
                "duration": duration_sec,
                "total_time": total_time
            })
            
        except Exception as e:
            logger.error(f"✗ ERROR: {e}", exc_info=True)
        
        await asyncio.sleep(1)
    
    # Final analysis
    logger.info("")
    logger.info("="*80)
    logger.info("FINAL ANALYSIS")
    logger.info("="*80)
    
    if len(results) >= 2:
        r1 = results[0]
        r2 = results[1]
        
        logger.info(f"Pace 0.5x: {r1['duration']:.2f}s duration, {r1['chunks']} chunks, {r1['bytes']} bytes")
        logger.info(f"Pace 2.0x: {r2['duration']:.2f}s duration, {r2['chunks']} chunks, {r2['bytes']} bytes")
        
        duration_ratio = r1['duration'] / r2['duration']
        chunk_ratio = r1['chunks'] / r2['chunks']
        byte_ratio = r1['bytes'] / r2['bytes']
        
        logger.info(f"")
        logger.info(f"Duration ratio (0.5x / 2.0x): {duration_ratio:.2f}x")
        logger.info(f"Chunk ratio (0.5x / 2.0x): {chunk_ratio:.2f}x")
        logger.info(f"Byte ratio (0.5x / 2.0x): {byte_ratio:.2f}x")
        
        if duration_ratio > 4.0:
            logger.info(f"")
            logger.info(f"✅ CONCLUSION: PACE PARAMETER IS WORKING CORRECTLY")
            logger.info(f"   - 0.5x produces {duration_ratio:.1f}x longer audio than 2.0x")
            logger.info(f"   - This is expected behavior")
            logger.info(f"   - Voice perceived as 'robotic' is likely Sarvam's natural voice character")
        else:
            logger.info(f"")
            logger.info(f"⚠️ CONCLUSION: Pace may not be working as expected")
    
    logger.info("")
    logger.info("="*80)
    logger.info(f"LOG FILE SAVED: {log_file}")
    logger.info("="*80)
    logger.info("")

if __name__ == "__main__":
    asyncio.run(test_with_full_logging())
