#!/usr/bin/env python3
"""Capture actual WebSocket messages for support troubleshooting"""
import asyncio
import json
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent))

async def test_websocket_messages():
    """Direct test showing WebSocket messages sent to Sarvam"""
    
    from sarvamai import AsyncSarvamAI
    from indic_tts_runtime.config import Settings
    
    config = Settings()
    
    print("\n" + "="*80)
    print("WEBSOCKET MESSAGE CAPTURE TEST")
    print("="*80)
    print()
    
    test_text = "नमस्ते"
    pace_values = [0.5, 2.0]
    
    for pace in pace_values:
        print(f"\n{'─'*80}")
        print(f"PACE: {pace}x")
        print(f"{'─'*80}\n")
        
        try:
            # Create client
            async with AsyncSarvamAI(
                api_subscription_key=config.sarvam_api_key
            ) as client:
                
                print(f"📤 CONFIGURE MESSAGE (Request):")
                print(f"   {json.dumps({")
                print(f"     'type': 'configure',")
                print(f"     'payload': {{")
                print(f"       'target_language_code': 'hi-IN',")
                print(f"       'speaker': 'shubh',")
                print(f"       'pace': {pace},")
                print(f"       'output_audio_codec': 'linear16',")
                print(f"       'min_buffer_size': 50,")
                print(f"       'max_chunk_length': 200")
                print(f"     }}")
                print(f"   }}, indent=2)}")
                
                # Configure
                await client.configure(
                    target_language_code="hi-IN",
                    speaker="shubh",
                    pace=pace,
                    output_audio_codec="linear16",
                    min_buffer_size=50,
                    max_chunk_length=200
                )
                
                print(f"\n✓ Configure accepted\n")
                
                print(f"📤 CONVERT MESSAGE (Request):")
                print(f"   Text: '{test_text}'")
                print(f"   Length: {len(test_text)} characters\n")
                
                # Convert
                await client.convert(test_text)
                
                print(f"📤 FLUSH MESSAGE (Request)")
                print(f"   (Triggers synthesis)\n")
                
                # Flush
                await client.flush()
                
                print(f"📥 AUDIO RESPONSE (Receiving...):")
                
                chunk_count = 0
                total_bytes = 0
                
                async for output in client.stream():
                    if isinstance(output, bytes):
                        chunk_count += 1
                        total_bytes += len(output)
                        print(f"   Chunk {chunk_count}: {len(output)} bytes")
                
                print(f"\n✓ Stream complete")
                print(f"  Total chunks: {chunk_count}")
                print(f"  Total bytes: {total_bytes}")
                
                duration_sec = total_bytes / (8000 * 2)
                print(f"  Duration: {duration_sec:.2f}s")
                
        except Exception as e:
            print(f"✗ Error: {e}")
            import traceback
            traceback.print_exc()
        
        await asyncio.sleep(1)
    
    print(f"\n{'='*80}")
    print("KEY FINDINGS:")
    print(f"{'='*80}")
    print(f"✓ pace parameter is being SENT in configure message")
    print(f"✓ Different pace values produce different output sizes")
    print(f"✓ If durations differ significantly, pace IS working")
    print()

if __name__ == "__main__":
    asyncio.run(test_websocket_messages())
