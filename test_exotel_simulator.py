"""
Local Exotel Stream Simulator:
Simulates Exotel Cloud Telephony WebSocket client locally without needing external tunnels or Exotel credentials.
Connects to ws://localhost:8000/ws/v1/exotel-stream?tenant_id=PARLOUR_001,
sends start/media/stop frames, and prints received AI response audio metrics.
"""

import asyncio
import json
import base64
import logging
import numpy as np
import websockets

from indic_tts_runtime.core.telephony_audio import pcm16_to_mulaw, mulaw_to_pcm16

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

WS_URL = "ws://localhost:8000/ws/v1/exotel-stream?tenant_id=PARLOUR_001"

async def run_simulator():
    logger.info(f"Connecting to local Exotel WebSocket: {WS_URL}")
    try:
        async with websockets.connect(WS_URL) as ws:
            logger.info("✅ Connected to Exotel endpoint!")

            # 1. Send 'start' event
            stream_sid = "sim_stream_999"
            start_event = {
                "event": "start",
                "stream_sid": stream_sid,
                "call_id": "sim_call_999",
                "start": {
                    "stream_sid": stream_sid,
                    "call_id": "sim_call_999",
                    "media_format": {
                        "encoding": "mulaw",
                        "sample_rate": 8000
                    }
                }
            }
            await ws.send(json.dumps(start_event))
            logger.info("📤 Sent 'start' event")

            # Receiver task to listen for Exotel media / clear frames from server
            total_audio_bytes = 0
            frames_received = 0

            async def listen_responses():
                nonlocal total_audio_bytes, frames_received
                try:
                    async for message in ws:
                        data = json.loads(message)
                        event = data.get("event")
                        if event == "media":
                            payload = data.get("media", {}).get("payload", "")
                            raw_mulaw = base64.b64decode(payload)
                            pcm16 = mulaw_to_pcm16(raw_mulaw)
                            total_audio_bytes += len(pcm16)
                            frames_received += 1
                            logger.info(f"📥 Received AI media frame #{frames_received} ({len(raw_mulaw)} mu-law bytes -> {len(pcm16)} PCM16 bytes)")
                        elif event == "clear":
                            logger.info("🔴 Received Exotel 'clear' frame (barge-in signal)")
                        else:
                            logger.info(f"💬 Received message: {data}")
                except websockets.exceptions.ConnectionClosed:
                    logger.info("WebSocket connection closed")

            listener_task = asyncio.create_task(listen_responses())

            # 2. Wait 2 seconds for greeting audio
            await asyncio.sleep(2.0)

            # 3. Simulate sending 1 second of audio (8kHz sine wave converted to mu-law)
            logger.info("🎙️ Simulating user speaking (1 second 8kHz audio frame)...")
            t = np.linspace(0, 1, 8000, False)
            sine_wave = (np.sin(2 * np.pi * 440 * t) * 10000).astype(np.int16)
            mulaw_bytes = pcm16_to_mulaw(sine_wave.tobytes())
            b64_payload = base64.b64encode(mulaw_bytes).decode("ascii")

            # Send in 20ms chunks (160 bytes per chunk at 8kHz mu-law)
            chunk_size = 160
            for i in range(0, len(mulaw_bytes), chunk_size):
                chunk = mulaw_bytes[i:i+chunk_size]
                chunk_b64 = base64.b64encode(chunk).decode("ascii")
                media_event = {
                    "event": "media",
                    "stream_sid": stream_sid,
                    "media": {
                        "payload": chunk_b64
                    }
                }
                await ws.send(json.dumps(media_event))
                await asyncio.sleep(0.02) # 20ms pacing

            logger.info("✓ Simulated audio sent. Waiting 3 seconds for AI response...")
            await asyncio.sleep(3.0)

            # 4. Send 'stop' event
            stop_event = {
                "event": "stop",
                "stream_sid": stream_sid
            }
            await ws.send(json.dumps(stop_event))
            logger.info("📤 Sent 'stop' event")

            await asyncio.sleep(0.5)
            listener_task.cancel()
            logger.info(f"🏁 Simulation complete! Received {frames_received} audio frames ({total_audio_bytes} total PCM bytes).")

    except Exception as e:
        logger.error(f"❌ Simulator error: {e}")

if __name__ == "__main__":
    asyncio.run(run_simulator())
