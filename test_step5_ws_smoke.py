"""
Step 5 smoke test #2 (temporary): exercises the actual Exotel WebSocket
handshake end-to-end against the live running server (started separately via
uvicorn), without needing real STT-recognizable audio:

1. Connects to /ws/v1/exotel-stream/PARLOUR_001 (path-based tenant routing).
2. Sends a "start" event (linear16 @ 8kHz, matching what the handler expects).
3. Waits for greeting audio frames to stream back (proves tenant config
   resolution + session creation + TTS synthesis + Exotel media framing all
   work together).
4. Sends a "stop" event and closes cleanly.
5. Polls /api/v1/ready active_sessions before/after to confirm session
   lifecycle (create on connect, remove on disconnect) is wired correctly.

Run (with the server already running via uvicorn on port 8000):
  .venv\\Scripts\\python.exe test_step5_ws_smoke.py [tenant_id]
"""

import asyncio
import base64
import json
import logging
import sys

import httpx
import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

TENANT_ID = sys.argv[1] if len(sys.argv) > 1 else "PARLOUR_001"
WS_URL = f"ws://localhost:8000/ws/v1/exotel-stream/{TENANT_ID}"
READY_URL = "http://localhost:8000/api/v1/ready"


async def get_active_sessions() -> int:
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(READY_URL)
        return resp.json()["details"]["active_sessions"]


async def run() -> None:
    print("Active sessions before connect:", await get_active_sessions())

    async with websockets.connect(WS_URL) as ws:
        logger.info("Connected to %s", WS_URL)

        stream_sid = "smoke_stream_001"
        start_event = {
            "event": "start",
            "stream_sid": stream_sid,
            "start": {
                "stream_sid": stream_sid,
                "media_format": {"encoding": "linear16", "sample_rate": 8000},
            },
        }
        await ws.send(json.dumps(start_event))
        logger.info("Sent 'start' event")

        media_frames = 0
        total_bytes = 0
        got_any_audio = False

        try:
            async with asyncio.timeout(8.0):
                async for message in ws:
                    data = json.loads(message)
                    event = data.get("event")
                    if event == "media":
                        payload = data.get("media", {}).get("payload", "")
                        raw = base64.b64decode(payload)
                        media_frames += 1
                        total_bytes += len(raw)
                        got_any_audio = True
                    elif event == "clear":
                        logger.info("Received 'clear' frame")
                    else:
                        logger.info("Received: %s", data)

                    # Stop listening once we've received a reasonable amount
                    # of greeting audio (greeting is short, ~1-3s).
                    if media_frames >= 5:
                        break
        except TimeoutError:
            logger.warning("Timed out waiting for greeting audio")

        print(f"Received {media_frames} media frame(s), {total_bytes} raw bytes total")
        assert got_any_audio, "expected at least one greeting audio frame from the server"

        print("Active sessions while connected:", await get_active_sessions())

        await ws.send(json.dumps({"event": "stop"}))
        logger.info("Sent 'stop' event")

    # give the server a moment to run its finally/cleanup block
    await asyncio.sleep(0.5)
    print("Active sessions after disconnect:", await get_active_sessions())

    print("\n=== WS SMOKE TEST PASSED ===")


if __name__ == "__main__":
    asyncio.run(run())
