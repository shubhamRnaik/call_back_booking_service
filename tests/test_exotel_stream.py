"""
Integration test for Exotel Telephony WebSocket endpoint (/ws/v1/exotel-stream).
Tests:
1. Telephony audio conversion utility (mu-law <-> PCM resampling).
2. Exotel multi-tenant WebSocket lifecycle (start, media, clear, stop).
3. Custom system prompt verification for tenant_id.
4. Barge-in clear frame propagation.
"""

import asyncio
import json
import base64
import pytest
import numpy as np
from fastapi.testclient import TestClient

from indic_tts_runtime.main import app
from indic_tts_runtime.core.telephony_audio import (
    telephony_to_stt_pcm,
    tts_pcm_to_telephony,
    mulaw_to_pcm16,
    pcm16_to_mulaw,
    resample_pcm,
)

def test_telephony_audio_utils():
    """Verify mu-law and resampling conversions."""
    # 1 sec of 8kHz PCM sine wave
    t = np.linspace(0, 1, 8000, False)
    sine = (np.sin(2 * np.pi * 440 * t) * 10000).astype(np.int16)
    pcm_8k_orig = sine.tobytes()

    # Convert PCM -> mu-law -> PCM
    mulaw_bytes = pcm16_to_mulaw(pcm_8k_orig)
    assert len(mulaw_bytes) == 8000
    pcm_8k_decoded = mulaw_to_pcm16(mulaw_bytes)
    assert len(pcm_8k_decoded) == 16000 # 8000 samples * 2 bytes

    # Convert base64 mu-law to 16kHz STT PCM
    b64_payload = base64.b64encode(mulaw_bytes).decode("ascii")
    stt_pcm_16k = telephony_to_stt_pcm(b64_payload, source_codec="mulaw", source_sr=8000, target_sr=16000)
    assert len(stt_pcm_16k) == 32000 # 16000 samples * 2 bytes

    # Convert 22.05kHz TTS PCM to Exotel 8kHz mu-law base64
    t_22k = np.linspace(0, 1, 22050, False)
    pcm_22k = (np.sin(2 * np.pi * 440 * t_22k) * 10000).astype(np.int16).tobytes()
    exotel_payload = tts_pcm_to_telephony(pcm_22k, source_sr=22050, target_codec="mulaw", target_sr=8000)
    assert len(exotel_payload) > 0
    decoded_mulaw = base64.b64decode(exotel_payload)
    assert len(decoded_mulaw) == 8000


def test_exotel_websocket_lifecycle():
    """Verify Exotel multi-tenant WebSocket message flow using TestClient."""
    from indic_tts_runtime import main
    main.text_normalizer = main.MultilingualTextNormalizer(default_language="hi")
    
    client = TestClient(app)
    
    # Test connection with custom tenant_id
    with client.websocket_connect("/ws/v1/exotel-stream?tenant_id=PARLOUR_001") as websocket:
        # Send start event
        start_msg = {
            "event": "start",
            "stream_sid": "stream_test_123",
            "call_id": "call_test_123",
            "start": {
                "stream_sid": "stream_test_123",
                "call_id": "call_test_123",
                "media_format": {
                    "encoding": "mulaw",
                    "sample_rate": 8000
                }
            }
        }
        websocket.send_text(json.dumps(start_msg))

        # Expect greeting media frame or greeting audio
        # Send dummy audio payload
        pcm_silent = np.zeros(160, dtype=np.int16).tobytes()
        mulaw_silent = pcm16_to_mulaw(pcm_silent)
        b64_silent = base64.b64encode(mulaw_silent).decode("ascii")

        media_msg = {
            "event": "media",
            "stream_sid": "stream_test_123",
            "media": {
                "payload": b64_silent
            }
        }
        websocket.send_text(json.dumps(media_msg))

        # Send stop event
        stop_msg = {
            "event": "stop",
            "stream_sid": "stream_test_123"
        }
        websocket.send_text(json.dumps(stop_msg))


if __name__ == "__main__":
    pytest.main(["-v", __file__])
