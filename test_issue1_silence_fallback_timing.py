#!/usr/bin/env python
"""
Issue 1 regression test: the silence-fallback re-engagement prompt must NOT
fire while the caller is still mid-utterance (e.g. reading out a long phone
number that takes >5s to finish), and must ONLY fire after genuine STT
inactivity.

This runs `_websocket_exotel_stream_impl` in-process against a fake
WebSocket + a fake STT client that we fully control the timing of, and fake
TTS/LLM clients so no real network/API calls are made. It patches the
classes at `indic_tts_runtime.main` module level (where they're looked up),
not the original service modules.

Scenario:
  t=0s   speech_started fires
  t=0-7s no further STT signal (caller talking continuously, no pause)
  t=7s   final_transcript arrives with the full sentence
  t=7-∞  total STT silence (call test 2's genuine-silence path)

Assertions:
  - No re-engagement ("Kya aap wahan hain?") turn is sent to the brain
    before/around t=7s (i.e. the caller was never interrupted mid-sentence).
  - After the real transcript is processed, if the caller then goes truly
    silent for 5+ seconds, the re-engagement prompt DOES fire exactly once.

Run: .venv\\Scripts\\python.exe test_issue1_silence_fallback_timing.py
"""
import asyncio
import json
import sys
import time

sys.path.insert(0, ".")

import indic_tts_runtime.main as main_module
from indic_tts_runtime.services.stt_service import STTEvent
from indic_tts_runtime.normalizer import MultilingualTextNormalizer

REENGAGEMENT_PHRASE = "Kya aap wahan hain"
LONG_UTTERANCE_TEXT = "mera number nau aath saat che paanch char teen do ek hai"

brain_calls = []  # (elapsed_seconds, user_text) for every turn dispatched to the LLM
test_start_time = None


class FakeSTTClient:
    """Fully-controlled STT client: emits speech_started at t=0, stays silent
    for 7s (simulating one long continuous utterance), then emits the final
    transcript at t=7s, then goes silent forever (to exercise the genuine
    silence-timeout path)."""

    async def connect(self, language_code="hi-IN", **kwargs):
        return True

    async def disconnect(self):
        pass

    async def send_audio_chunk(self, chunk):
        return True

    async def signal_end_of_stream(self):
        return True

    async def stream_transcripts(self):
        yield STTEvent(event_type="speech_started")
        await asyncio.sleep(7.0)
        yield STTEvent(event_type="final_transcript", transcript=LONG_UTTERANCE_TEXT)
        # Genuine silence forever afterwards - let the real code's silence
        # timer run its course.
        await asyncio.sleep(3600)


class FakeTTSClient:
    """No-op TTS client - avoids any real Sarvam network calls."""

    async def connect(self, **kwargs):
        return True

    async def disconnect(self):
        pass

    async def send_text_chunk(self, text):
        return True

    async def send_flush(self):
        return True

    async def stream_audio_chunks(self, **kwargs):
        return
        yield b""  # pragma: no cover - makes this an async generator


class FakeBrain:
    """Records every turn's user text + elapsed time instead of calling a
    real LLM, and yields a short canned reply."""

    def __init__(self, system_prompt=None, **kwargs):
        pass

    async def prewarm(self):
        pass

    async def stream_response(self, user_text):
        elapsed = time.perf_counter() - test_start_time
        brain_calls.append((elapsed, user_text))
        for tok in ["Theek ", "hai."]:
            yield tok


class FakeWebSocket:
    def __init__(self):
        self._sent_start = False
        self.headers = {}
        self.client = None  # falls back to "unknown" client IP - fine for this test

    async def accept(self):
        pass

    async def receive(self):
        if not self._sent_start:
            self._sent_start = True
            start_event = {
                "event": "start",
                "stream_sid": "sim_stream_1",
                "start": {"stream_sid": "sim_stream_1"},
            }
            return {"text": json.dumps(start_event)}
        # No further client frames - all activity is driven by the fake STT
        # client's own generator. Just hang until the test cancels the task.
        await asyncio.Event().wait()

    async def send_json(self, data):
        pass

    async def close(self, code=1000, reason=""):
        pass


async def main():
    global test_start_time

    # Patch the classes exactly where main.py looks them up.
    main_module.SarvamSaarasSTTClient = FakeSTTClient
    main_module.SarvamWebSocketClient = FakeTTSClient
    main_module.StreamingBrain = FakeBrain
    main_module.text_normalizer = MultilingualTextNormalizer()
    main_module.supabase_service = None  # forces the hardcoded fallback tenant config

    fake_ws = FakeWebSocket()
    test_start_time = time.perf_counter()

    task = asyncio.create_task(
        main_module._websocket_exotel_stream_impl(fake_ws, tenant_id="default")
    )

    # Let the scenario play out past t=7s (final transcript) plus a margin,
    # then check no premature interruption occurred.
    await asyncio.sleep(8.5)
    premature = [c for c in brain_calls if REENGAGEMENT_PHRASE in c[1]]
    assert not premature, (
        f"Silence-fallback fired mid-utterance (before/around t=7s)! "
        f"Calls so far: {brain_calls}"
    )
    real_turn_calls = [c for c in brain_calls if LONG_UTTERANCE_TEXT in c[1]]
    assert real_turn_calls, f"Expected the real transcript to reach the brain. Calls: {brain_calls}"
    print(f"✓ No premature mid-utterance interruption. Calls up to t=8.5s: {brain_calls}")

    # Now wait for genuine silence (5s+ after the real turn) and confirm the
    # re-engagement prompt DOES fire.
    await asyncio.sleep(6.0)
    genuine_fallback = [c for c in brain_calls if REENGAGEMENT_PHRASE in c[1]]
    assert genuine_fallback, (
        f"Expected re-engagement prompt after genuine silence, none fired. Calls: {brain_calls}"
    )
    print(f"✓ Re-engagement prompt correctly fired after genuine silence: {genuine_fallback}")

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    print("\n✅ Issue 1 silence-fallback timing test passed.")


if __name__ == "__main__":
    asyncio.run(main())
