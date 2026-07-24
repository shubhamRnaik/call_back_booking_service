#!/usr/bin/env python
"""
Issue 7 regression test: the caller's own phone number (captured from the
telephony 'start' event) must be captured into session.caller_phone, and the
booking flow must use it instead of asking the caller for a number.

Run: .venv\\Scripts\\python.exe test_issue7_caller_phone.py
"""
import asyncio
import json
import sys

sys.path.insert(0, ".")

import indic_tts_runtime.main as main_module
from indic_tts_runtime.core.session import session_manager, CallSession
from indic_tts_runtime.normalizer import MultilingualTextNormalizer

TEST_PHONE = "+919876543210"


class FakeSTTClient:
    """No STT activity needed for this test - the call just needs to reach
    the 'start' event handling; everything after that stays silent."""

    async def connect(self, language_code="hi-IN", **kwargs):
        return True

    async def disconnect(self):
        pass

    async def send_audio_chunk(self, chunk):
        return True

    async def signal_end_of_stream(self):
        return True

    async def stream_transcripts(self):
        await asyncio.sleep(3600)
        return
        yield  # pragma: no cover - makes this an async generator


class FakeTTSClient:
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
        yield b""  # pragma: no cover


class FakeBrain:
    def __init__(self, system_prompt=None, **kwargs):
        pass

    async def prewarm(self):
        pass

    async def stream_response(self, user_text):
        yield "unused"


class FakeWebSocketWithPhone:
    def __init__(self, start_extra: dict):
        self._sent_start = False
        self._start_extra = start_extra
        self.headers = {}
        self.client = None

    async def accept(self):
        pass

    async def receive(self):
        if not self._sent_start:
            self._sent_start = True
            start_event = {
                "event": "start",
                "stream_sid": "sim_stream_phone",
                "start": {
                    "stream_sid": "sim_stream_phone",
                    **self._start_extra,
                },
            }
            return {"text": json.dumps(start_event)}
        await asyncio.Event().wait()

    async def send_json(self, data):
        pass

    async def close(self, code=1000, reason=""):
        pass


async def test_caller_phone_captured_from_start_event():
    main_module.SarvamSaarasSTTClient = FakeSTTClient
    main_module.SarvamWebSocketClient = FakeTTSClient
    main_module.StreamingBrain = FakeBrain
    main_module.text_normalizer = MultilingualTextNormalizer()
    main_module.supabase_service = None

    fake_ws = FakeWebSocketWithPhone(start_extra={"from": TEST_PHONE})
    task = asyncio.create_task(
        main_module._websocket_exotel_stream_impl(fake_ws, tenant_id="default")
    )
    await asyncio.sleep(0.3)

    sessions = [s for s in session_manager.all_sessions() if s.call_id == "sim_stream_phone"]
    assert sessions, "Expected a session to be created for this call"
    session = sessions[0]
    assert session.caller_phone == TEST_PHONE, (
        f"Expected session.caller_phone == {TEST_PHONE!r}, got {session.caller_phone!r}"
    )
    print(f"✓ session.caller_phone captured from 'start' event: {session.caller_phone}")

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def test_booking_flow_uses_caller_phone_without_asking():
    session = CallSession(connection_id="test-conn-issue7", tenant_id="default")
    session.set_caller_phone(TEST_PHONE)

    recorded_calls = []

    class FakeSupabaseService:
        async def check_slot_available(self, **kwargs):
            return True, "Slot is available."

        async def create_appointment_async(self, **kwargs):
            recorded_calls.append(kwargs)
            return {"status": "CONFIRMED"}

    main_module.supabase_service = FakeSupabaseService()

    tenant_config = {
        "items": [{"name": "Dr. Sharma", "id": "item-1", "slot_duration_mins": 30}],
        "timezone": "Asia/Kolkata",
    }
    # Note: no 'phone' field in the tag body - the caller never gave one.
    tag_body = "item=Dr. Sharma|when=tomorrow at 5pm|name=Rahul"

    response_text = await main_module._process_booking_tag(
        tag_body, session, tenant_config, "en-IN"
    )

    assert recorded_calls, f"create_appointment_async was never called. Bot response: {response_text!r}"
    assert recorded_calls[0]["patient_phone"] == TEST_PHONE, (
        f"Expected patient_phone == {TEST_PHONE!r}, got {recorded_calls[0]['patient_phone']!r}"
    )
    print(
        "✓ Booking used session.caller_phone without the caller providing one: "
        f"{recorded_calls[0]['patient_phone']!r}. Bot response: {response_text!r}"
    )


async def main():
    await test_caller_phone_captured_from_start_event()
    await test_booking_flow_uses_caller_phone_without_asking()
    print("\n✅ Issue 7 caller-phone tests passed.")


if __name__ == "__main__":
    asyncio.run(main())
