#!/usr/bin/env python
"""
Issue 4 regression test: Guard 0a in SarvamWebSocketClient.stream_audio_chunks()
must only treat a second large (>=8000 byte) chunk as an utterance-restart
replay when its CONTENT matches a previously-seen large chunk - not on size
alone (which used to false-trigger on Sarvam's normal end-of-stream flush
chunk, truncating legitimate responses, including the very first greeting).

Run: .venv\\Scripts\\python.exe test_guard0a_replay_detection.py
"""
import asyncio
import base64
import sys

sys.path.insert(0, ".")

from sarvamai import AudioOutput
from sarvamai.types.audio_output import AudioOutputData

from indic_tts_runtime.services.sarvam_service import SarvamWebSocketClient

LARGE_SIZE = 8500  # >= LARGE_CHUNK_THRESHOLD_BYTES (8000)
NORMAL_SIZE = 3200


def _audio_message(content: bytes) -> AudioOutput:
    """Wrap raw PCM bytes into the same AudioOutput shape stream_audio_chunks expects."""
    b64 = base64.b64encode(content).decode("utf-8")
    return AudioOutput(data=AudioOutputData(content_type="audio/wav", audio=b64))


class FakeWSConnection:
    """Minimal async-iterable stand-in for sarvamai's real WS connection object."""

    def __init__(self, messages):
        self._messages = list(messages)

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for message in self._messages:
            yield message


def _make_client_with_messages(messages) -> SarvamWebSocketClient:
    client = SarvamWebSocketClient()
    client._connected = True
    client._ws_connection = FakeWSConnection(messages)
    return client


async def test_legitimate_second_large_chunk_is_not_treated_as_replay():
    """A second large chunk with DIFFERENT content (e.g. a legitimate
    end-of-stream flush chunk) must NOT stop the stream early."""
    lead_in = b"\x01" * LARGE_SIZE
    normal1 = b"\x02" * NORMAL_SIZE
    normal2 = b"\x03" * NORMAL_SIZE
    flush_chunk = b"\x04" * LARGE_SIZE  # large, but different content

    messages = [
        _audio_message(lead_in),
        _audio_message(normal1),
        _audio_message(normal2),
        _audio_message(flush_chunk),
    ]
    client = _make_client_with_messages(messages)

    yielded = [chunk async for chunk in client.stream_audio_chunks()]

    assert len(yielded) == len(messages), (
        f"Expected all {len(messages)} chunks to be yielded (no false-positive "
        f"replay detection), got {len(yielded)}"
    )
    assert yielded[-1] == flush_chunk, "Legitimate flush chunk must be yielded, not dropped"
    print("✓ Legitimate second large chunk (different content) streamed through unaffected")


async def test_identical_second_large_chunk_is_detected_as_replay():
    """A second large chunk with IDENTICAL content to a previously-seen large
    chunk IS a genuine replay and must stop the stream before it's yielded."""
    lead_in = b"\x01" * LARGE_SIZE
    normal1 = b"\x02" * NORMAL_SIZE
    normal2 = b"\x03" * NORMAL_SIZE
    replay_of_lead_in = b"\x01" * LARGE_SIZE  # identical bytes to lead_in

    messages = [
        _audio_message(lead_in),
        _audio_message(normal1),
        _audio_message(normal2),
        _audio_message(replay_of_lead_in),
    ]
    client = _make_client_with_messages(messages)

    yielded = [chunk async for chunk in client.stream_audio_chunks()]

    assert len(yielded) == 3, (
        f"Expected stream to stop BEFORE yielding the replayed lead-in chunk "
        f"(3 chunks), got {len(yielded)}"
    )
    assert yielded == [lead_in, normal1, normal2], (
        "Only the pre-replay chunks should have been yielded, in order"
    )
    print("✓ Identical second large chunk (genuine replay) correctly stops the stream")


async def main():
    await test_legitimate_second_large_chunk_is_not_treated_as_replay()
    await test_identical_second_large_chunk_is_detected_as_replay()
    print("\n✅ All Guard 0a replay-detection tests passed.")


if __name__ == "__main__":
    asyncio.run(main())
