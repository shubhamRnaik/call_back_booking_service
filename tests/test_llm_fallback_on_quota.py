import asyncio

from indic_tts_runtime.brain.llm_service import StreamingBrain


class FakeModels:
    def __init__(self, error_message: str):
        self.error_message = error_message

    def generate_content_stream(self, **kwargs):
        raise RuntimeError(self.error_message)


class FakeClient:
    def __init__(self, error_message: str):
        self.models = FakeModels(error_message)


def test_stream_response_returns_quota_fallback_message():
    brain = StreamingBrain(api_key="test-key")
    brain.client = FakeClient("429 RESOURCE_EXHAUSTED")

    async def collect_response():
        parts = []
        async for token in brain.stream_response("hello"):
            parts.append(token)
        return "".join(parts)

    response = asyncio.run(collect_response())

    assert "अभी जवाब देने में असमर्थ" in response
