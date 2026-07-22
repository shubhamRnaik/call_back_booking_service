"""
LLM streaming service for the voice agent.
Supports Gemini or OpenAI based on the configured provider.
"""

import asyncio
import logging
import threading
from typing import Optional, AsyncGenerator, Deque, List, Dict, Any
from collections import deque
from datetime import datetime
from dataclasses import dataclass
import time

try:
    from google import genai
except ImportError:
    import google.genai as genai

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None

from ..config import settings
from .prompts import SYSTEM_PROMPT

logger = logging.getLogger(__name__)


@dataclass
class ConversationTurn:
    """Represents a single conversation turn (user + assistant)."""
    user_text: str
    assistant_response: str
    timestamp: datetime


class StreamingBrain:
    """
    Streaming LLM service that can use Gemini or OpenAI.
    Maintains a rolling context window and streams responses token-by-token.
    """

    # Configuration constants
    GEMINI_MODEL = "gemini-2.0-flash"
    OPENAI_MODEL = "gpt-4o-mini"
    CONTEXT_WINDOW_TURNS = 4  # Keep last 4 turns for lower prompt latency
    TEMPERATURE = 0.5  # Stable concise responses for voice calls
    MAX_OUTPUT_TOKENS = 50  # Keep output short to reduce generation tail

    def __init__(
        self,
        api_key: Optional[str] = None,
        system_prompt: Optional[str] = None,
    ):
        """
        Initialize the LLM brain.

        Args:
            api_key: Optional override for the provider API key.
            system_prompt: Optional custom system prompt for multi-tenant context.
        """
        self.provider = settings.effective_llm_provider
        self.system_prompt = system_prompt or SYSTEM_PROMPT
        self.api_key = api_key or (
            settings.openai_api_key if self.provider == "openai"
            else settings.gemini_api_key
        )

        if self.provider == "openai":
            self.model = settings.openai_model or self.OPENAI_MODEL
            self.client = None
            if OpenAI is not None and self.api_key:
                try:
                    self.client = OpenAI(api_key=self.api_key)
                except Exception as exc:
                    logger.error(f"Failed to initialize OpenAI client: {exc}")
            else:
                logger.warning(
                    "OpenAI package not available or API key missing"
                )
        else:
            self.model = settings.gemini_model or self.GEMINI_MODEL
            self.model_candidates = [
                self.model,
                "gemini-1.5-flash",
                "gemini-2.0-flash",
            ]
            self.model_candidates = list(dict.fromkeys(self.model_candidates))

            try:
                self.client = genai.Client(api_key=self.api_key)
            except (AttributeError, TypeError):
                try:
                    genai.configure(api_key=self.api_key)
                    self.client = genai.Client()
                except Exception as exc:
                    logger.error(f"Failed to initialize Gemini client: {exc}")
                    self.client = None

        # Conversation history (rolling context window)
        self._conversation_history: Deque[ConversationTurn] = deque(
            maxlen=self.CONTEXT_WINDOW_TURNS
        )

        # Statistics
        self._stats = {
            "total_requests": 0,
            "total_tokens_generated": 0,
            "total_errors": 0,
            "avg_ttft_ms": 0.0,
            "avg_response_time_ms": 0.0,
            "last_error": None,
        }

        self._response_times = deque(maxlen=50)
        self._ttft_times = deque(maxlen=50)

        logger.info(
            "Streaming Brain initialized with provider: "
            f"{self.provider} model: {self.model}"
        )

    async def _iter_sync_generator(self, gen_factory) -> AsyncGenerator[Any, None]:
        """
        Drain a blocking/synchronous generator (e.g. an OpenAI/Gemini streaming
        iterator that performs network I/O per item) on a background thread,
        forwarding items to the caller without blocking the asyncio event loop.
        """
        loop = asyncio.get_event_loop()
        queue: asyncio.Queue = asyncio.Queue()
        _SENTINEL = object()

        def _worker():
            try:
                for item in gen_factory():
                    loop.call_soon_threadsafe(queue.put_nowait, item)
            except Exception as exc:  # noqa: BLE001
                loop.call_soon_threadsafe(queue.put_nowait, exc)
            finally:
                loop.call_soon_threadsafe(queue.put_nowait, _SENTINEL)

        threading.Thread(target=_worker, daemon=True).start()

        while True:
            item = await queue.get()
            if item is _SENTINEL:
                break
            if isinstance(item, Exception):
                raise item
            yield item

    async def prewarm(self) -> None:
        """
        Fire a tiny, throwaway completion request as soon as the brain is
        created so the TLS/connection-pool handshake with the LLM provider
        happens in parallel with the greeting instead of on the user's first
        turn. Errors are swallowed - this is best-effort latency mitigation.
        """
        try:
            if self.provider == "openai":
                if self.client is None:
                    return

                def _call():
                    self.client.chat.completions.create(
                        model=self.model,
                        messages=[{"role": "user", "content": "hi"}],
                        stream=False,
                        max_tokens=1,
                        temperature=self.TEMPERATURE,
                    )

                await asyncio.wait_for(
                    asyncio.to_thread(_call),
                    timeout=settings.llm_api_timeout_sec,
                )
            else:
                if self.client is None:
                    return

                def _call():
                    self.client.models.generate_content(
                        model=self.model,
                        contents="hi",
                        config=genai.types.GenerateContentConfig(
                            max_output_tokens=1,
                            temperature=self.TEMPERATURE,
                        ),
                    )

                await asyncio.wait_for(
                    asyncio.to_thread(_call),
                    timeout=settings.llm_api_timeout_sec,
                )
            logger.debug("Brain prewarm request completed")
        except Exception as exc:  # noqa: BLE001
            logger.debug(f"Brain prewarm skipped/failed (non-fatal): {exc}")

    async def stream_response(
        self, user_text: str
    ) -> AsyncGenerator[str, None]:
        """
        Generate a streaming response to user text using the active
        LLM provider.
        Yields tokens as they arrive from the API.
        """
        try:
            start_time = time.time()
            ttft_recorded = False
            logger.debug(f"Streaming response for: {user_text[:100]}...")

            if self.provider == "openai":
                messages = self._build_openai_messages(user_text)

                def call_openai():
                    if self.client is None:
                        raise RuntimeError("OpenAI client is not initialized")
                    return self.client.chat.completions.create(
                        model=self.model,
                        messages=messages,
                        stream=True,
                        temperature=self.TEMPERATURE,
                        max_tokens=self.MAX_OUTPUT_TOKENS,
                    )

                try:
                    response = await asyncio.wait_for(
                        asyncio.to_thread(call_openai),
                        timeout=settings.llm_api_timeout_sec,
                    )
                except asyncio.TimeoutError:
                    logger.error(
                        "❌ OpenAI API timeout after %.1f seconds!",
                        settings.llm_api_timeout_sec,
                    )
                    self._stats["total_errors"] += 1
                    self._stats["last_error"] = "OpenAI API timeout"
                    yield "क्षमा करें, सर्वर से संपर्क नहीं हो पा रहा है।"
                    return

                def get_text_chunks():
                    try:
                        for chunk in response:
                            if getattr(chunk, "choices", None):
                                delta = getattr(
                                    chunk.choices[0], "delta", None
                                )
                                content = getattr(delta, "content", None)
                                if content:
                                    yield content
                    except Exception as exc:
                        logger.error(f"Error iterating OpenAI response: {exc}")
                        raise
            else:
                contents = self._build_gemini_contents(user_text)

                def call_gemini():
                    last_error = None
                    for candidate_model in self.model_candidates:
                        try:
                            logger.debug(
                                f"Trying Gemini model: {candidate_model}"
                            )
                            return self.client.models.generate_content_stream(
                                model=candidate_model,
                                contents=contents,
                                config=genai.types.GenerateContentConfig(
                                    temperature=self.TEMPERATURE,
                                    max_output_tokens=self.MAX_OUTPUT_TOKENS,
                                    system_instruction=self.system_prompt,
                                ),
                            )
                        except Exception as exc:
                            last_error = exc
                            logger.warning(
                                f"Gemini model {candidate_model} failed: {exc}"
                            )
                            continue

                    if self.client is None:
                        raise RuntimeError("Gemini client is not initialized")

                    if last_error is not None:
                        raise last_error

                    return genai.GenerativeModel(
                        self.model,
                        system_instruction=self.system_prompt,
                    ).generate_content(
                        contents,
                        stream=True,
                        generation_config=genai.types.GenerationConfig(
                            temperature=self.TEMPERATURE,
                            max_output_tokens=self.MAX_OUTPUT_TOKENS,
                        ),
                    )

                try:
                    response = await asyncio.wait_for(
                        asyncio.to_thread(call_gemini),
                        timeout=settings.llm_api_timeout_sec,
                    )
                except asyncio.TimeoutError:
                    logger.error(
                        "❌ Gemini API timeout after %.1f seconds!",
                        settings.llm_api_timeout_sec,
                    )
                    self._stats["total_errors"] += 1
                    self._stats["last_error"] = "Gemini API timeout"
                    yield "क्षमा करें, सर्वर से संपर्क नहीं हो पा रहा है।"
                    return

                def get_text_chunks():
                    try:
                        for chunk in response:
                            if hasattr(chunk, "text") and chunk.text:
                                yield chunk.text
                            elif isinstance(chunk, str):
                                yield chunk
                            else:
                                yield str(chunk)
                    except Exception as exc:
                        logger.error(f"Error iterating Gemini response: {exc}")
                        raise

            full_response = ""
            async for token in self._iter_sync_generator(get_text_chunks):
                if token:
                    if not ttft_recorded:
                        ttft = (time.time() - start_time) * 1000
                        self._ttft_times.append(ttft)
                        logger.debug(f"TTFT: {ttft:.0f}ms")
                        ttft_recorded = True

                    full_response += token
                    self._stats["total_tokens_generated"] += 1
                    yield token
                    await asyncio.sleep(0)

            response_time = (time.time() - start_time) * 1000
            self._response_times.append(response_time)
            self._conversation_history.append(
                ConversationTurn(
                    user_text=user_text,
                    assistant_response=full_response,
                    timestamp=datetime.now(),
                )
            )

            self._stats["total_requests"] += 1
            self._stats["avg_response_time_ms"] = (
                sum(self._response_times) / len(self._response_times)
            )
            if self._ttft_times:
                self._stats["avg_ttft_ms"] = sum(self._ttft_times) / len(
                    self._ttft_times
                )

            logger.debug(
                f"Response complete ({response_time:.0f}ms): "
                f"{full_response[:100]}..."
            )

        except asyncio.TimeoutError:
            logger.error("❌ Brain response generation timed out!")
            self._stats["total_errors"] += 1
            self._stats["last_error"] = "Timeout"
            yield "माफ करें, मुझे जवाब देने में समय लग गया।"
        except Exception as exc:
            logger.error(f"❌ Error streaming response: {exc}")
            self._stats["total_errors"] += 1
            self._stats["last_error"] = str(exc)
            if (
                "429" in str(exc)
                or "quota" in str(exc).lower()
                or "rate" in str(exc).lower()
            ):
                yield (
                    "अभी जवाब देने में असमर्थ हूँ, कृपया थोड़ी देर बाद "
                    "फिर से कोशिश करें।"
                )
            else:
                yield "कुछ गड़बड़ हुई, कृपया दोबारा कोशिश करें।"

    def _build_openai_messages(self, user_text: str) -> List[Dict[str, str]]:
        """Build OpenAI chat messages with role-separated history."""
        messages: List[Dict[str, str]] = [
            {"role": "system", "content": self.system_prompt}
        ]

        for turn in self._conversation_history:
            messages.append({"role": "user", "content": turn.user_text})
            messages.append(
                {"role": "assistant", "content": turn.assistant_response}
            )

        messages.append({"role": "user", "content": user_text})
        return messages

    def _build_gemini_contents(self, user_text: str) -> List[Dict[str, Any]]:
        """Build Gemini contents with role-separated history."""
        contents: List[Dict[str, Any]] = []

        for turn in self._conversation_history:
            contents.append(
                {"role": "user", "parts": [{"text": turn.user_text}]}
            )
            contents.append(
                {
                    "role": "model",
                    "parts": [{"text": turn.assistant_response}],
                }
            )

        contents.append({"role": "user", "parts": [{"text": user_text}]})
        return contents

    def clear_history(self) -> None:
        """Clear conversation history."""
        self._conversation_history.clear()
        logger.info("Conversation history cleared")

    def get_conversation_history(self) -> list:
        """Get current conversation history."""
        return list(self._conversation_history)

    def get_stats(self) -> dict:
        """Get LLM service statistics."""
        return {
            **self._stats,
            "context_window_turns": len(self._conversation_history),
            "provider": self.provider,
            "model": self.model,
        }

    def get_last_response(self) -> Optional[str]:
        """Get the last assistant response."""
        if self._conversation_history:
            return self._conversation_history[-1].assistant_response
        return None
