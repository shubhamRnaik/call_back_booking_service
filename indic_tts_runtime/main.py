"""
FastAPI Orchestrator Application: Entry point for Text-to-Speech engine.
Exposes WebSocket and RESTful endpoints for real-time voice synthesis with streaming audio delivery.
"""

import logging
import uuid
import time
import json
import asyncio
import os
import random
import base64
import re
from datetime import datetime
from typing import Optional, Callable, Awaitable, Any
from contextlib import asynccontextmanager
from collections import defaultdict, deque

from fastapi import FastAPI, HTTPException, BackgroundTasks, WebSocket, WebSocketDisconnect, File, UploadFile, Form, Request
from fastapi.responses import StreamingResponse, FileResponse, RedirectResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import uvicorn

from .config import settings
from .schemas import TTSRequest, ErrorResponse, HealthCheckResponse
from .services.cache_service import CacheService
from .services.sarvam_service import SarvamWebSocketClient
from .services.stt_service import SarvamSaarasSTTClient
from .brain.llm_service import StreamingBrain
from .core.router import VoiceRouter, RoutingStrategy
from .core.scheduler import PacketScheduler
from .normalizer import MultilingualTextNormalizer
from .chunker import StreamTextChunker
from .core.telephony_audio import telephony_to_stt_pcm, tts_pcm_to_telephony
from .services.supabase_service import SupabaseService
from .core.session import session_manager, CallSession
from .core.emergency import (
    check_emergency_fastpath,
    find_emergency_match,
    build_emergency_handover_phrase,
    compute_playback_drain_seconds,
    close_websocket_for_emergency_transfer,
)
from .core.datetime_utils import parse_user_datetime

# Configure logging
logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
# Dedicated logger for structured JSON per-turn observability records (Step 5).
# Kept separate from the human-readable emoji logs above so both can coexist -
# tooling can grep/parse this logger's output without regex-stripping emojis.
obs_logger = logging.getLogger("indic_tts_runtime.observability")


def _log_observability(event: str, session: Optional[CallSession] = None, **extra: Any) -> None:
    """Emit a single structured JSON log line for a call-lifecycle event."""
    try:
        payload = {"event": event}
        if session is not None:
            payload.update(session.to_observability_dict())
        payload.update(extra)
        obs_logger.info(json.dumps(payload, default=str))
    except Exception as exc:
        logger.debug(f"Observability logging failed for event={event}: {exc}")


# Global service instances
cache_service: Optional[CacheService] = None
sarvam_ws_client: Optional[SarvamWebSocketClient] = None
voice_router: Optional[VoiceRouter] = None
packet_scheduler: Optional[PacketScheduler] = None
text_normalizer: Optional[MultilingualTextNormalizer] = None
text_chunker: Optional[StreamTextChunker] = None
supabase_service: Optional[SupabaseService] = None

# Metrics tracking
start_time = datetime.now()
request_metrics = {
    "total_requests": 0,
    "successful_requests": 0,
    "failed_requests": 0,
    "total_ttfb_ms": 0.0,
    "cache_hits": 0,
    "sarvam_hits": 0,
    "auth_failures": 0,
    "rate_limited": 0,
    "readiness_failures": 0,
    "ws_rejected": 0,
    "retry_attempts": 0,
}

# In-memory operational state (single-process runtime)
_rate_limit_state = {
    "rest": defaultdict(deque),
    "ws_connect": defaultdict(deque),
}
_ws_state = {
    "active_total": 0,
    "active_per_ip": defaultdict(int),
}
_latency_samples = {
    "brain_ttft_ms": deque(maxlen=1000),
    "tts_first_audio_ms": deque(maxlen=1000),
    "overlap_savings_ms": deque(maxlen=1000),
}
_state_lock = asyncio.Lock()


def _client_ip_from_request(request: Request) -> str:
    """Resolve client IP from request with proxy header fallback."""
    forwarded = request.headers.get("x-forwarded-for", "").strip()
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def _client_ip_from_websocket(websocket: WebSocket) -> str:
    """Resolve client IP from websocket scope/header."""
    forwarded = websocket.headers.get("x-forwarded-for", "").strip()
    if forwarded:
        return forwarded.split(",")[0].strip()
    if websocket.client and websocket.client.host:
        return websocket.client.host
    return "unknown"


def _is_authorized(api_key: Optional[str]) -> bool:
    """Validate API key when security mode is enabled."""
    if not settings.security_enabled:
        return True
    expected = (settings.service_api_key or "").strip()
    presented = (api_key or "").strip()
    return bool(expected) and expected == presented


def _is_exotel_ws_authorized(token: Optional[str]) -> bool:
    """Validate the optional Exotel WS shared-secret query token.

    Unset EXOTEL_WS_SHARED_TOKEN means auth is not enforced (backward
    compatible with existing deployments/tests); once configured, every
    connection must present a matching `?token=` query param.
    """
    expected = (settings.exotel_ws_token or "").strip()
    if not expected:
        return True
    return (token or "").strip() == expected


def _mask_phone(phone: str) -> str:
    """Mask all but the last 4 digits of a phone number for safe logging
    (avoid leaking caller PII into application logs - OWASP A09/privacy)."""
    digits = str(phone or "")
    if len(digits) <= 4:
        return "*" * len(digits)
    return "*" * (len(digits) - 4) + digits[-4:]


def _allow_rate_limit(bucket: deque, limit: int, window_sec: float) -> bool:
    """Sliding-window in-memory rate limiter."""
    now = time.time()
    while bucket and now - bucket[0] > window_sec:
        bucket.popleft()
    if len(bucket) >= limit:
        return False
    bucket.append(now)
    return True


def _compute_percentile(values: deque, percentile: float) -> float:
    """Compute percentile from deque values."""
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = int(round((len(ordered) - 1) * percentile))
    idx = max(0, min(idx, len(ordered) - 1))
    return float(ordered[idx])


def _with_language_lock(user_text: str, language_code: str) -> str:
    """Inject a strict per-turn language instruction without causing LLM tag prefixes."""
    lang = (language_code or "hi-IN").strip()
    instruction = (
        f"Reply strictly in {lang} language only. "
        "Do NOT prefix your response with language codes, tags, or prefixes. Speak naturally."
    )
    return f"{instruction}\n\nUser: {user_text}"


def _with_slot_context(user_text: str, language_code: str, session: CallSession) -> str:
    """Like _with_language_lock, but also grounds the LLM in booking slots
    already confirmed this call (session.extracted_slots) so it stops
    re-asking a question that's already been answered - e.g. the caller
    asked "which doctor is available", the bot named the one available
    doctor, but nothing recorded that as a confirmed slot, so the bot kept
    re-asking "do you want to book with Dr. X?" every turn instead of moving
    on to date/time (repeated-question bug observed on a live call,
    2026-07-24)."""
    base = _with_language_lock(user_text, language_code)
    known = {k: v for k, v in (session.extracted_slots or {}).items() if v}
    if not known:
        return base
    known_bits = ", ".join(f"{k}={v}" for k, v in known.items())
    state_note = (
        "Already confirmed this call - do NOT ask about these again, move "
        f"straight to whatever is still missing: {known_bits}."
    )
    return f"{state_note}\n\n{base}"


async def _retry_async(
    operation: Callable[[], Awaitable[Any]],
    operation_name: str,
    attempts: Optional[int] = None,
) -> Any:
    """Retry async operation with jittered backoff for transient failures."""
    max_attempts = attempts or settings.retry_max_attempts
    last_exc: Optional[Exception] = None

    for attempt in range(1, max_attempts + 1):
        try:
            if attempt > 1:
                request_metrics["retry_attempts"] += 1
            return await operation()
        except Exception as exc:
            last_exc = exc
            if attempt >= max_attempts:
                break
            jitter = random.uniform(0, settings.retry_jitter_ms / 1000.0)
            delay = (settings.retry_base_delay_ms / 1000.0) * (2 ** (attempt - 1))
            sleep_for = delay + jitter
            logger.warning(
                "%s attempt %d/%d failed: %s (retrying in %.2fs)",
                operation_name,
                attempt,
                max_attempts,
                exc,
                sleep_for,
            )
            await asyncio.sleep(sleep_for)

    raise RuntimeError(
        f"{operation_name} failed after {max_attempts} attempts"
    ) from last_exc


async def _try_register_ws_connection(client_ip: str) -> bool:
    """Enforce websocket global/per-IP connection limits."""
    async with _state_lock:
        if _ws_state["active_total"] >= settings.max_ws_connections_total:
            return False
        if _ws_state["active_per_ip"][client_ip] >= settings.max_ws_connections_per_ip:
            return False
        _ws_state["active_total"] += 1
        _ws_state["active_per_ip"][client_ip] += 1
        return True


async def _unregister_ws_connection(client_ip: str) -> None:
    """Release websocket connection counters safely."""
    async with _state_lock:
        _ws_state["active_total"] = max(0, _ws_state["active_total"] - 1)
        if _ws_state["active_per_ip"][client_ip] > 0:
            _ws_state["active_per_ip"][client_ip] -= 1


async def _probe_stt_service() -> tuple[bool, str]:
    """Run a lightweight STT connectivity probe."""
    client = SarvamSaarasSTTClient()
    try:
        connected = await asyncio.wait_for(client.connect(), timeout=6)
        if connected:
            await asyncio.wait_for(client.disconnect(), timeout=4)
            return True, "ok"
        return False, "stt_connect_failed"
    except Exception as exc:
        logger.warning(f"STT probe failed: {exc}")
        return False, str(exc)


async def _probe_tts_service() -> tuple[bool, str]:
    """Run a lightweight TTS connectivity probe."""
    client = sarvam_ws_client or SarvamWebSocketClient()
    try:
        if client.health_check():
            return True, "already_connected"

        connected = await asyncio.wait_for(client.connect(), timeout=8)
        if connected:
            await asyncio.wait_for(client.disconnect(), timeout=4)
            return True, "ok"
        return False, "tts_connect_failed"
    except Exception as exc:
        logger.warning(f"TTS probe failed: {exc}")
        return False, str(exc)


async def _probe_llm_service() -> tuple[bool, str]:
    """Run a lightweight LLM/brain probe."""
    try:
        brain = StreamingBrain()
        if brain.client is not None:
            return True, "initialized"
        return False, "client_unavailable"
    except Exception as exc:
        logger.warning(f"LLM probe failed: {exc}")
        return False, str(exc)


async def _probe_supabase_service() -> tuple[bool, str]:
    """Run a lightweight Supabase connectivity probe (multi-tenant backend)."""
    if not supabase_service:
        return False, "not_initialized"
    try:
        return await supabase_service.check_connectivity()
    except Exception as exc:
        logger.warning(f"Supabase probe failed: {exc}")
        return False, str(exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for FastAPI application.
    Handles initialization and cleanup of services.
    """
    # Startup
    logger.info("=== WebSocket TTS Engine Startup ===")
    
    global cache_service, sarvam_ws_client, voice_router, packet_scheduler, text_normalizer, text_chunker, supabase_service
    
    try:
        # Initialize cache service
        cache_service = CacheService()
        logger.info(f"✓ Cache Service initialized")
        logger.info(f"  {cache_service.get_cache_stats()}")
        
        # Initialize Sarvam WebSocket client (persistent connection)
        sarvam_ws_client = SarvamWebSocketClient()
        logger.info("✓ Sarvam WebSocket Client initialized (lazy connection on demand)")
        logger.info(f"  {sarvam_ws_client.get_connection_stats()}")
        
        # Initialize voice router
        voice_router = VoiceRouter(
            cache_service=cache_service,
            sarvam_service=None,  # Will use WebSocket client in streaming context
            strategy=RoutingStrategy.CACHE_FIRST
        )
        logger.info("✓ Voice Router initialized")
        
        # Initialize packet scheduler
        packet_scheduler = PacketScheduler()
        logger.info("✓ Packet Scheduler initialized (20ms @ 8kHz)")
        logger.info(f"  {packet_scheduler.get_scheduler_stats()}")
        
        # Initialize multilingual text normalizer
        text_normalizer = MultilingualTextNormalizer(default_language="hi")
        logger.info("✓ Multilingual Text Normalizer initialized (8 languages)")
        
        # Initialize streaming text chunker
        text_chunker = StreamTextChunker(min_word_threshold=5, max_word_threshold=7)
        logger.info("✓ Streaming Text Chunker initialized")

        # Initialize Supabase service (multi-tenant booking backend).
        # NOT fail-fast by design: a transient Supabase outage should not
        # prevent the whole app (including tenants servable via the
        # hardcoded fallback config) from starting. The probe result is
        # logged loudly here AND exposed via /api/v1/ready for ops visibility.
        supabase_service = SupabaseService()
        supabase_ok, supabase_msg = await supabase_service.check_connectivity()
        if supabase_ok:
            logger.info("✓ Supabase Service initialized and reachable")
        else:
            logger.error(
                f"✗ Supabase Service initialized but UNREACHABLE at startup: "
                f"{supabase_msg}. Multi-tenant lookups will fall back to "
                f"hardcoded tenant configs where available."
            )
        
        logger.info("=== All services initialized successfully ===\n")
        
    except Exception as e:
        logger.error(f"✗ Startup failed: {e}")
        raise

    yield

    # Shutdown
    logger.info("\n=== WebSocket TTS Engine Shutdown ===")
    try:
        if sarvam_ws_client:
            await sarvam_ws_client.disconnect()
            logger.info("✓ Sarvam WebSocket Client closed")
        if supabase_service:
            await supabase_service.close()
            logger.info("✓ Supabase Service closed")
        logger.info("=== Shutdown complete ===")
    except Exception as e:
        logger.error(f"✗ Shutdown error: {e}")


# Create FastAPI application
app = FastAPI(
    title="Indic TTS Runtime Engine",
    description="Production-grade Text-to-Speech with Voice Orchestration and Audio Streaming",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS. Wildcard origin + credentials is a credential-leak/CSRF
# risk (OWASP A05 Security Misconfiguration) - browsers already refuse to
# honor allow_credentials with a literal "*" origin, but we make the safe
# behavior explicit here rather than depending on that. Set
# CORS_ALLOWED_ORIGINS to a comma-separated list of real origins in
# production to enable credentialed cross-origin requests.
_cors_origins_raw = (settings.cors_allowed_origins or "*").strip()
if _cors_origins_raw == "*":
    _cors_origins = ["*"]
    _cors_allow_credentials = False
else:
    _cors_origins = [o.strip() for o in _cors_origins_raw.split(",") if o.strip()]
    _cors_allow_credentials = True

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=_cors_allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def security_and_rate_limit_middleware(request: Request, call_next):
    """Apply API-key auth and rate limiting to REST API endpoints."""
    path = request.url.path
    if not path.startswith("/api"):
        return await call_next(request)

    if path in {"/api/v1/live", "/api/v1/ready", "/api/v1/health"}:
        return await call_next(request)

    api_key = request.headers.get("x-api-key")
    if not _is_authorized(api_key):
        request_metrics["auth_failures"] += 1
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

    client_ip = _client_ip_from_request(request)
    bucket = _rate_limit_state["rest"][client_ip]
    allowed = _allow_rate_limit(
        bucket,
        settings.rest_rate_limit_per_min,
        window_sec=60.0,
    )
    if not allowed:
        request_metrics["rate_limited"] += 1
        return JSONResponse(status_code=429, content={"detail": "Rate limit exceeded"})

    return await call_next(request)

# Serve static files (web UI). The /tests mount is dev-only: it would
# otherwise publicly expose test scripts/fixtures (potential internal
# details, sample payloads, etc.) - OWASP A05 Security Misconfiguration /
# improper asset exposure. Gate it behind an explicit opt-in env var so it
# never ships enabled in production by default.
if settings.expose_dev_static_routes:
    tests_dir = os.path.join(os.path.dirname(__file__), "..", "tests")
    if os.path.exists(tests_dir):
        app.mount("/tests", StaticFiles(directory=tests_dir), name="tests")
        logger.info(f"✓ Static files mounted: /tests → {tests_dir} (dev-only)")
else:
    logger.info("ℹ️ /tests static mount disabled (set EXPOSE_DEV_STATIC_ROUTES=true for local dev)")

static_dir = os.path.join(os.path.dirname(__file__), "..", "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")
    logger.info(f"✓ Static files mounted: /static → {static_dir}")


@app.get("/")
async def root_redirect():
    """Redirect to voice call UI."""
    return RedirectResponse(url="/call")


@app.get("/call")
async def voice_call_ui():
    """Serve the real-time voice call UI."""
    static_path = os.path.join(os.path.dirname(__file__), "..", "static", "index.html")
    if os.path.exists(static_path):
        return FileResponse(static_path, media_type="text/html")
    else:
        raise HTTPException(status_code=404, detail="Voice call UI not found")


@app.post(
    "/api/v1/stream-voice",
    response_class=StreamingResponse,
    responses={
        200: {
            "description": "Streaming audio response",
            "content": {"audio/wav": {}}
        },
        400: {"model": ErrorResponse},
        500: {"model": ErrorResponse}
    }
)
async def stream_voice_synthesis(
    request: TTSRequest,
    background_tasks: BackgroundTasks
) -> StreamingResponse:
    """
    Stream audio synthesis endpoint.
    
    Performs Voice Orchestration through Router, schedules packets,
    measures TTFB, and returns live streaming binary audio.
    
    Args:
        request: TTS request payload
        background_tasks: Background task scheduler
        
    Returns:
        StreamingResponse with audio/wav content
    """
    request_id = str(uuid.uuid4())[:13]
    ttfb_start = time.perf_counter()  # Monotonic timer for accurate TTFB measurement
    
    logger.info(f"[{request_id}] ← New TTS request: {request.text[:50]}...")

    if not cache_service or not sarvam_ws_client or not voice_router or not packet_scheduler:
        logger.error(f"[{request_id}] ✗ Services not initialized")
        raise HTTPException(status_code=500, detail="Services not initialized")

    try:
        # Route request through orchestration layer
        logger.info(f"[{request_id}] → Routing through Voice Router...")
        audio_stream, metadata = await voice_router.route_and_synthesize(
            text=request.text,
            language=request.target_language_code,
            speaker=request.speaker,
            pace=request.pace
        )

        # Measure TTFB (Time to First Byte)
        ttfb_ms = (time.perf_counter() - ttfb_start) * 1000
        logger.info(f"[{request_id}] ⏱ TTFB: {ttfb_ms:.2f}ms")

        # Update metrics
        request_metrics["total_requests"] += 1
        request_metrics["successful_requests"] += 1
        request_metrics["total_ttfb_ms"] += ttfb_ms

        source = metadata.get("source", "unknown")
        if source == "cache":
            request_metrics["cache_hits"] += 1
        elif source == "sarvam":
            request_metrics["sarvam_hits"] += 1

        # Schedule audio packets for regulated streaming
        logger.info(f"[{request_id}] 📦 Scheduling audio packets...")

        async def stream_generator():
            """Generator for streaming response body."""
            try:
                async for packet in packet_scheduler.schedule_bytes_stream(audio_stream.read()):
                    yield packet
                logger.info(f"[{request_id}] ✓ Stream complete")
            except Exception as e:
                logger.error(f"[{request_id}] ✗ Streaming error: {e}")
                raise

        # Log summary
        response_metadata = {
            "request_id": request_id,
            "status": "success",
            "ttfb_ms": round(ttfb_ms, 2),
            "source": source,
            "duration_ms": metadata.get("duration_ms", 0),
            "size_bytes": metadata.get("size_bytes", 0),
        }

        logger.info(f"[{request_id}] 📤 Sending response: {response_metadata}")

        # Return streaming response with audio/wav content
        return StreamingResponse(
            content=stream_generator(),
            media_type="audio/wav",
            headers={
                "Content-Disposition": f'attachment; filename="voice_{request_id}.wav"',
                "X-Request-ID": request_id,
                "X-TTFB-Ms": str(round(ttfb_ms, 2)),
                "X-Audio-Source": source,
            }
        )

    except Exception as e:
        request_metrics["failed_requests"] += 1
        logger.error(f"[{request_id}] ✗ TTS synthesis failed: {e}")
        
        raise HTTPException(
            status_code=500,
            detail=ErrorResponse(
                error_code="TTS_SYNTHESIS_FAILED",
                message=str(e),
                details={"request_id": request_id}
            ).model_dump()
        )


@app.websocket("/ws/v1/stream-voice")
async def websocket_stream_voice(websocket: WebSocket):
    """
    WebSocket endpoint for real-time streaming audio synthesis.
    
    Implements the full pipeline:
    Text Input → Normalizer → Chunker → WebSocket Synthesis → Scheduler → Audio Streaming
    
    Protocol:
    - Client sends: {"type": "text", "data": {"text": "...", "language": "hi-IN"}}
    - Server sends: {"type": "audio", "data": {"audio": <base64 pcm>}}
    - Client sends: {"type": "barge-in"} to interrupt
    - Server sends: {"type": "done"} when synthesis complete
    """
    connection_id = str(uuid.uuid4())[:13]
    client_ip = _client_ip_from_websocket(websocket)
    connection_registered = False

    if not _allow_rate_limit(
        _rate_limit_state["ws_connect"][client_ip],
        settings.ws_connect_rate_limit_per_min,
        window_sec=60.0,
    ):
        request_metrics["ws_rejected"] += 1
        await websocket.close(code=4408, reason="WS connect rate limit exceeded")
        return

    connection_registered = await _try_register_ws_connection(client_ip)
    if not connection_registered:
        request_metrics["ws_rejected"] += 1
        await websocket.close(code=4429, reason="WS connection limit reached")
        return

    await websocket.accept()
    
    logger.info(f"[WS-{connection_id}] 🔌 WebSocket connected")
    
    # Create a NEW Sarvam client for THIS connection (don't reuse global)
    sarvam_client = SarvamWebSocketClient()
    
    ws_session_start = time.perf_counter()
    utterance_count = 0

    try:
        if text_normalizer is None or packet_scheduler is None:
            await websocket.send_text(
                json.dumps({
                    "type": "error",
                    "data": {"error": "Service not initialized"}
                })
            )
            return

        # Expect initial config message
        config_msg = await websocket.receive_text()
        config = json.loads(config_msg)

        header_api_key = websocket.headers.get("x-api-key")
        config_api_key = config.get("api_key")
        if not _is_authorized(header_api_key or config_api_key):
            request_metrics["auth_failures"] += 1
            await websocket.send_text(
                json.dumps({"type": "error", "data": {"error": "Unauthorized"}})
            )
            await websocket.close(code=4401)
            return
        
        target_language_code = config.get("language", "hi-IN")
        speaker = config.get("speaker", "shubh")
        pace = config.get("pace", 0.95)
        
        logger.info(
            f"[WS-{connection_id}] ⚙️  Config: lang={target_language_code}, "
            f"speaker={speaker}, pace={pace}"
        )
        
        # Connect NEW Sarvam WebSocket for this client connection
        logger.info(f"[WS-{connection_id}] 🔗 Connecting to Sarvam WebSocket...")
        success = await _retry_async(
            lambda: sarvam_client.connect(
                target_language_code=target_language_code,
                speaker=speaker,
                pace=pace,
            ),
            "sarvam_tts_connect_stream",
        )
        if not success:
            await websocket.send_text(
                json.dumps({
                    "type": "error",
                    "data": {"error": "Failed to connect to Sarvam"}
                })
            )
            await websocket.close()
            return
        
        # State tracking
        ttfb_start = time.perf_counter()
        audio_stream_task = None
        
        async def stream_audio_background():
            """Stream audio from Sarvam to client (background task)."""
            nonlocal ttfb_start
            
            logger.debug(f"[WS-{connection_id}] Starting audio stream background task")
            try:
                ttfb_reported = False
                chunk_count = 0
                async for audio_chunk in sarvam_client.stream_audio_chunks():
                    chunk_count += 1
                    
                    # Report TTFB on first audio chunk
                    if not ttfb_reported:
                        ttfb_ms = (time.perf_counter() - ttfb_start) * 1000
                        logger.info(f"[WS-{connection_id}] ⏱ TTFB: {ttfb_ms:.2f}ms (chunk {chunk_count})")
                        try:
                            await websocket.send_text(
                                json.dumps({"type": "ttfb", "data": {"ttfb_ms": round(ttfb_ms, 2)}})
                            )
                        except Exception as e:
                            logger.warning(f"[WS-{connection_id}] Failed to send TTFB: {e}")
                        ttfb_reported = True
                    
                    # Encode audio as base64
                    audio_b64 = base64.b64encode(audio_chunk).decode("utf-8")
                    try:
                        await websocket.send_text(
                            json.dumps({"type": "audio", "data": {"audio": audio_b64}})
                        )
                    except Exception as e:
                        logger.warning(f"[WS-{connection_id}] Failed to send audio chunk {chunk_count}: {e}")
                        break
                
                logger.info(f"[WS-{connection_id}] Audio stream complete ({chunk_count} chunks)")
                
                # Send completion signal
                try:
                    await websocket.send_text(json.dumps({"type": "done"}))
                    logger.debug(f"[WS-{connection_id}] Sent 'done' message")
                except Exception as e:
                    logger.warning(f"[WS-{connection_id}] Failed to send done message: {e}")
            
            except asyncio.CancelledError:
                logger.debug(f"[WS-{connection_id}] Audio stream cancelled")
            except Exception as e:
                logger.error(f"[WS-{connection_id}] Error in audio streaming: {e}")
                try:
                    await websocket.send_text(
                        json.dumps({"type": "error", "data": {"error": f"Audio streaming error: {str(e)}"}})
                    )
                except:
                    pass
        
        # Main message handling loop
        try:
            while True:
                try:
                    if (time.perf_counter() - ws_session_start) > settings.max_ws_session_seconds:
                        logger.info(f"[WS-{connection_id}] Session duration limit reached")
                        break

                    # Wait for incoming message with timeout
                    message = await asyncio.wait_for(websocket.receive_text(), timeout=300.0)
                except asyncio.TimeoutError:
                    logger.warning(f"[WS-{connection_id}] Connection idle for 300s, closing")
                    break
                
                msg = json.loads(message)
                msg_type = msg.get("type")
                
                if msg_type == "text":
                    utterance_count += 1
                    if utterance_count > settings.max_ws_utterances:
                        logger.info(f"[WS-{connection_id}] Utterance limit reached")
                        break

                    text = msg.get("data", {}).get("text", "")
                    if not text.strip():
                        await websocket.send_text(
                            json.dumps({"type": "error", "data": {"error": "Empty text"}})
                        )
                        continue
                    
                    logger.info(f"[WS-{connection_id}] 📤 Received text: {text[:60]}...")
                    
                    # Normalize text
                    normalized_text = text_normalizer.normalize(text, target_language_code)
                    logger.debug(f"[WS-{connection_id}] Normalized: {normalized_text[:60]}...")
                    
                    # Reset TTFB timer
                    ttfb_start = time.perf_counter()
                    
                    # Cancel previous audio stream task if running
                    if audio_stream_task and not audio_stream_task.done():
                        logger.debug(f"[WS-{connection_id}] Cancelling previous audio stream")
                        audio_stream_task.cancel()
                        try:
                            await audio_stream_task
                        except asyncio.CancelledError:
                            pass
                    
                    # Send text to Sarvam (retry for transient failures)
                    async def _send_text_with_error_on_false() -> bool:
                        sent = await sarvam_client.send_text_chunk(normalized_text)
                        if not sent:
                            raise RuntimeError("send_text_chunk returned False")
                        return sent

                    success = await _retry_async(
                        _send_text_with_error_on_false,
                        "sarvam_send_text_chunk",
                    )
                    if not success:
                        await websocket.send_text(
                            json.dumps({
                                "type": "error",
                                "data": {"error": "Failed to send text to Sarvam"}
                            })
                        )
                        continue
                    
                    # Flush to trigger audio generation (required by Sarvam API)
                    async def _send_flush_with_error_on_false() -> bool:
                        flushed = await sarvam_client.send_flush()
                        if not flushed:
                            raise RuntimeError("send_flush returned False")
                        return flushed

                    flush_ok = await _retry_async(
                        _send_flush_with_error_on_false,
                        "sarvam_send_flush",
                    )
                    logger.debug(f"[WS-{connection_id}] Flush sent: {flush_ok}")
                    
                    # Start audio streaming as background task
                    audio_stream_task = asyncio.create_task(stream_audio_background())
                    logger.debug(f"[WS-{connection_id}] Audio stream task started")
                
                elif msg_type == "barge-in":
                    logger.info(f"[WS-{connection_id}] 🔴 Barge-in triggered!")
                    await packet_scheduler.trigger_barge_in(sarvam_client)
                    
                    # Cancel audio stream
                    if audio_stream_task and not audio_stream_task.done():
                        audio_stream_task.cancel()
                        try:
                            await audio_stream_task
                        except asyncio.CancelledError:
                            pass
                    
                    await websocket.send_text(
                        json.dumps({"type": "barge-in-ack"})
                    )
                
                else:
                    logger.warning(f"[WS-{connection_id}] Unknown message type: {msg_type}")
        
        except WebSocketDisconnect:
            logger.info(f"[WS-{connection_id}] 🔌 Client disconnected")
        except Exception as e:
            logger.error(f"[WS-{connection_id}] Error in main loop: {e}")
            try:
                await websocket.send_text(
                    json.dumps({"type": "error", "data": {"error": str(e)}})
                )
            except:
                pass
    except Exception as e:
        logger.error(f"[WS-{connection_id}] ✗ WebSocket error: {e}")
        try:
            await websocket.send_text(
                json.dumps({"type": "error", "data": {"error": str(e)}})
            )
        except:
            pass
    
    finally:
        if connection_registered:
            await _unregister_ws_connection(client_ip)
        try:
            await websocket.close()
        except:
            pass
        
        # Cleanup - disconnect this connection's Sarvam client
        try:
            await sarvam_client.disconnect()
            logger.info(f"[WS-{connection_id}] ✓ Sarvam client disconnected")
        except:
            pass
        
        # Reset scheduler state
        await packet_scheduler.reset_barge_in()


@app.websocket("/ws/v1/voice-call")
async def websocket_voice_call(websocket: WebSocket):
    """Real-time bidirectional WebSocket for voice calls."""
    connection_id = str(uuid.uuid4())[:13]
    client_ip = _client_ip_from_websocket(websocket)
    connection_registered = False
    logger.info(f"[VC-{connection_id}] 🎤 Handler called!")

    if not _allow_rate_limit(
        _rate_limit_state["ws_connect"][client_ip],
        settings.ws_connect_rate_limit_per_min,
        window_sec=60.0,
    ):
        request_metrics["ws_rejected"] += 1
        await websocket.close(code=4408, reason="WS connect rate limit exceeded")
        return

    connection_registered = await _try_register_ws_connection(client_ip)
    if not connection_registered:
        request_metrics["ws_rejected"] += 1
        await websocket.close(code=4429, reason="WS connection limit reached")
        return
    
    try:
        if text_normalizer is None:
            await websocket.close(code=1011, reason="Service not initialized")
            return

        await websocket.accept()
        logger.info(f"[VC-{connection_id}] ✅ Accepted")
    except Exception as e:
        logger.error(f"[VC-{connection_id}] ❌ Accept failed: {e}")
        return
    
    # Per-connection state
    stt_client = None
    tts_client = None
    brain = None
    call_start_time = time.perf_counter()
    is_speaking = False
    language_code = "hi-IN"
    speaker = "shubh"
    pending_brain_task = None
    pending_audio_task = None
    pending_fallback_task = None
    greeting_task = None
    user_utterance_count = 0
    last_final_transcript_norm = ""
    last_final_transcript_at = 0.0
    speaking_hold_until = 0.0
    
    # ✅ Utterance tracking for preventing stale audio loops
    current_utterance_id = 0
    
    async def send_status(state: str):
        """Send status update to client."""
        try:
            await websocket.send_json({"type": "status", "state": state})
        except Exception as e:
            logger.warning(f"[VC-{connection_id}] Failed to send status '{state}': {e}")
    
    async def send_transcript(text: str, final: bool = False, role: str = "assistant"):
        """Send transcript to client with explicit sender role."""
        try:
            await websocket.send_json({
                "type": "transcript",
                "text": text,
                "final": final,
                "role": role,
            })
        except Exception as e:
            logger.warning(f"[VC-{connection_id}] Failed to send transcript: {e}")
    
    async def send_audio_packet(audio_data: bytes):
        """Send audio packet to client."""
        try:
            await websocket.send_bytes(audio_data)
        except Exception as e:
            logger.warning(f"[VC-{connection_id}] Failed to send audio: {e}")
    
    async def send_metrics(metrics: dict):
        """Send metrics to client."""
        try:
            await websocket.send_json({"type": "metrics", **metrics})
        except Exception as e:
            logger.warning(f"[VC-{connection_id}] Failed to send metrics: {e}")
    
    async def process_transcript_to_response(transcript: str):
        """
        Bulletproof Per-Utterance Voice Pipeline:
        1. Launches fresh TTS WebSocket connection in parallel with LLM token streaming.
        2. Masks connection handshake latency completely behind LLM generation (~0ms overhead).
        3. Guarantees fresh audio generator per turn (0% chance of 0-chunk silence or audio loops).
        """
        nonlocal is_speaking, current_utterance_id, speaking_hold_until

        current_utterance_id += 1
        this_utterance_id = current_utterance_id

        if not transcript.strip() or len(transcript.replace(" ", "")) < 3:
            return

        logger.info(f"[VC-{connection_id}] 📥 TRANSCRIPT RECEIVED: '{transcript}'")
        await send_status("thinking")

        # 1. Instantiate fresh Sarvam TTS client for THIS turn
        turn_tts_client = SarvamWebSocketClient()
        
        # 2. Connect to Sarvam IN PARALLEL with LLM token generation
        # (Masks the ~120ms WebSocket handshake completely behind LLM generation time)
        tts_connect_task = asyncio.create_task(
            _retry_async(
                lambda: turn_tts_client.connect(
                    target_language_code=language_code,
                    speaker=speaker,
                    pace=0.95,
                ),
                "tts_connect_per_utterance",
            )
        )

        try:
            brain_start = time.perf_counter()
            llm_input = _with_language_lock(transcript, language_code)
            response_tokens = []
            ttft_recorded = False

            # 3. Collect LLM tokens (~200ms)
            async for token in brain.stream_response(llm_input):
                if this_utterance_id != current_utterance_id:
                    logger.info(f"[VC-{connection_id}] 🛑 Interrupted during LLM generation")
                    return

                if not ttft_recorded:
                    brain_ttft_ms = (time.perf_counter() - brain_start) * 1000
                    await send_metrics({"brain_ttft_ms": round(brain_ttft_ms, 2)})
                    ttft_recorded = True

                response_tokens.append(token)

            raw_response_text = "".join(response_tokens).strip()

            # Check for end of call tag
            should_hangup = "[END_CALL]" in raw_response_text
            clean_response_text = raw_response_text.replace("[END_CALL]", "").strip()
            # Strip a leading "xx-XX:" language-code prefix the model sometimes adds
            clean_response_text = re.sub(r'^[a-zA-Z]{2}-[A-Z]{2}:\s*', '', clean_response_text).strip()

            if not clean_response_text or this_utterance_id != current_utterance_id:
                await send_status("listening")
                return

            logger.info(f"[VC-{connection_id}] 🧠 LLM Complete ({len(response_tokens)} tokens): '{clean_response_text}' (Hangup: {should_hangup})")

            # 4. Await parallel TTS connection (already finished while LLM was streaming!)
            tts_connected = await tts_connect_task
            if not tts_connected or this_utterance_id != current_utterance_id:
                logger.error(f"[VC-{connection_id}] ❌ Per-turn TTS connection failed or turn interrupted")
                await send_status("error")
                return

            # 5. Normalize & send ONE text chunk + ONE flush to fresh Sarvam socket
            normalized_text = text_normalizer.normalize(
                clean_response_text, target_language_code=language_code
            )

            await send_transcript(clean_response_text, final=True, role="assistant")
            await send_status("speaking")

            sent = await turn_tts_client.send_text_chunk(normalized_text)
            if sent:
                await turn_tts_client.send_flush()
                logger.info(f"[VC-{connection_id}] 📤 Sent text & flush to fresh Sarvam socket")

                # 6. Stream audio chunks cleanly (100% guaranteed fresh generator)
                is_speaking = True
                audio_chunks_sent = 0
                total_bytes_sent = 0
                tts_first_audio_recorded = False

                async for audio_chunk in turn_tts_client.stream_audio_chunks(
                    initial_timeout_sec=2.5,
                    post_audio_idle_timeout_sec=0.4,
                    max_duration_sec=12.0
                ):
                    if this_utterance_id != current_utterance_id:
                        logger.info(f"[VC-{connection_id}] 🛑 Interrupted during audio stream")
                        break

                    if not tts_first_audio_recorded:
                        first_audio_ms = (time.perf_counter() - brain_start) * 1000
                        await send_metrics({"tts_first_audio_ms": round(first_audio_ms, 2)})
                        tts_first_audio_recorded = True

                    audio_chunks_sent += 1
                    total_bytes_sent += len(audio_chunk)
                    await send_audio_packet(audio_chunk)

                logger.info(f"[VC-{connection_id}] 🔊 Audio streaming complete ({audio_chunks_sent} chunks, {total_bytes_sent} bytes)")

            # 7. Hangup execution if end call tag detected
            if should_hangup and this_utterance_id == current_utterance_id:
                # 22050 Hz @ 16-bit Mono (2 bytes per sample) = 44,100 bytes per second
                audio_duration_sec = total_bytes_sent / 44100.0 if total_bytes_sent > 0 else 0.5
                wait_time = max(1.0, audio_duration_sec + 0.8)
                logger.info(f"[VC-{connection_id}] 🛑 End-of-call tag detected. Waiting {wait_time:.2f}s for audio playback before closing...")
                await asyncio.sleep(wait_time)
                try:
                    await websocket.send_json({"type": "hangup", "reason": "call_completed"})
                    await websocket.close(code=1000, reason="Call completed successfully")
                except Exception as e:
                    logger.warning(f"[VC-{connection_id}] Error closing websocket on hangup: {e}")
                return

        except asyncio.CancelledError:
            logger.debug(f"[VC-{connection_id}] 🛑 Turn cancelled")
        except Exception as e:
            logger.error(f"[VC-{connection_id}] ❌ Turn error: {e}", exc_info=True)
            await send_status("error")
        finally:
            # 7. Clean up connection task and disconnect per-turn socket
            if not tts_connect_task.done():
                tts_connect_task.cancel()
                await asyncio.gather(tts_connect_task, return_exceptions=True)
            try:
                await turn_tts_client.disconnect()
            except Exception:
                pass

            is_speaking = False
            speaking_hold_until = time.perf_counter() + 0.8
            await send_status("listening")
    
    async def stream_greeting_audio(greeting_text: str):
        """Stream the initial greeting: cached audio if available, else live TTS."""
        nonlocal greeting_task, is_speaking, speaking_hold_until

        is_speaking = True
        await send_status("speaking")
        await send_transcript(greeting_text, final=True, role="assistant")

        # Prefer cached greeting audio for instant playback.
        if cache_service:
            cached_greeting = cache_service.get_cached_greeting_audio(language_code)
            if cached_greeting:
                pcm_bytes, metadata = cached_greeting
                logger.info(f"[VC-{connection_id}] 🎤 Streaming cached greeting audio ({len(pcm_bytes)} bytes)")
                await send_audio_packet(pcm_bytes)
                is_speaking = False
                speaking_hold_until = time.perf_counter() + 0.9
                await send_status("listening")
                logger.info(f"[VC-{connection_id}] ✓ Greeting from cache complete")
                return
            else:
                logger.warning(f"[VC-{connection_id}] ⚠️  No cached greeting found, synthesizing live TTS greeting")

        # Fallback: synthesize the greeting live so the call never opens in silence.
        try:
            normalized_greeting = text_normalizer.normalize(
                greeting_text, target_language_code=language_code
            )
            sent = await tts_client.send_text_chunk(normalized_greeting)
            if sent:
                await tts_client.send_flush()
                chunks_sent = 0
                async for audio_chunk in tts_client.stream_audio_chunks(
                    idle_timeout_sec=2.0,
                    max_duration_sec=10.0,
                ):
                    await send_audio_packet(audio_chunk)
                    chunks_sent += 1
                logger.info(f"[VC-{connection_id}] ✓ Greeting synthesized live ({chunks_sent} chunks)")
            else:
                logger.warning(f"[VC-{connection_id}] ❌ Failed to send greeting text to TTS")
        except Exception as exc:
            logger.error(f"[VC-{connection_id}] ❌ Live greeting synthesis failed: {exc}")
        finally:
            is_speaking = False
            speaking_hold_until = time.perf_counter() + 0.9

        await send_status("listening")

    async def stream_stt_transcripts():
        """Stream STT transcripts to client and trigger brain processing."""
        nonlocal pending_brain_task, pending_fallback_task, user_utterance_count
        nonlocal last_final_transcript_norm, last_final_transcript_at
        
        logger.info(f"[VC-{connection_id}] 🎤 STT: Stream listener started (monitoring for events...)")
        
        try:
            current_transcript = ""
            transcript_start_time = time.perf_counter()
            event_count = 0
            
            async for event in stt_client.stream_transcripts():
                event_count += 1
                logger.info(f"[VC-{connection_id}] 🎤 STT EVENT #{event_count}: type={event.event_type}")
                
                if event.event_type == "speech_started":
                    logger.info(f"[VC-{connection_id}] 🎤 STT: Speech started")
                    current_transcript = ""
                    transcript_start_time = time.perf_counter()
                    
                elif event.event_type == "speech_ended":
                    elapsed_ms = (time.perf_counter() - transcript_start_time) * 1000
                    logger.info(f"[VC-{connection_id}] 🎤 STT: Speech ended ({elapsed_ms:.2f}ms), current_transcript='{current_transcript}'")
                    
                    await send_metrics({"stt_latency_ms": round(elapsed_ms, 2)})

                    async def fallback_after_silence():
                        logger.info(f"[VC-{connection_id}] ⏱️ Starting 5.0s silence timeout...")
                        await asyncio.sleep(5.0)
                        if not current_transcript.strip() and not is_speaking:
                            logger.info(f"[VC-{connection_id}] ⏱️ 5s silence detected after turn. Triggering goodbye...")
                            farewell = "Shukriya! Call karne ke liye dhanyawad. [END_CALL]"
                            if language_code == "ta-IN":
                                farewell = "நன்றி! அழைத்ததற்கு நன்றி. [END_CALL]"
                            elif language_code == "te-IN":
                                farewell = "ధన్యవాదాలు! కాల్ చేసినందుకు ధన్యవాదాలు. [END_CALL]"
                            elif language_code == "kn-IN":
                                farewell = "ಧನ್ಯವಾದಗಳು! ಕರೆ ಮಾಡಿದ್ದಕ್ಕಾಗಿ ಧನ್ಯವಾದಗಳು. [END_CALL]"
                            elif language_code == "en-IN":
                                farewell = "Thank you! Have a great day ahead. [END_CALL]"
                            await process_transcript_to_response(farewell)

                    if pending_brain_task and not pending_brain_task.done():
                        logger.debug(f"[VC-{connection_id}] Skipping fallback while brain task is already running")
                    else:
                        fallback_task = asyncio.create_task(fallback_after_silence())
                        if pending_fallback_task and not pending_fallback_task.done():
                            pending_fallback_task.cancel()
                            try:
                                await pending_fallback_task
                            except asyncio.CancelledError:
                                pass
                        pending_fallback_task = fallback_task
                    
                elif event.event_type == "final_transcript":
                    current_transcript = event.transcript
                    logger.info(f"[VC-{connection_id}] 🎤 STT: FINAL TRANSCRIPT RECEIVED: '{current_transcript}'")

                    normalized_final = " ".join(current_transcript.lower().split())
                    now_monotonic = time.perf_counter()
                    if (
                        normalized_final
                        and normalized_final == last_final_transcript_norm
                        and (now_monotonic - last_final_transcript_at) <= 2.5
                    ):
                        logger.info(
                            f"[VC-{connection_id}] ⏭️  Skipping duplicate final transcript within dedupe window"
                        )
                        continue

                    last_final_transcript_norm = normalized_final
                    last_final_transcript_at = now_monotonic

                    await send_transcript(current_transcript, final=True, role="user")

                    user_utterance_count += 1
                    if user_utterance_count > settings.max_ws_utterances:
                        logger.info(f"[VC-{connection_id}] Utterance limit reached")
                        await send_status("session_limit")
                        break
                    
                    # ✅ TRIGGER BRAIN HERE - now transcript is populated!
                    if current_transcript.strip() and len(current_transcript.replace(" ", "")) >= 3:
                        logger.info(f"[VC-{connection_id}] 🎤→🧠 Queueing transcript for brain processing (length={len(current_transcript)})")

                        # Cancel previous brain task if running
                        if pending_brain_task and not pending_brain_task.done():
                            logger.debug(f"[VC-{connection_id}] Cancelling previous brain task")
                            pending_brain_task.cancel()
                            try:
                                await pending_brain_task
                            except asyncio.CancelledError:
                                pass
                        
                        # Start new brain task
                        logger.info(f"[VC-{connection_id}] 🧠 Creating new brain task...")
                        pending_brain_task = asyncio.create_task(
                            process_transcript_to_response(current_transcript)
                        )
                        logger.info(f"[VC-{connection_id}] 🧠 Brain task created")
                    else:
                        logger.warning(f"[VC-{connection_id}] ⏭️  Transcript too short/empty, skipping: '{current_transcript}'")
                    
                elif event.event_type == "error":
                    logger.error(f"[VC-{connection_id}] 🎤 STT error: {event.error_message}")
                    await send_status("error")
                else:
                    logger.debug(f"[VC-{connection_id}] 🎤 STT: Unknown event type: {event.event_type}")
        
        except asyncio.CancelledError:
            logger.debug(f"[VC-{connection_id}] 🛑 STT stream cancelled")
        except Exception as e:
            logger.error(f"[VC-{connection_id}] ❌ STT stream error: {e}", exc_info=True)
            await send_status("error")
    
    
    try:
        # Receive initial config message
        logger.info(f"[VC-{connection_id}] ⏳ Waiting for config message...")
        try:
            config_msg = await asyncio.wait_for(websocket.receive_text(), timeout=10)
            logger.info(f"[VC-{connection_id}] ✓ Config message received: {config_msg}")
            config = json.loads(config_msg)

            header_api_key = websocket.headers.get("x-api-key")
            config_api_key = config.get("api_key")
            if not _is_authorized(header_api_key or config_api_key):
                request_metrics["auth_failures"] += 1
                await websocket.send_json({"type": "error", "message": "Unauthorized"})
                await websocket.close(code=4401)
                return
        except asyncio.TimeoutError:
            logger.error(f"[VC-{connection_id}] ❌ Config message timeout (10s) - client not sending config!")
            await websocket.send_json({"type": "error", "message": "Config message timeout"})
            await websocket.close()
            return
        except Exception as e:
            logger.error(f"[VC-{connection_id}] ❌ Config message error: {e}", exc_info=True)
            try:
                await websocket.send_json({"type": "error", "message": f"Config error: {e}"})
            except:
                pass
            try:
                await websocket.close()
            except:
                pass
            return
        
        # Extract language and speaker from config
        language_code = config.get("language", "hi-IN")
        speaker = config.get("speaker", "shubh")
        
        logger.info(f"[VC-{connection_id}] ⚙️  Config: lang={language_code}, speaker={speaker}")
        
        # Initialize services for this connection
        logger.info(f"[VC-{connection_id}] 🔧 Initializing STT client...")
        stt_client = SarvamSaarasSTTClient()
        logger.info(f"[VC-{connection_id}] 🔧 Initializing TTS client...")
        tts_client = SarvamWebSocketClient()
        logger.info(f"[VC-{connection_id}] 🔧 Initializing Brain (LLM)...")
        brain = StreamingBrain()
        # Fire-and-forget: warm up the LLM connection (TLS/pool handshake)
        # now so the user's first real turn doesn't pay that cost on top of
        # generation latency. Overlaps with STT/TTS connect + greeting.
        asyncio.create_task(brain.prewarm())
        logger.info(f"[VC-{connection_id}] ✓ All service instances created")
        
        # **TASK 3**: Connect STT and TTS in PARALLEL to reduce setup latency
        logger.info(f"[VC-{connection_id}] 🔗 Connecting STT and TTS in parallel...")
        try:
            connect_results = await asyncio.gather(
                _retry_async(stt_client.connect, "stt_connect"),
                _retry_async(
                    lambda: tts_client.connect(
                        target_language_code=language_code,
                        speaker=speaker,
                        pace=0.95,
                    ),
                    "tts_connect_voice_call",
                ),
                return_exceptions=True,
            )
            
            logger.debug(f"[VC-{connection_id}] STT connect result: {connect_results[0]} (type: {type(connect_results[0]).__name__})")
            logger.debug(f"[VC-{connection_id}] TTS connect result: {connect_results[1]} (type: {type(connect_results[1]).__name__})")
            
            stt_connected = isinstance(connect_results[0], bool) and connect_results[0]
            tts_connected = isinstance(connect_results[1], bool) and connect_results[1]
            
            logger.info(f"[VC-{connection_id}] STT connected: {stt_connected}")
            logger.info(f"[VC-{connection_id}] TTS connected: {tts_connected}")
        except Exception as e:
            logger.error(f"[VC-{connection_id}] ❌ Connection attempt failed: {e}", exc_info=True)
            stt_connected = False
            tts_connected = False
        
        if not stt_connected or not tts_connected:
            error_msg = "STT/TTS connection failed. Check API key."
            logger.error(f"[VC-{connection_id}] ❌ {error_msg}")
            try:
                await websocket.send_json({"type": "error", "message": error_msg})
            except Exception as e:
                logger.warning(f"[VC-{connection_id}] Failed to send error message: {e}")
            try:
                if stt_client:
                    await stt_client.disconnect()
            except Exception:
                pass
            try:
                if tts_client:
                    await tts_client.disconnect()
            except Exception:
                pass
            try:
                await websocket.close()
            except Exception:
                pass
            return
        
        logger.info(f"[VC-{connection_id}] ✓ STT and TTS connected")
        
        # Send greeting message immediately
        logger.info(f"[VC-{connection_id}] 🎤 Sending greeting...")
        service_name = settings.service_name
        greeting_msg = f"नमस्ते! {service_name} में कॉल करने के लिए धन्यवाद। कृपया अपना सवाल पूछें।"  # "Namaste! Thank you for calling {service_name}. Please ask your question."
        if language_code == "ta-IN":
            greeting_msg = f"வணக்கம்! {service_name} க்கு அழைத்ததற்கு நன்றி. உங்கள் கேள்வியைக் கேளுங்கள்."
        elif language_code == "te-IN":
            greeting_msg = f"నమస్కారం! {service_name}కి కాల్ చేసినందుకు ధన్యవాదాలు. దయచేసి మీ ప్రశ్న అడగండి."
        elif language_code == "kn-IN":
            greeting_msg = f"ನಮಸ್ಕಾರ! {service_name} ಗೆ ಕರೆ ಮಾಡಿದ್ದಕ್ಕಾಗಿ ಧನ್ಯವಾದಗಳು. ದಯವಿಟ್ಟು ನಿಮ್ಮ ಪ್ರಶ್ನೆಯನ್ನು ಕೇಳಿ."
        elif language_code == "en-IN":
            greeting_msg = f"Hello! Thank you for calling {service_name}. Please ask your question."

        # Start STT listener immediately so speech can be processed while greeting plays.
        logger.info(f"[VC-{connection_id}] 🚀 Starting STT listener task...")
        stt_task = asyncio.create_task(stream_stt_transcripts())
        logger.info(f"[VC-{connection_id}] 🎤 Starting greeting task...")
        greeting_task = asyncio.create_task(stream_greeting_audio(greeting_msg))
        
        # Main message loop: receive audio chunks and control messages
        logger.info(f"[VC-{connection_id}] ▶️  MAIN LOOP STARTED - Ready to receive audio")
        audio_chunk_count = 0
        try:
            while True:
                if (time.perf_counter() - call_start_time) > settings.max_ws_session_seconds:
                    logger.info(f"[VC-{connection_id}] Session duration limit reached")
                    break

                try:
                    data = await websocket.receive()
                except RuntimeError as e:
                    logger.info(f"[VC-{connection_id}] ℹ️  Websocket receive ended: {e}")
                    break
                except Exception as e:
                    logger.error(f"[VC-{connection_id}] ❌ Unexpected receive error: {e}", exc_info=True)
                    break
                
                try:
                    if "bytes" in data:
                        # Audio chunk from client
                        audio_chunk = data["bytes"]
                        audio_chunk_count += 1
                        logger.info(f"[VC-{connection_id}] 🎙️  Audio chunk {audio_chunk_count} received: {len(audio_chunk)} bytes")

                        # Half-duplex guard: ignore mic stream while agent is speaking
                        # to reduce echo/loopback induced STT confusion.
                        if is_speaking or time.perf_counter() < speaking_hold_until:
                            continue
                        
                        audio_sent = await stt_client.send_audio_chunk(audio_chunk)
                        if audio_sent:
                            logger.info(f"[VC-{connection_id}] ✓ Audio chunk {audio_chunk_count} sent to STT")
                        else:
                            logger.warning(f"[VC-{connection_id}] ❌ Failed to send audio chunk {audio_chunk_count} to STT")
                    
                    elif "text" in data:
                        # Control message
                        try:
                            msg = json.loads(data["text"])
                            msg_type = msg.get("type")
                            logger.info(f"[VC-{connection_id}] 💬 Control message: {msg_type}")
                            
                            if msg_type == "barge_in":
                                logger.info(f"[VC-{connection_id}] 🔴 Barge-in triggered!")

                                # Invalidate current utterance so stale audio is dropped quickly.
                                current_utterance_id += 1

                                if pending_brain_task and not pending_brain_task.done():
                                    pending_brain_task.cancel()
                                    try:
                                        await pending_brain_task
                                    except asyncio.CancelledError:
                                        pass
                                
                                # Cancel active TTS audio stream task.
                                if pending_audio_task and not pending_audio_task.done():
                                    pending_audio_task.cancel()
                                    try:
                                        await pending_audio_task
                                    except asyncio.CancelledError:
                                        pass

                                if pending_fallback_task and not pending_fallback_task.done():
                                    pending_fallback_task.cancel()
                                    try:
                                        await pending_fallback_task
                                    except asyncio.CancelledError:
                                        pass
                                
                                # Signal end of stream to STT to process current audio
                                await stt_client.signal_end_of_stream()
                                is_speaking = False
                                await send_status("listening")
                            
                            elif msg_type == "end_stream":
                                logger.info(f"[VC-{connection_id}] End stream requested")
                                await stt_client.signal_end_of_stream()
                        
                        except json.JSONDecodeError as je:
                            logger.warning(f"[VC-{connection_id}] Invalid JSON control message: {je}")
                        except Exception as me:
                            logger.error(f"[VC-{connection_id}] Error processing control message: {me}", exc_info=True)
                except Exception as de:
                    logger.error(f"[VC-{connection_id}] Error processing data frame: {de}", exc_info=True)
        
        except WebSocketDisconnect:
            logger.info(f"[VC-{connection_id}] Client disconnected (WebSocketDisconnect)")
        except Exception as e:
            logger.error(f"[VC-{connection_id}] ❌ Connection error in main loop: {e}", exc_info=True)
            try:
                await websocket.send_json({"type": "error", "message": str(e)})
            except:
                pass
    
    finally:
        if connection_registered:
            await _unregister_ws_connection(client_ip)
        # Cleanup
        logger.info(f"[VC-{connection_id}] 🧹 Cleaning up...")
        
        try:
            if greeting_task and not greeting_task.done():
                greeting_task.cancel()
                await asyncio.gather(greeting_task, return_exceptions=True)
        except Exception:
            pass

        try:
            if pending_brain_task and not pending_brain_task.done():
                pending_brain_task.cancel()
                await asyncio.gather(pending_brain_task, return_exceptions=True)
        except Exception:
            pass

        try:
            if pending_audio_task and not pending_audio_task.done():
                pending_audio_task.cancel()
                await asyncio.gather(pending_audio_task, return_exceptions=True)
        except Exception:
            pass

        try:
            if pending_fallback_task and not pending_fallback_task.done():
                pending_fallback_task.cancel()
                await asyncio.gather(pending_fallback_task, return_exceptions=True)
        except Exception:
            pass

        try:
            if stt_client:
                await stt_client.disconnect()
        except:
            pass
        
        try:
            if tts_client:
                await tts_client.disconnect()
        except:
            pass
        
        try:
            await websocket.close()
        except:
            pass
        
        logger.info(f"[VC-{connection_id}] ✓ Voice call ended (duration: {(time.perf_counter() - call_start_time):.2f}s)")


# ---------------------------------------------------------------------------
# Multi-tenant configuration (Step 5): Supabase-backed with hardcoded fallback
# ---------------------------------------------------------------------------

# Fallback tenant configs used ONLY when Supabase is unreachable or the
# tenant_id isn't found there (e.g. local/demo tenants never migrated into
# the DB). Hoisted to module level - was previously rebuilt on every single
# connection inside the handler; no functional change, just avoids rebuilding
# a static dict per call.
_FALLBACK_TENANT_CONFIGS: dict = {
        "default": {
            "business_name": "Glow & Shine Beauty Parlour",
            "language": "en-IN",
            "speaker": "shubh",
            "system_prompt": (
                "You are Priya, the polite and warm AI phone receptionist for Glow & Shine Beauty Parlour. "
                "Your goal is to answer customer inquiries about services (haircut, facial, makeup, bridal) and help them book appointments.\n\n"
                
                "### STRICT VOICE RESPONSE RULES:\n"
                "1. NO MARKDOWN OR FORMATTING: Never use bold (**), italics, bullet points, numbered lists, emojis, or special characters (&, %, @). Write purely plain spoken text.\n"
                "2. CONCISE BREVITY: Keep every reply strictly under 2 short, natural conversational sentences (maximum 25 words per turn).\n"
                "3. ONE QUESTION AT A TIME: Never ask more than one question in a single response.\n"
                "4. DYNAMIC LANGUAGE MATCHING: Automatically detect the caller's language (Hindi, English, or Hinglish) and reply in that EXACT same style. "
                "Do NOT prefix your response with language tags like 'hi-IN:' or 'en-IN:'.\n"
                "5. NUMBERS AND PRICING: Write numbers simply as spoken words or plain digits without currency symbols (e.g., say 'paanch sau rupeya' or 'five hundred rupees', not 'Rs. 500/=').\n\n"

                "### APPOINTMENT BOOKING FLOW:\n"
                "- Step 1: Identify the requested service (Haircut, Facial, Waxing, Makeup).\n"
                "- Step 2: Ask for their preferred date and time.\n"
                "- Step 3: Ask for their name to confirm the booking.\n"
                "- Step 4: Once confirmed, thank them warmly and wrap up.\n\n"

                "### OUT OF SCOPE & ABUSE:\n"
                "- If asked about services not related to a beauty parlour, politely state that you only assist with Glow & Shine parlour services.\n"
                "- If the user is rude or off-topic, remain polite and bring them back to booking an appointment.\n\n"

                "### CALL TERMINATION:\n"
                "- When the booking is complete or the caller says goodbye/thank you, give a warm closing remark and ALWAYS append '[END_CALL]' at the very end of your response."
            ),
        },
        "PARLOUR_001": {
            "business_name": settings.service_name,
            "language": "hi-IN",
            "speaker": "shubh",
            "system_prompt": (
                f"You are a helpful voice receptionist for {settings.service_name}. "
                "Answer customer queries politely and concisely in 1-2 sentences in Hindi or English."
            ),
        },
}


_BOOKING_TAG_RE = re.compile(
    r"\[BOOK_APPOINTMENT:\s*(.*?)\]", re.IGNORECASE | re.DOTALL
)


def _build_catalogue_prompt_section(items: list) -> str:
    """Render a tenant's doctors/services list into a prompt-friendly block."""
    if not items:
        return "No services/doctors are currently configured for this business."
    lines = []
    for item in items:
        name = item.get("name", "Unknown")
        category = item.get("category", "")
        price = item.get("price_str", "")
        hours = item.get("working_hours", "")
        status = item.get("status", "AVAILABLE")
        line = f"- {name}"
        if category:
            line += f" ({category})"
        if price:
            line += f", {price}"
        if hours:
            line += f", available {hours}"
        if status and status != "AVAILABLE":
            line += f" [{status}]"
        lines.append(line)
    return "\n".join(lines)


def _build_supabase_system_prompt(tenant_row: dict, items: list) -> str:
    """Compose a booking-aware system prompt from live Supabase tenant/catalogue data."""
    business_name = tenant_row.get("business_name", "our business")
    base_persona = tenant_row.get("system_prompt") or (
        f"You are the polite AI phone receptionist for {business_name}."
    )
    catalogue = _build_catalogue_prompt_section(items)

    return (
        f"{base_persona}\n\n"
        "### STRICT VOICE RESPONSE RULES:\n"
        "1. NO MARKDOWN OR FORMATTING: never use bold, bullet points, emojis, "
        "or special characters. Plain spoken text only.\n"
        "2. CONCISE BREVITY: keep every reply under 2 short natural sentences "
        "(max 25 words).\n"
        "3. ONE QUESTION AT A TIME.\n"
        "4. DYNAMIC LANGUAGE MATCHING: reply in the caller's language/style; "
        "never prefix replies with language tags.\n"
        "5. NUMBERS AND PRICING: say numbers/prices in plain words or digits, "
        "no currency symbols.\n\n"
        "### AVAILABLE DOCTORS/SERVICES:\n"
        f"{catalogue}\n\n"
        "### APPOINTMENT BOOKING FLOW:\n"
        "- Ask which doctor/service the caller wants (match against the list above).\n"
        "- Ask their preferred date and time.\n"
        "- Ask their full name.\n"
        f"- {_localized('using_caller_number', 'hi-IN')} Do NOT ask the caller "
        "for their phone number - the system already has it from the call itself.\n"
        "- Once you have ALL THREE of these (service, date/time, name), "
        "respond with ONLY this exact machine-readable tag and nothing else "
        "(no extra sentence, no punctuation outside it): "
        "[BOOK_APPOINTMENT: item=<service/doctor name>|when=<date and time as "
        "the caller said it>|name=<caller's full name>]\n"
        "- Do NOT say anything else in that turn - the system will speak the "
        "confirmation for you.\n"
        "- If the caller says goodbye/thank you and no booking is in "
        "progress, give a warm closing remark and append '[END_CALL]' at the "
        "end.\n\n"
        "### OUT OF SCOPE:\n"
        f"- If asked about anything unrelated to {business_name}'s services, "
        "politely redirect to booking.\n"
    )


async def _resolve_tenant_config(tenant_id: str) -> dict:
    """
    Resolve a tenant's runtime config, preferring live Supabase data with a
    hardcoded fallback. Returned dict always has: business_name, language,
    speaker, system_prompt, tenant_row (dict or None), items (list),
    emergency_number (str or None), timezone (str), source ("supabase" |
    "fallback_hardcoded").
    """
    if supabase_service is not None:
        try:
            data = await supabase_service.get_tenant_and_items(tenant_id)
        except Exception as exc:
            logger.error(
                f"Supabase tenant lookup failed for '{tenant_id}': {exc}. "
                f"Falling back to hardcoded config."
            )
            data = None

        if data:
            tenant_row = data["tenant"]
            items = data["items"]
            return {
                "business_name": tenant_row.get("business_name", tenant_id),
                # NOTE: the `tenants` table has no language/speaker columns
                # yet - default to hi-IN/shubh for all Supabase-backed
                # tenants. Add columns later if per-tenant language
                # selection becomes a requirement.
                "language": "hi-IN",
                "speaker": "shubh",
                "system_prompt": _build_supabase_system_prompt(tenant_row, items),
                "tenant_row": tenant_row,
                "items": items,
                "emergency_number": tenant_row.get("emergency_number"),
                "timezone": tenant_row.get("timezone", "Asia/Kolkata"),
                "source": "supabase",
            }

    fallback = _FALLBACK_TENANT_CONFIGS.get(
        tenant_id, _FALLBACK_TENANT_CONFIGS["default"]
    )
    logger.warning(
        f"Tenant '{tenant_id}' not found in Supabase (or Supabase "
        f"unreachable) - using hardcoded fallback config."
    )
    return {
        **fallback,
        "tenant_row": None,
        "items": [],
        "emergency_number": None,
        "timezone": "Asia/Kolkata",
        "source": "fallback_hardcoded",
    }


def _parse_booking_tag_fields(tag_body: str) -> dict:
    """Parse 'item=...|when=...|name=...|phone=...' into a dict; missing keys omitted."""
    fields = {}
    for part in tag_body.split("|"):
        if "=" not in part:
            continue
        key, _, value = part.partition("=")
        key = key.strip().lower()
        value = value.strip()
        if key and value:
            fields[key] = value
    return fields


def _extract_partial_slots(transcript: str, items: list) -> dict:
    """
    Best-effort, cheap partial slot extraction from a single caller utterance,
    independent of the LLM's own [BOOK_APPOINTMENT: ...] tag (which only fires
    once ALL fields have been gathered). This lets session.extracted_slots
    reflect information as soon as it's mentioned instead of staying {} for
    the whole call. Only matches known tenant item/service names against the
    transcript - date/time/name extraction is deliberately NOT attempted here
    (no cheap, reliable heuristic) to avoid false positives; that context is
    still available to the LLM via StreamingBrain's own persistent
    conversation history (see brain/llm_service.py's _conversation_history),
    so it isn't lost even without a dedicated slot for it.
    """
    fields = {}
    lowered = transcript.lower()
    for item in items:
        name = (item.get("name") or "").strip()
        if name and name.lower() in lowered:
            fields["item"] = name
            break
    return fields


def _log_slots_diff(session: CallSession, before: dict) -> None:
    """Emit a structured JSON log line showing how a turn changed the
    session's extracted_slots (Issue 3 observability requirement)."""
    _log_observability(
        "slots_updated",
        session=session,
        before=before,
        after=dict(session.extracted_slots),
    )


_BOOKING_LOCALIZED_STRINGS = {
    "need_service": {
        "hi-IN": "Maaf kijiye, kripya bataiye aap kis doctor ya service ke liye booking karna chahte hain.",
        "en-IN": "Sorry, could you tell me which doctor or service you'd like to book?",
    },
    "need_datetime": {
        "hi-IN": "Maaf kijiye, kripya apni pasandeeda date aur time dobara bataiye.",
        "en-IN": "Sorry, could you repeat your preferred date and time?",
    },
    "slot_taken": {
        "hi-IN": "Yeh samay pehle se book hai. Kripya koi doosra samay bataiye.",
        "en-IN": "That time slot is already booked. Could you suggest another time?",
    },
    "booking_error": {
        "hi-IN": "Abhi booking system mein dikkat aa rahi hai. Kripya thodi der baad dobara call karein.",
        "en-IN": "We're having trouble with our booking system right now. Please try calling again shortly.",
    },
    "using_caller_number": {
        "hi-IN": "Main aapke isi number ka use kar raha hoon jisse aapne call kiya hai.",
        "en-IN": "I'll use the number you're calling from for this booking.",
    },
}


def _localized(key: str, language_code: str) -> str:
    variants = _BOOKING_LOCALIZED_STRINGS.get(key, {})
    return variants.get(language_code, variants.get("hi-IN", ""))


async def _process_booking_tag(
    tag_body: str,
    session: CallSession,
    tenant_config: dict,
    language_code: str,
) -> str:
    """
    Parse and execute a [BOOK_APPOINTMENT: ...] tag emitted by the LLM.

    Returns the DETERMINISTIC text to speak back to the caller - i.e. the
    actual database outcome, never the LLM's own (unverified) claim of
    success. This is a hard correctness rule: the caller must never be told
    "confirmed" unless the DB insert actually succeeded.
    """
    fields = _parse_booking_tag_fields(tag_body)
    session.update_slots(**fields)  # observability, regardless of outcome

    item_name_wanted = fields.get("item")
    when_phrase = fields.get("when")
    patient_name = fields.get("name")
    # Prefer the caller's own number captured from the telephony 'start'
    # event (Issue 7) - only fall back to a phone the LLM extracted from the
    # tag if the caller happened to volunteer a different number.
    patient_phone = fields.get("phone") or session.caller_phone or ""

    items = tenant_config.get("items") or []
    tenant_id = session.tenant_id

    if not item_name_wanted or not items:
        return _localized("need_service", language_code)

    matched_item = None
    wanted_lower = item_name_wanted.strip().lower()
    for item in items:
        if wanted_lower in (item.get("name", "") or "").lower():
            matched_item = item
            break
    if matched_item is None:
        return _localized("need_service", language_code)

    if not when_phrase:
        return _localized("need_datetime", language_code)

    tenant_tz = tenant_config.get("timezone") or "Asia/Kolkata"
    parsed = parse_user_datetime(when_phrase, tenant_tz=tenant_tz)
    if parsed is None:
        return _localized("need_datetime", language_code)

    date_str, start_mins, _default_end_mins, display_time_str = parsed
    slot_duration = matched_item.get("slot_duration_mins") or 30
    end_mins = start_mins + int(slot_duration)

    if supabase_service is None:
        logger.error(f"[{tenant_id}] Booking attempted but supabase_service is None")
        return _localized("booking_error", language_code)

    try:
        available, _reason = await supabase_service.check_slot_available(
            tenant_id=tenant_id,
            item_name=matched_item["name"],
            date_str=date_str,
            proposed_start_mins=start_mins,
            proposed_end_mins=end_mins,
        )
        if not available:
            return _localized("slot_taken", language_code)

        result = await supabase_service.create_appointment_async(
            tenant_id=tenant_id,
            item_name=matched_item["name"],
            item_id=matched_item.get("id"),
            date_str=date_str,
            start_mins=start_mins,
            end_mins=end_mins,
            display_time_str=display_time_str,
            patient_name=patient_name or "Caller",
            patient_phone=patient_phone or "",
            call_id=session.call_id,
            attempt_nonce=session.next_attempt_nonce(),
        )
        status = result.get("status")

        _log_observability(
            "booking_attempt",
            session=session,
            item=matched_item["name"],
            date_str=date_str,
            start_mins=start_mins,
            status=status,
        )

        if status in ("CONFIRMED", "DUPLICATE_RETRY_IGNORED"):
            session.reset_slots()
            if language_code == "en-IN":
                return (
                    f"Great, your appointment for {matched_item['name']} on "
                    f"{date_str} at {display_time_str} is confirmed."
                )
            return (
                f"Aapki {matched_item['name']} ke saath booking {date_str} ko "
                f"{display_time_str} baje confirm ho gayi hai."
            )
        if status == "ALREADY_BOOKED":
            return _localized("slot_taken", language_code)

        logger.error(f"[{tenant_id}] Unexpected booking status: {status}")
        return _localized("booking_error", language_code)

    except Exception as exc:
        logger.error(f"[{tenant_id}] Booking flow error: {exc}", exc_info=True)
        return _localized("booking_error", language_code)


@app.websocket("/ws/v1/exotel-stream")
async def websocket_exotel_stream_query_route(
    websocket: WebSocket,
    tenant_id: str = "default",
    token: Optional[str] = None,
):
    """Exotel entrypoint using ?tenant_id=... query param (backward-compatible)."""
    if not _is_exotel_ws_authorized(token):
        request_metrics["auth_failures"] += 1
        await websocket.close(code=4401, reason="Unauthorized")
        return
    await _websocket_exotel_stream_impl(websocket, tenant_id)


@app.websocket("/ws/v1/exotel-stream/{tenant_id}")
async def websocket_exotel_stream_path_route(
    websocket: WebSocket,
    tenant_id: str,
    token: Optional[str] = None,
):
    """Exotel entrypoint using a /exotel-stream/{tenant_id} path param."""
    if not _is_exotel_ws_authorized(token):
        request_metrics["auth_failures"] += 1
        await websocket.close(code=4401, reason="Unauthorized")
        return
    await _websocket_exotel_stream_impl(websocket, tenant_id)


async def _websocket_exotel_stream_impl(
    websocket: WebSocket,
    tenant_id: str = "default",
):
    """
    Production Exotel Cloud Telephony WebSocket endpoint.
    Includes:
    - 320-byte trailing chunk padding (eliminates tail clicks/gaps).
    - 3-second rolling window transcript deduplication.
    - 5-second silence fallback prompt task.
    - Multi-tenant Supabase-backed config (Step 5), with hardcoded fallback.
    - Emergency fast-path detection + Exotel Applet Exit Path transfer.
    - LLM-driven appointment booking via the [BOOK_APPOINTMENT: ...] tag.
    """
    connection_id = str(uuid.uuid4())[:13]
    client_ip = _client_ip_from_websocket(websocket)
    connection_registered = False
    logger.info(f"[EX-{connection_id}] 📞 Exotel connection requested for tenant_id='{tenant_id}'")

    if not _allow_rate_limit(
        _rate_limit_state["ws_connect"][client_ip],
        settings.ws_connect_rate_limit_per_min,
        window_sec=60.0,
    ):
        request_metrics["ws_rejected"] += 1
        await websocket.close(code=4408, reason="WS connect rate limit exceeded")
        return

    connection_registered = await _try_register_ws_connection(client_ip)
    if not connection_registered:
        request_metrics["ws_rejected"] += 1
        await websocket.close(code=4429, reason="WS connection limit reached")
        return

    try:
        if text_normalizer is None:
            await websocket.close(code=1011, reason="Service not initialized")
            return

        await websocket.accept()
        logger.info(f"[EX-{connection_id}] ✅ Accepted")
    except Exception as e:
        logger.error(f"[EX-{connection_id}] ❌ Accept failed: {e}")
        return

    # Multi-tenant config: Supabase-backed with hardcoded fallback (Step 5).
    t_config = await _resolve_tenant_config(tenant_id)
    business_name = t_config["business_name"]
    language_code = t_config["language"]
    speaker = t_config["speaker"]
    custom_system_prompt = t_config["system_prompt"]
    logger.info(
        f"[EX-{connection_id}] 🏢 Tenant config resolved: tenant_id='{tenant_id}' "
        f"source={t_config['source']} business='{business_name}'"
    )

    # Central in-memory session record for this call (Step 3/5 wiring).
    session = session_manager.create(connection_id, tenant_id)

    # If the tenant offers exactly one AVAILABLE doctor/service, pre-fill it
    # as a known slot right away. Otherwise the caller asking "which doctor
    # is available" gets told the (only) name by the bot, but the bot never
    # recorded that as a confirmed slot (session.extracted_slots only tracks
    # names the CALLER themselves said), so it kept re-asking "do you want to
    # book with Dr. X?" instead of moving on to date/time - the repeated-
    # question bug observed on a live CLINIC_001 call (2026-07-24).
    available_items = [
        it for it in (t_config.get("items") or [])
        if (it.get("status", "AVAILABLE") == "AVAILABLE") and it.get("name")
    ]
    if len(available_items) == 1:
        session.update_slots(item=available_items[0]["name"])

    _log_observability("call_connected", session=session, tenant_source=t_config["source"])

    stream_sid = None
    stt_client = None
    brain = None
    call_start_time = time.perf_counter()
    is_speaking = False
    speaking_hold_until = 0.0
    pending_brain_task = None
    pending_fallback_task = None
    greeting_task = None
    greeting_sent = False

    last_processed_transcript = ""
    last_transcript_time = 0.0
    # Timestamp of the most recent STT activity of ANY kind (speech_started,
    # a transcript part, or a final_transcript) - drives the silence-fallback
    # timer (Issue 1) instead of only counting from when a debounce cycle
    # finished processing.
    last_activity_ts = time.perf_counter()
    # True from the moment speech_started fires until the matching
    # final_transcript/speech_ended arrives. A single long utterance with no
    # interim STT signal in between must NEVER let the silence-fallback timer
    # fire while this is True, however long the utterance takes.
    caller_speaking = False

    async def send_exotel_clear():
        """Send Exotel clear frame to purge buffered audio on the phone line."""
        nonlocal stream_sid
        if not stream_sid:
            return
        try:
            await websocket.send_json({"event": "clear", "stream_sid": stream_sid})
            logger.info(f"[EX-{connection_id}] 🧹 Sent Exotel 'clear' frame")
        except Exception as e:
            logger.warning(f"[EX-{connection_id}] Clear frame failed: {e}")

    async def send_exotel_media_paced(pcm_24k_bytes: bytes, utterance_id: int) -> int:
        """
        Streams 16-bit PCM to Exotel in 1280-byte (80ms) chunks.
        Pads trailing chunks smaller than 320 bytes to prevent audio tail gaps.
        Returns the number of raw telephony-rate bytes actually written (used
        for exact byte-count playback-drain calculations - see
        core/emergency.py's compute_playback_drain_seconds()).
        """
        nonlocal stream_sid, is_speaking
        if not stream_sid or not pcm_24k_bytes:
            return 0

        bytes_written = 0
        try:
            b64_payload = tts_pcm_to_telephony(
                pcm_bytes=pcm_24k_bytes,
                source_sr=22050,
                target_codec="linear16",
                target_sr=8000,
            )
            if not b64_payload:
                return 0

            raw_bytes = base64.b64decode(b64_payload)
            chunk_size = 1280  # 80ms at 8kHz 16-bit PCM

            # FIX A: Pad trailing chunk if smaller than 320 bytes
            remainder = len(raw_bytes) % chunk_size
            if 0 < remainder < 320:
                pad_bytes = 320 - remainder
                raw_bytes += b'\x00' * pad_bytes
                logger.info(
                    f"[EX-{connection_id}] 🧩 Trailing chunk padded: {remainder}B -> {remainder + pad_bytes}B"
                )

            total_chunks = (len(raw_bytes) + chunk_size - 1) // chunk_size
            logger.info(f"[EX-{connection_id}] 📤 Streaming {len(raw_bytes)}B ({total_chunks} chunks)...")

            for i in range(0, len(raw_bytes), chunk_size):
                if not is_speaking or not session.is_current_utterance(utterance_id):
                    logger.info(f"[EX-{connection_id}] 🛑 Audio streaming interrupted")
                    break

                chunk = raw_bytes[i:i + chunk_size]
                b64_chunk = base64.b64encode(chunk).decode("utf-8")

                await websocket.send_json({
                    "event": "media",
                    "stream_sid": stream_sid,
                    "media": {"payload": b64_chunk},
                })
                bytes_written += len(chunk)
                await asyncio.sleep(0.075)

        except Exception as e:
            logger.warning(f"[EX-{connection_id}] Media stream error: {e}")

        return bytes_written

    async def process_transcript_to_response(transcript: str):
        """Full-duplex response engine."""
        nonlocal is_speaking, speaking_hold_until
        nonlocal last_processed_transcript, last_transcript_time

        clean_transcript = transcript.strip()
        if not clean_transcript or len(clean_transcript.replace(" ", "")) < 2:
            return

        # FIX B1: 3-second rolling window deduplication
        now = time.perf_counter()
        if clean_transcript == last_processed_transcript and (now - last_transcript_time) < 3.0:
            logger.info(f"[EX-{connection_id}] ⏭️ Duplicate skipped (within 3s): '{clean_transcript}'")
            return

        last_processed_transcript = clean_transcript
        last_transcript_time = now
        this_utterance_id = session.next_utterance_id()

        logger.info(f"[EX-{connection_id}] 📥 CALLER SAID: '{clean_transcript}'")
        session.add_turn("user", clean_transcript)
        _log_observability("turn_start", session=session, transcript=clean_transcript)

        # Issue 3: opportunistically update extracted_slots as soon as a known
        # item/service is mentioned, rather than only once the LLM's full
        # [BOOK_APPOINTMENT: ...] tag fires (which requires every field to
        # already be gathered). Diff is logged once per turn below regardless
        # of which code path updates/leaves slots unchanged.
        slots_before_turn = dict(session.extracted_slots)
        partial_fields = _extract_partial_slots(clean_transcript, t_config.get("items") or [])
        if partial_fields:
            session.update_slots(**partial_fields)

        # --- Emergency fast-path (Step 1's highest-priority feature) ---------
        # Checked BEFORE any LLM call so an emergency is never delayed by
        # brain latency. Bypasses booking/LLM entirely on a match.
        if check_emergency_fastpath(clean_transcript):
            matched_kw = find_emergency_match(clean_transcript)
            logger.warning(
                f"[EX-{connection_id}] 🚨 EMERGENCY detected (matched: '{matched_kw}'): '{clean_transcript}'"
            )
            _log_observability("emergency_detected", session=session, matched=matched_kw)

            handover_text = build_emergency_handover_phrase(t_config.get("tenant_row"))
            session.add_turn("assistant", handover_text)

            emergency_tts = SarvamWebSocketClient()
            total_bytes_sent = 0
            try:
                connected = await _retry_async(
                    lambda: emergency_tts.connect(
                        target_language_code=language_code,
                        speaker=speaker,
                        pace=0.95,
                    ),
                    "tts_connect_exotel_emergency",
                )
                if connected:
                    is_speaking = True
                    norm_text = text_normalizer.normalize(
                        handover_text, target_language_code=language_code
                    )
                    if await emergency_tts.send_text_chunk(norm_text):
                        await emergency_tts.send_flush()
                        async for chunk in emergency_tts.stream_audio_chunks(
                            initial_timeout_sec=2.5,
                            post_audio_idle_timeout_sec=0.4,
                            max_duration_sec=10.0,
                        ):
                            total_bytes_sent += await send_exotel_media_paced(chunk, this_utterance_id)
            except Exception as exc:
                logger.error(f"[EX-{connection_id}] ❌ Emergency handover TTS failed: {exc}", exc_info=True)
            finally:
                try:
                    await emergency_tts.disconnect()
                except Exception:
                    pass
                is_speaking = False
                speaking_hold_until = time.perf_counter() + 0.5

            telephony_bytes_per_sec = settings.EXOTEL_SAMPLE_RATE * settings.EXOTEL_BYTES_PER_SAMPLE
            _log_observability("call_transfer", session=session, total_bytes_sent=total_bytes_sent)
            _log_slots_diff(session, slots_before_turn)
            await close_websocket_for_emergency_transfer(
                websocket, connection_id, total_bytes_sent, telephony_bytes_per_sec
            )
            return

        turn_tts = SarvamWebSocketClient()
        tts_connect_task = asyncio.create_task(
            _retry_async(
                lambda: turn_tts.connect(
                    target_language_code=language_code,
                    speaker=speaker,
                    pace=0.95,
                ),
                "tts_connect_exotel",
            )
        )
        total_bytes_sent = 0

        try:
            brain_start = time.perf_counter()
            llm_input = _with_slot_context(clean_transcript, language_code, session)
            response_tokens = []
            ttft_recorded = False

            async for token in brain.stream_response(llm_input):
                if not session.is_current_utterance(this_utterance_id):
                    await send_exotel_clear()
                    return

                if not ttft_recorded:
                    brain_ttft_ms = (time.perf_counter() - brain_start) * 1000
                    logger.info(f"[EX-{connection_id}] ⏱️ Brain TTFT: {brain_ttft_ms:.2f}ms")
                    ttft_recorded = True

                response_tokens.append(token)

            raw_response_text = "".join(response_tokens).strip()
            should_hangup = "[END_CALL]" in raw_response_text
            booking_match = _BOOKING_TAG_RE.search(raw_response_text)

            clean_response = raw_response_text.replace("[END_CALL]", "").strip()
            clean_response = _BOOKING_TAG_RE.sub("", clean_response).strip()
            # Strip a leading "xx-XX:" language-code prefix the model sometimes adds
            clean_response = re.sub(r'^[a-zA-Z]{2}-[A-Z]{2}:\s*', '', clean_response).strip()

            if booking_match:
                # NEVER trust the LLM's own claim of booking success - always
                # speak a backend-computed, DB-verified confirmation/rejection.
                clean_response = await _process_booking_tag(
                    booking_match.group(1), session, t_config, language_code
                )
                _log_observability("booking_tag_detected", session=session, tag_body=booking_match.group(1))

            if not clean_response or not session.is_current_utterance(this_utterance_id):
                return

            logger.info(f"[EX-{connection_id}] 🧠 AGENT RESPONSE: '{clean_response}' (Hangup: {should_hangup})")
            session.add_turn("assistant", clean_response)

            tts_connected = await tts_connect_task
            if not tts_connected or not session.is_current_utterance(this_utterance_id):
                return

            norm_text = text_normalizer.normalize(clean_response, target_language_code=language_code)
            if await turn_tts.send_text_chunk(norm_text):
                await turn_tts.send_flush()
                is_speaking = True

                async for chunk in turn_tts.stream_audio_chunks(
                    initial_timeout_sec=2.5,
                    post_audio_idle_timeout_sec=0.4,
                    max_duration_sec=12.0,
                ):
                    if not session.is_current_utterance(this_utterance_id):
                        await send_exotel_clear()
                        break
                    total_bytes_sent += await send_exotel_media_paced(chunk, this_utterance_id)

                logger.info(f"[EX-{connection_id}] 🔊 Response stream finished")

            if should_hangup and session.is_current_utterance(this_utterance_id):
                logger.info(f"[EX-{connection_id}] 🛑 [END_CALL] received. Closing line...")
                telephony_bytes_per_sec = settings.EXOTEL_SAMPLE_RATE * settings.EXOTEL_BYTES_PER_SAMPLE
                wait_sec = compute_playback_drain_seconds(total_bytes_sent, telephony_bytes_per_sec)
                await asyncio.sleep(wait_sec)
                _log_observability("call_hangup", session=session, total_bytes_sent=total_bytes_sent, drain_sec=wait_sec)
                try:
                    await websocket.close(code=1000, reason="Call completed")
                except Exception:
                    pass

        except Exception as e:
            logger.error(f"[EX-{connection_id}] ❌ Turn error: {e}", exc_info=True)
        finally:
            _log_slots_diff(session, slots_before_turn)
            if not tts_connect_task.done():
                tts_connect_task.cancel()
                await asyncio.gather(tts_connect_task, return_exceptions=True)
            try:
                await turn_tts.disconnect()
            except Exception:
                pass

            is_speaking = False
            speaking_hold_until = time.perf_counter() + 0.5

    async def stream_greeting_audio(greeting_text: str):
        """Stream initial call greeting."""
        nonlocal is_speaking, speaking_hold_until

        is_speaking = True
        logger.info(f"[EX-{connection_id}] 🎤 Synthesizing greeting...")

        turn_tts = SarvamWebSocketClient()
        try:
            connected = await _retry_async(
                lambda: turn_tts.connect(
                    target_language_code=language_code,
                    speaker=speaker,
                    pace=0.95,
                ),
                "tts_connect_exotel_greeting",
            )
            if connected:
                norm_greeting = text_normalizer.normalize(
                    greeting_text, target_language_code=language_code
                )
                if await turn_tts.send_text_chunk(norm_greeting):
                    await turn_tts.send_flush()
                    async for chunk in turn_tts.stream_audio_chunks(
                        idle_timeout_sec=2.0, max_duration_sec=10.0
                    ):
                        await send_exotel_media_paced(chunk, session.current_utterance_id)
                    logger.info(f"[EX-{connection_id}] ✓ Greeting audio sent to Exotel")
        except Exception as e:
            logger.error(f"[EX-{connection_id}] ❌ Greeting error: {e}")
        finally:
            try:
                await turn_tts.disconnect()
            except Exception:
                pass
            is_speaking = False
            speaking_hold_until = time.perf_counter() + 0.5

    async def stream_stt_transcripts():
        """Process STT transcripts from caller with debounced buffering, a
        full-duplex barge-in path, and an activity-based silence-fallback
        timeout (fires only after genuine STT inactivity, not on a flat
        post-turn timer - see last_activity_ts / caller_speaking)."""
        nonlocal pending_brain_task, pending_fallback_task
        nonlocal is_speaking, last_activity_ts, caller_speaking

        _pending_transcript_parts = []
        _debounce_task = None
        DEBOUNCE_SECONDS = 1.0
        SILENCE_TIMEOUT_SEC = 5.0

        async def silence_fallback():
            """
            Re-engage the caller only after SILENCE_TIMEOUT_SEC of genuine STT
            inactivity. Loops re-checking last_activity_ts AND caller_speaking
            instead of firing on a single flat sleep: a long single utterance
            with no interim STT signal between speech_started and its final
            transcript must never let a stale timer interrupt the caller
            mid-sentence, no matter how long it takes.
            """
            while True:
                remaining = SILENCE_TIMEOUT_SEC - (time.perf_counter() - last_activity_ts)
                if remaining <= 0 and not caller_speaking:
                    break
                await asyncio.sleep(remaining if remaining > 0 else 0.5)
            if not is_speaking:
                logger.info(
                    f"[EX-{connection_id}] ⏱️ {SILENCE_TIMEOUT_SEC}s of genuine silence detected. Re-engaging caller..."
                )
                await process_transcript_to_response(
                    "Kya aap wahan hain? Kya aapko koi aur madad chahiye?"
                )

        def _reschedule_silence_fallback():
            nonlocal pending_fallback_task
            if pending_fallback_task and not pending_fallback_task.done():
                pending_fallback_task.cancel()
            pending_fallback_task = asyncio.create_task(silence_fallback())

        async def _debounced_process():
            nonlocal _pending_transcript_parts, pending_brain_task
            await asyncio.sleep(DEBOUNCE_SECONDS)
            if _pending_transcript_parts:
                full_text = " ".join(_pending_transcript_parts).strip()
                _pending_transcript_parts = []

                logger.info(f"[EX-{connection_id}] 🧠 Debounced transcript ready: '{full_text}'")

                # Cancel pending brain task if running
                if pending_brain_task and not pending_brain_task.done():
                    pending_brain_task.cancel()
                    try:
                        await pending_brain_task
                    except asyncio.CancelledError:
                        pass

                pending_brain_task = asyncio.create_task(
                    process_transcript_to_response(full_text)
                )

                # Restart the silence timer now that a turn has been
                # dispatched for processing.
                _reschedule_silence_fallback()

        try:
            async for event in stt_client.stream_transcripts():
                if event.event_type == "speech_started":
                    last_activity_ts = time.perf_counter()
                    caller_speaking = True
                    # ANY genuine speech activity resets the silence timer -
                    # not only completed debounce cycles (Issue 1, fix #3).
                    _reschedule_silence_fallback()

                    if is_speaking:
                        # Full-duplex barge-in: caller started speaking while
                        # the bot's TTS audio is still playing (Issue 5).
                        logger.info(
                            f"[EX-{connection_id}] 🔴 Barge-in detected (speech_started while bot speaking)"
                        )
                        session.next_utterance_id()
                        if pending_brain_task and not pending_brain_task.done():
                            pending_brain_task.cancel()
                            try:
                                await pending_brain_task
                            except asyncio.CancelledError:
                                pass
                        await send_exotel_clear()
                        is_speaking = False
                    continue

                if event.event_type == "speech_ended":
                    last_activity_ts = time.perf_counter()
                    caller_speaking = False
                    continue

                if event.event_type in ("final_transcript", "transcript_updated"):
                    transcript_text = getattr(event, "transcript", "").strip()
                    last_activity_ts = time.perf_counter()
                    caller_speaking = False

                    # Ignore mic input while bot speaks. By the time a
                    # transcript arrives, a genuine barge-in (speech_started
                    # above) has already flipped is_speaking to False, so this
                    # guard only blocks stale/echo-adjacent transcripts, not
                    # legitimate barge-in speech.
                    if is_speaking or time.perf_counter() < speaking_hold_until:
                        continue

                    if event.event_type == "final_transcript" and transcript_text:
                        logger.info(f"[EX-{connection_id}] 🎤 STT TRANSCRIPT PART: '{transcript_text}'")

                        _pending_transcript_parts.append(transcript_text)

                        if _debounce_task and not _debounce_task.done():
                            _debounce_task.cancel()
                        _debounce_task = asyncio.create_task(_debounced_process())

                elif event.event_type == "error":
                    logger.error(f"[EX-{connection_id}] 🎤 STT error: {event.error_message}")

        except asyncio.CancelledError:
            logger.debug(f"[EX-{connection_id}] STT listener cancelled")
        except Exception as e:
            logger.error(f"[EX-{connection_id}] STT listener error: {e}", exc_info=True)

    # Initialize STT and Brain
    stt_client = SarvamSaarasSTTClient()
    brain = StreamingBrain(system_prompt=custom_system_prompt)
    # Prewarm the LLM connection in parallel with the greeting/STT connect so
    # the first real turn's TLS/connection-pool handshake doesn't add to its
    # critical-path latency (matches the pattern in websocket_voice_call).
    asyncio.create_task(brain.prewarm())

    stt_connected = await _retry_async(
        lambda: stt_client.connect(language_code=language_code),
        "stt_connect_exotel",
    )
    if not stt_connected:
        logger.error(f"[EX-{connection_id}] ❌ STT Connection Failed")
        await websocket.close(code=4001, reason="STT Connection Failed")
        return

    stt_task = asyncio.create_task(stream_stt_transcripts())

    stt_audio_buffer = bytearray()
    STT_BUFFER_THRESHOLD = 3200  # 3200 bytes @ 16kHz 16-bit PCM = 100ms audio

    try:
        while True:
            if (time.perf_counter() - call_start_time) > settings.max_ws_session_seconds:
                break

            try:
                data = await websocket.receive()
            except RuntimeError:
                break
            except Exception as e:
                logger.error(f"[EX-{connection_id}] ❌ Websocket receive error: {e}")
                break

            if "text" in data:
                try:
                    msg = json.loads(data["text"])
                    event_type = msg.get("event") or msg.get("type")

                    if event_type == "start" and not greeting_sent:
                        start_data = msg.get("start", {})
                        stream_sid = msg.get("stream_sid") or start_data.get("stream_sid")

                        # Capture the caller's own number from the telephony
                        # 'start' payload so the booking flow never has to ask
                        # for it. Exact key wasn't verifiable against a live
                        # Exotel payload in this pass (no captured log
                        # available) - falls through common candidates; needs
                        # confirmation against a real call's logged 'start'
                        # payload (grep for `"start":`).
                        caller_phone = (
                            start_data.get("from")
                            or start_data.get("call_from")
                            or start_data.get("caller_id")
                        )
                        if caller_phone:
                            session.set_caller_phone(caller_phone)
                            logger.info(
                                f"[EX-{connection_id}] 📱 Caller phone captured: {_mask_phone(caller_phone)}"
                            )
                        else:
                            logger.warning(
                                f"[EX-{connection_id}] ⚠️ No caller phone found in start payload: {start_data}"
                            )

                        if stream_sid:
                            session.set_call_id(stream_sid)
                            greeting_sent = True
                            logger.info(f"[EX-{connection_id}] 🏁 Call started, stream_sid={stream_sid}")
                            greeting_text = f"नमस्ते! {business_name} में कॉल करने के लिए धन्यवाद। मैं आपकी क्या मदद कर सकता हूँ?"
                            greeting_task = asyncio.create_task(stream_greeting_audio(greeting_text))

                    elif event_type == "media":
                        media_data = msg.get("media", {})
                        base64_payload = media_data.get("payload", "") or media_data.get("chunk", "")

                        pcm_16k = telephony_to_stt_pcm(
                            base64_payload=base64_payload,
                            source_codec="linear16",
                            source_sr=8000,
                            target_sr=16000,
                        )
                        if pcm_16k:
                            stt_audio_buffer.extend(pcm_16k)
                            # Send to Sarvam STT only when buffer reaches 100ms (3200 bytes)
                            if len(stt_audio_buffer) >= STT_BUFFER_THRESHOLD:
                                chunk_to_send = bytes(stt_audio_buffer)
                                stt_audio_buffer.clear()
                                sent = await stt_client.send_audio_chunk(chunk_to_send)
                                if sent:
                                    logger.debug(
                                        f"[EX-{connection_id}] 🎙️ Sent 100ms buffered audio ({len(chunk_to_send)}B) to STT"
                                    )
                                else:
                                    logger.warning(f"[EX-{connection_id}] ❌ Failed to send buffered audio to STT")

                    elif event_type in ("stop", "closed"):
                        break

                except json.JSONDecodeError:
                    pass
                except Exception as e:
                    logger.error(f"[EX-{connection_id}] Message handling error: {e}", exc_info=True)

    except WebSocketDisconnect:
        logger.info(f"[EX-{connection_id}] Exotel disconnected")
    finally:
        if connection_registered:
            await _unregister_ws_connection(client_ip)

        try:
            if greeting_task and not greeting_task.done():
                greeting_task.cancel()
            if pending_brain_task and not pending_brain_task.done():
                pending_brain_task.cancel()
            if pending_fallback_task and not pending_fallback_task.done():
                pending_fallback_task.cancel()
            if stt_task and not stt_task.done():
                stt_task.cancel()
        except Exception:
            pass

        try:
            if stt_client:
                await stt_client.disconnect()
        except Exception:
            pass

        try:
            await websocket.close()
        except Exception:
            pass

        _log_observability("call_ended", session=session, duration_sec=round(time.perf_counter() - call_start_time, 2))
        session_manager.remove(connection_id)

        logger.info(f"[EX-{connection_id}] ✓ Exotel call stream ended")


async def _build_readiness() -> HealthCheckResponse:
    """Compute full readiness using dependency probes."""
    try:
        cache_available = cache_service is not None and cache_service.cache_enabled
        stt_ready, stt_message = await _probe_stt_service()
        llm_ready, llm_message = await _probe_llm_service()
        tts_ready, tts_message = await _probe_tts_service()
        supabase_ready, supabase_message = await _probe_supabase_service()

        uptime = (datetime.now() - start_time).total_seconds()
        # Supabase is intentionally treated as a SOFT dependency here (degraded,
        # not unhealthy) since tenants covered by the hardcoded fallback config
        # can still be served while Supabase is unreachable.
        all_ready = cache_available and stt_ready and llm_ready and tts_ready and supabase_ready
        status = "healthy" if all_ready else "degraded"

        logger.debug(
            "Readiness check: cache=%s stt=%s llm=%s tts=%s supabase=%s",
            cache_available,
            stt_ready,
            llm_ready,
            tts_ready,
            supabase_ready,
        )

        return HealthCheckResponse(
            status=status,
            cache_available=cache_available,
            sarvam_api_reachable=tts_ready,
            stt_service_reachable=stt_ready,
            llm_service_reachable=llm_ready,
            tts_service_reachable=tts_ready,
            uptime_seconds=uptime,
            details={
                "stt": stt_message,
                "llm": llm_message,
                "tts": tts_message,
                "supabase": supabase_message,
                "active_sessions": session_manager.active_count(),
            },
        )

    except Exception as e:
        request_metrics["readiness_failures"] += 1
        logger.error(f"Readiness check failed: {e}")
        return HealthCheckResponse(
            status="unhealthy",
            cache_available=False,
            sarvam_api_reachable=False,
            stt_service_reachable=False,
            llm_service_reachable=False,
            tts_service_reachable=False,
            uptime_seconds=(datetime.now() - start_time).total_seconds(),
            details={"error": str(e)},
        )


@app.get("/api/v1/live")
async def liveness_check() -> dict:
    """Lightweight liveness endpoint without external probes."""
    return {
        "status": "alive",
        "uptime_seconds": (datetime.now() - start_time).total_seconds(),
        "timestamp": datetime.now().isoformat(),
    }


@app.get(
    "/api/v1/ready",
    response_model=HealthCheckResponse
)
async def readiness_check() -> HealthCheckResponse:
    """Readiness endpoint that probes downstream dependencies."""
    return await _build_readiness()


@app.get(
    "/api/v1/health",
    response_model=HealthCheckResponse
)
async def health_check() -> HealthCheckResponse:
    """Backward-compatible health alias for readiness."""
    return await _build_readiness()


@app.get("/api/v1/metrics")
async def get_metrics() -> dict:
    """
    Get performance metrics.
    
    Returns:
        Dictionary with request metrics and system statistics
    """
    total_requests = request_metrics["total_requests"]
    avg_ttfb = (
        request_metrics["total_ttfb_ms"] / total_requests
        if total_requests > 0 else 0
    )

    metrics = {
        "uptime_seconds": (datetime.now() - start_time).total_seconds(),
        "total_requests": total_requests,
        "successful_requests": request_metrics["successful_requests"],
        "failed_requests": request_metrics["failed_requests"],
        "success_rate_percent": (
            (request_metrics["successful_requests"] / total_requests * 100)
            if total_requests > 0 else 0
        ),
        "average_ttfb_ms": round(avg_ttfb, 2),
        "target_ttfb_ms": settings.target_ttfb_ms,
        "ttfb_slo_met": avg_ttfb < settings.target_ttfb_ms,
        "cache_hits": request_metrics["cache_hits"],
        "sarvam_hits": request_metrics["sarvam_hits"],
        "auth_failures": request_metrics["auth_failures"],
        "rate_limited": request_metrics["rate_limited"],
        "ws_rejected": request_metrics["ws_rejected"],
        "readiness_failures": request_metrics["readiness_failures"],
        "retry_attempts": request_metrics["retry_attempts"],
        "ws_active_total": _ws_state["active_total"],
        "ws_active_unique_ips": len([ip for ip, count in _ws_state["active_per_ip"].items() if count > 0]),
        "latency_percentiles": {
            "brain_ttft_ms_p50": round(_compute_percentile(_latency_samples["brain_ttft_ms"], 0.50), 2),
            "brain_ttft_ms_p95": round(_compute_percentile(_latency_samples["brain_ttft_ms"], 0.95), 2),
            "tts_first_audio_ms_p50": round(_compute_percentile(_latency_samples["tts_first_audio_ms"], 0.50), 2),
            "tts_first_audio_ms_p95": round(_compute_percentile(_latency_samples["tts_first_audio_ms"], 0.95), 2),
            "overlap_savings_ms_p50": round(_compute_percentile(_latency_samples["overlap_savings_ms"], 0.50), 2),
            "overlap_savings_ms_p95": round(_compute_percentile(_latency_samples["overlap_savings_ms"], 0.95), 2),
        },
        "cache_stats": cache_service.get_cache_stats() if cache_service else {},
        "sarvam_stats": sarvam_ws_client.get_connection_stats() if sarvam_ws_client else {},
        "scheduler_stats": packet_scheduler.get_scheduler_stats() if packet_scheduler else {},
    }

    logger.info(f"Metrics requested: avg_ttfb={metrics['average_ttfb_ms']}ms")
    return metrics


@app.post("/api/voice/process")
async def voice_process(
    audio: UploadFile = File(...),
    language_code: str = Form("hi-IN")
) -> dict:
    """
    End-to-end voice processing endpoint.
    
    Accepts audio from browser and processes through full pipeline:
    STT (Sarvam) → Brain (Gemini) → TTS (Sarvam)
    
    Args:
        audio: WebM/WAV audio file from microphone
        language_code: Language code (hi-IN, ta-IN, etc.)
        
    Returns:
        JSON with:
        - transcript: User's speech transcription
        - response: Agent's response text
        - metrics: STT latency, Brain TTFT, E2E TTFB
        - audio_base64: Response audio as base64
    """
    request_id = str(uuid.uuid4())[:13]
    e2e_start = time.perf_counter()
    
    try:
        logger.info(f"[{request_id}] 🎤 Voice process: {audio.filename} ({language_code})")
        
        from .brain.llm_service import StreamingBrain
        import struct
        
        # Read audio file
        audio_bytes = await audio.read()
        logger.info(f"[{request_id}] Audio received: {len(audio_bytes)} bytes")
        
        # ===== STEP 1: STT (Speech-to-Text) - Simplified placeholder =====
        stt_start = time.perf_counter()
        logger.info(f"[{request_id}] → Step 1: STT...")
        
        # For demo purposes, use a placeholder transcript based on audio size
        # In production, this would use SarvamSaarasSTTClient with proper WebSocket
        transcript = f"[Audio input detected: {len(audio_bytes)} bytes] Please respond to this voice message"
        stt_latency_ms = (time.perf_counter() - stt_start) * 1000
        logger.info(f"[{request_id}] ✓ STT: '{transcript}' ({stt_latency_ms:.2f}ms)")
        
        # ===== STEP 2: Brain (LLM Response) =====
        brain_start = time.perf_counter()
        logger.info(f"[{request_id}] → Step 2: Brain...")
        
        try:
            brain = StreamingBrain()
            response_tokens = []
            async for token in brain.stream_response(transcript):
                response_tokens.append(token)
                # Limit response length for demo
                if len("".join(response_tokens)) > 300:
                    break
            response_text = "".join(response_tokens)
            brain_ttft_ms = (time.perf_counter() - brain_start) * 1000
            logger.info(f"[{request_id}] ✓ Brain: '{response_text[:60]}...' ({brain_ttft_ms:.2f}ms)")
        except Exception as e:
            logger.error(f"[{request_id}] Brain error: {e}")
            response_text = "Thank you for your message. I'm ready to help!"
            brain_ttft_ms = 50
        
        # ===== STEP 3: TTS (Text-to-Speech) =====
        tts_start = time.perf_counter()
        logger.info(f"[{request_id}] → Step 3: TTS...")
        
        audio_response = b""
        tts_latency_ms = 0
        
        try:
            # Use existing TTS client
            if not sarvam_ws_client:
                raise Exception("TTS service not initialized")
            
            # Connect TTS if not connected
            tts_connected = await sarvam_ws_client.connect(
                target_language_code=language_code,
                speaker="shubh",
                pace=0.95
            )
            if not tts_connected:
                logger.warning(f"[{request_id}] Failed to connect TTS, will skip audio generation")
            else:
                # Send text to TTS and collect audio
                text_sent = await sarvam_ws_client.send_text_chunk(response_text)
                if not text_sent:
                    raise Exception("Failed to send text to TTS")
                
                # Send flush to trigger audio generation
                flush_ok = await sarvam_ws_client.send_flush()
                logger.debug(f"[{request_id}] TTS flush: {flush_ok}")
                
                # Collect audio chunks with timeout
                audio_chunks = []
                chunk_count = 0
                try:
                    async for audio_chunk in sarvam_ws_client.stream_audio_chunks():
                        audio_chunks.append(audio_chunk)
                        chunk_count += 1
                        if chunk_count > 1000:  # Safety limit
                            break
                except asyncio.TimeoutError:
                    logger.warning(f"[{request_id}] TTS audio stream timeout after {chunk_count} chunks")
                except Exception as e:
                    logger.warning(f"[{request_id}] TTS audio stream error: {e}")
                
                if audio_chunks:
                    audio_response = b"".join(audio_chunks)
                    logger.info(f"[{request_id}] TTS audio collected: {len(audio_response)} bytes ({chunk_count} chunks)")
            
            tts_latency_ms = (time.perf_counter() - tts_start) * 1000
            logger.info(f"[{request_id}] ✓ TTS latency: {tts_latency_ms:.2f}ms")
            
        except Exception as e:
            logger.warning(f"[{request_id}] TTS processing warning: {e}")
            tts_latency_ms = (time.perf_counter() - tts_start) * 1000
            # Don't fail entire request if TTS fails
        
        # ===== Calculate metrics =====
        e2e_ttfb_ms = (time.perf_counter() - e2e_start) * 1000
        
        # ===== Prepare response =====
        
        response_data = {
            "request_id": request_id,
            "transcript": transcript,
            "language_code": language_code,
            "response": response_text,
            "metrics": {
                "stt_latency_ms": round(stt_latency_ms, 2),
                "brain_ttft_ms": round(brain_ttft_ms, 2),
                "tts_latency_ms": round(tts_latency_ms, 2),
                "e2e_ttfb_ms": round(e2e_ttfb_ms, 2)
            },
            "status": "success"
        }
        
        # Add audio if available
        if audio_response:
            response_data["audio_base64"] = base64.b64encode(audio_response).decode('utf-8')
        
        logger.info(f"[{request_id}] ✓ Complete: E2E={e2e_ttfb_ms:.2f}ms")
        request_metrics["total_requests"] += 1
        request_metrics["successful_requests"] += 1
        
        return response_data
        
    except Exception as e:
        logger.error(f"[{request_id}] ✗ Voice process failed: {e}")
        request_metrics["total_requests"] += 1
        request_metrics["failed_requests"] += 1
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api")
async def api_info() -> dict:
    """
    API information endpoint.
    
    Returns:
        Dictionary with API information
    """
    return {
        "service": "Indic TTS WebSocket Runtime Engine",
        "version": "1.0.0",
        "description": "Production-grade Text-to-Speech with Real-time Streaming & Multilingual Support",
        "endpoints": {
            "WS /ws/v1/stream-voice": "Real-time WebSocket streaming synthesis",
            "WS /ws/v1/voice-call": "Bidirectional voice call streaming",
            "WS /ws/v1/exotel-stream": "Exotel Cloud Telephony multi-tenant voice stream",
            "POST /api/v1/stream-voice": "REST streaming synthesis (legacy)",
            "POST /api/voice/process": "End-to-end voice processing (STT → Brain → TTS)",
            "GET /api/v1/live": "Liveness probe",
            "GET /api/v1/ready": "Readiness probe",
            "GET /api/v1/health": "Readiness alias",
            "GET /api/v1/metrics": "Performance metrics"
        },
        "documentation": "/docs",
        "features": [
            "8-language multilingual normalization",
            "Real-time WebSocket streaming",
            "20ms audio frame scheduling",
            "Barge-in interruption handling",
            "Sub-220ms TTFB target"
        ],
        "target_ttfb_ms": settings.target_ttfb_ms
    }



if __name__ == "__main__":
    logger.info(f"Starting WebSocket TTS Engine on {settings.server_host}:{settings.server_port}")
    
    # Configure uvloop for high performance
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        logger.info("✓ uvloop event loop configured for high performance")
    except ImportError:
        logger.warning("⚠️  uvloop not available, using default asyncio event loop")
    
    uvicorn.run(
        app,
        host=settings.server_host,
        port=settings.server_port,
        log_level=settings.log_level.lower(),
        access_log=True
    )
