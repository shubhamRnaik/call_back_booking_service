"""
Emergency fast-path detection and Exotel "Applet Exit Path" transfer helpers.

Fast-path detection: a caller uttering a medical/safety emergency phrase
should bypass the normal LLM turn entirely (no waiting on token generation) -
the caller gets an immediate short acknowledgement and is handed off to a
human/PSTN line as fast as possible. Detection is a plain regex (not an LLM
classifier) so it costs ~0ms and can run before invoking the LLM at all.

Transfer mechanism (per explicit architecture decision - NOT a REST Connect
API call):
  1. Synthesize and stream a short handover phrase in full.
  2. Wait for the callee's audio buffer to actually drain, using an exact
     byte-count calculation (see compute_playback_drain_seconds()) rather
     than a fixed sleep.
  3. Close the WebSocket cleanly with code=1000, tagging the disconnect
     reason as "emergency_transfer" in structured logs.
  4. On Exotel's side (dashboard configuration, NOT code in this repo), the
     Voicebot applet's Exit/Success port is wired to a Connect Applet
     configured with tenant.emergency_number - Exotel performs the actual
     PSTN transfer once this bot's WebSocket closes normally on that exit
     port. This module never calls Exotel's REST Connect API directly.
"""

import asyncio
import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

# Deliberately broad but bounded - a false positive (bot offers to transfer
# when it wasn't truly an emergency) is far less costly than a false
# negative (a genuine emergency proceeds through the normal slow booking
# flow). English + Hindi/Hinglish phrasing likely to appear in STT
# transcripts (STT output, not necessarily correctly spelled Hindi).
_EMERGENCY_KEYWORDS = [
    r"emergency",
    r"heart attack",
    r"chest pain",
    r"can'?t breathe",
    r"cannot breathe",
    r"not breathing",
    r"unconscious",
    r"passed out",
    r"severe bleeding",
    r"bleeding heavily",
    r"stroke",
    r"seizure",
    r"accident",
    r"help me",
    r"dying",
    r"suicide",
    r"overdose",
    # Hindi / Hinglish (transliterated, as an STT engine would output it)
    r"dil ka daura",
    r"saans nahi aa rahi",
    r"saans nahi le pa",
    r"behosh",
    r"khoon beh raha",
    r"accident ho gaya",
    r"bachao",
    r"jaan khatre",
    r"marne wala",
]

EMERGENCY_PATTERN = re.compile(
    r"\b(" + "|".join(_EMERGENCY_KEYWORDS) + r")\b", re.IGNORECASE
)


def check_emergency_fastpath(text: str) -> bool:
    """
    Return True if `text` (a caller transcript) matches a known emergency
    phrase. Must be called BEFORE the normal LLM turn/booking flow, not
    after - that's the whole point of a fast-path.
    """
    if not text:
        return False
    return bool(EMERGENCY_PATTERN.search(text))


def find_emergency_match(text: str) -> Optional[str]:
    """Return the matched keyword/phrase (for structured logging), or None."""
    if not text:
        return None
    m = EMERGENCY_PATTERN.search(text)
    return m.group(0) if m else None


def build_emergency_handover_phrase(tenant: Optional[dict] = None) -> str:
    """
    Short, unconditional phrase spoken to the caller immediately before
    transfer. Does not depend on the LLM (kept static/fast) - only
    personalizes with the tenant's business_name if available.
    """
    business_name = tenant.get("business_name") if tenant else None

    if business_name:
        return (
            f"This sounds like an emergency. I'm connecting you to "
            f"{business_name}'s emergency line right now. Please stay on "
            f"the line."
        )
    return (
        "This sounds like an emergency. I'm connecting you to our "
        "emergency line right now. Please stay on the line."
    )


def compute_playback_drain_seconds(
    total_bytes_sent: int,
    bytes_per_second: float,
    min_wait_sec: float = 0.3,
    safety_margin_sec: float = 0.2,
) -> float:
    """
    Exact byte-count-based playback drain time, replacing fixed-sleep
    heuristics (e.g. the existing END_CALL flow's `asyncio.sleep(1.2)`-style
    wait). `bytes_per_second` MUST match the audio rate the bytes were
    actually sent at - e.g. settings.TTS_BYTES_PER_SECOND (44100) for
    22050Hz/16-bit TTS-native bytes, or
    settings.EXOTEL_SAMPLE_RATE * settings.EXOTEL_BYTES_PER_SAMPLE (16000)
    for telephony-resampled 8kHz bytes. Passing the wrong rate for the stage
    of the pipeline being timed will under/over-wait.

    A small safety_margin_sec is added on top of the exact duration to
    absorb scheduler/network jitter: this wait precedes closing a live
    connection (irreversible), so erring slightly long is cheap while erring
    short truncates the caller's audio mid-sentence.
    """
    if bytes_per_second <= 0:
        return min_wait_sec
    exact_duration_sec = total_bytes_sent / bytes_per_second
    return max(min_wait_sec, exact_duration_sec + safety_margin_sec)


async def close_websocket_for_emergency_transfer(
    websocket,
    connection_id: str,
    total_bytes_sent: int,
    bytes_per_second: float,
) -> None:
    """
    Wait for exact byte-count playback drain, then close the WebSocket with
    code=1000 on the applet's Exit/Success port so Exotel's dashboard-
    configured Connect Applet takes over the transfer to
    tenant.emergency_number. Never calls any Exotel REST API - this IS the
    Applet Exit Path.
    """
    wait_sec = compute_playback_drain_seconds(total_bytes_sent, bytes_per_second)
    logger.info(
        f"[EMERGENCY-{connection_id}] Waiting {wait_sec:.2f}s for handover "
        f"phrase playback to drain before closing "
        f"(exit_reason=emergency_transfer)"
    )
    await asyncio.sleep(wait_sec)

    try:
        await websocket.close(code=1000)
        logger.info(
            f"[EMERGENCY-{connection_id}] WebSocket closed cleanly "
            f"(exit_reason=emergency_transfer) - Exotel Connect Applet "
            f"should now engage."
        )
    except Exception as exc:
        logger.warning(
            f"[EMERGENCY-{connection_id}] Error closing websocket during "
            f"emergency transfer: {exc}"
        )
