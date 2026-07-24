"""
Deterministic date/time parser for caller booking requests.

Parses relative expressions ("tomorrow at 6 PM", "next Monday at 5:30 PM",
including common Hindi/Hinglish phrasing like "kal", "parso", "shaam ko"),
anchored to `datetime.now(ZoneInfo(tenant_tz))`.

IMPORTANT: this is intentionally NOT a general-purpose NLP date parser. It only
recognizes a bounded set of deterministic patterns. Anything genuinely
ambiguous (no recognizable day reference, or no recognizable time reference)
returns None so the caller can ask the user a clarifying question instead of
silently guessing. Do NOT let the LLM free-form parse dates - always route
through this function first.

NOTE on end_time_mins: this function has no knowledge of the specific
doctor/service's `slot_duration_mins`, so it returns a DEFAULT_SLOT_MINUTES
placeholder end time. Callers (e.g. the booking flow in main.py) MUST
recompute `end_time_mins = start_time_mins + item.slot_duration_mins` using
the actual service duration before calling `check_slot_available` /
`create_appointment_async`. The value returned here is only meaningful if the
caller doesn't override it.
"""

import re
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

DEFAULT_SLOT_MINUTES = 30

_WEEKDAYS = {
    "monday": 0, "somvar": 0, "mon": 0,
    "tuesday": 1, "mangalvar": 1, "tue": 1, "tues": 1,
    "wednesday": 2, "budhvar": 2, "wed": 2,
    "thursday": 3, "guruvar": 3, "thu": 3, "thurs": 3,
    "friday": 4, "shukravar": 4, "fri": 4,
    "saturday": 5, "shanivar": 5, "sat": 5,
    "sunday": 6, "raviwar": 6, "ravivar": 6, "sun": 6,
}

# Relative-day keywords, ordered so longer/more specific phrases match first.
_RELATIVE_DAY_PATTERNS = [
    (r"\bday after tomorrow\b", 2),
    (r"\bparso\b", 2),  # Hindi: could mean day-before/day-after; booking context => future
    (r"\btomorrow\b", 1),
    (r"\bkal\b", 1),  # Hindi "kal" - future booking context => tomorrow
    (r"\btoday\b", 0),
    (r"\baaj\b", 0),
]

# Time-of-day qualifiers used to disambiguate 12-hour phrasing without AM/PM.
_MORNING_WORDS = r"(?:subah|morning)"
_AFTERNOON_WORDS = r"(?:dopahar|afternoon)"
_EVENING_WORDS = r"(?:shaam|sham|evening)"
_NIGHT_WORDS = r"(?:raat|night)"

_TIME_AMPM_RE = re.compile(
    r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm|a\.m\.|p\.m\.)\b", re.IGNORECASE
)
_TIME_24H_RE = re.compile(r"\b([01]?\d|2[0-3]):([0-5]\d)\b")
_QUALIFIER_ALT = rf"(?:{_MORNING_WORDS}|{_AFTERNOON_WORDS}|{_EVENING_WORDS}|{_NIGHT_WORDS})"
# Qualifier may appear either before ("shaam ko 6 baje") or after ("6 baje shaam")
# the number - both orderings are common in Hinglish speech.
_TIME_BAJE_RE = re.compile(
    rf"\b(?:({_QUALIFIER_ALT})\s*(?:ko\s*)?)?"
    r"(\d{1,2})(?::(\d{2}))?\s*baje\b"
    rf"(?:\s*({_QUALIFIER_ALT}))?",
    re.IGNORECASE,
)
_TIME_QUALIFIED_RE = re.compile(
    rf"\b({_MORNING_WORDS}|{_AFTERNOON_WORDS}|{_EVENING_WORDS}|{_NIGHT_WORDS})\s*"
    r"(?:ko\s*)?(\d{1,2})(?::(\d{2}))?\b",
    re.IGNORECASE,
)

_NEXT_WEEKDAY_RE = re.compile(
    r"\b(?:next|agle|agla|agli)\s+(" + "|".join(_WEEKDAYS.keys()) + r")\b",
    re.IGNORECASE,
)
_THIS_WEEKDAY_RE = re.compile(
    r"\b(?:this|is)\s+(" + "|".join(_WEEKDAYS.keys()) + r")\b",
    re.IGNORECASE,
)
_BARE_WEEKDAY_RE = re.compile(
    r"\b(" + "|".join(_WEEKDAYS.keys()) + r")\b", re.IGNORECASE
)


def _resolve_qualified_hour(hour: int, qualifier: Optional[str]) -> Optional[int]:
    """Convert a 1-12 (or already-24h) hour + optional Hindi/English qualifier
    into a 24-hour hour value. Returns None if genuinely ambiguous."""
    if hour > 23:
        return None
    if hour >= 13:
        # Already unambiguous 24h-style hour (e.g. "18 baje").
        return hour if hour <= 23 else None

    if qualifier:
        q = qualifier.lower()
        if re.fullmatch(_MORNING_WORDS, q):
            return 0 if hour == 12 else hour
        if re.fullmatch(_AFTERNOON_WORDS, q) or re.fullmatch(_EVENING_WORDS, q) or re.fullmatch(_NIGHT_WORDS, q):
            return hour if hour == 12 else hour + 12

    # No qualifier and no AM/PM marker: genuinely ambiguous for hours 1-11.
    # Hour 0 or 12 without qualifier is also ambiguous (midnight vs noon).
    return None


def _extract_time_mins(text: str) -> Optional[int]:
    """Extract start-of-day minutes (0-1439) from text, or None if ambiguous/missing.

    Tries each pattern in confidence order and falls through to the next
    pattern if a match is found but its hour can't be resolved unambiguously
    (rather than giving up on the very first partial match)."""
    # 1. Explicit AM/PM (highest confidence, unambiguous).
    m = _TIME_AMPM_RE.search(text)
    if m:
        hour = int(m.group(1))
        minute = int(m.group(2) or 0)
        meridiem = m.group(3).lower().replace(".", "")
        if 1 <= hour <= 12 and minute <= 59:
            if meridiem == "am":
                hour = 0 if hour == 12 else hour
            else:
                hour = 12 if hour == 12 else hour + 12
            return hour * 60 + minute

    # 2. Explicit 24-hour clock (e.g. "18:30").
    m = _TIME_24H_RE.search(text)
    if m:
        hour = int(m.group(1))
        minute = int(m.group(2))
        return hour * 60 + minute

    # 3. "<N> baje" with an optional Hindi qualifier before or after.
    m = _TIME_BAJE_RE.search(text)
    if m:
        qualifier = m.group(1) or m.group(4)
        hour = int(m.group(2))
        minute = int(m.group(3) or 0)
        resolved_hour = _resolve_qualified_hour(hour, qualifier)
        if resolved_hour is not None and minute <= 59:
            return resolved_hour * 60 + minute

    # 4. "<qualifier> [ko] <N>(:MM)" e.g. "shaam ko 6", "evening 6:30".
    m = _TIME_QUALIFIED_RE.search(text)
    if m:
        qualifier = m.group(1)
        hour = int(m.group(2))
        minute = int(m.group(3) or 0)
        resolved_hour = _resolve_qualified_hour(hour, qualifier)
        if resolved_hour is None or minute > 59:
            return None
        return resolved_hour * 60 + minute

    return None


def _extract_target_date(text: str, now: datetime) -> Optional[datetime]:
    """Resolve the target calendar date (still at midnight) or None if ambiguous/missing."""
    for pattern, day_offset in _RELATIVE_DAY_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return (now + timedelta(days=day_offset)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )

    m = _NEXT_WEEKDAY_RE.search(text)
    if m:
        target_wd = _WEEKDAYS[m.group(1).lower()]
        days_ahead = (target_wd - now.weekday() + 7) % 7
        days_ahead = days_ahead if days_ahead != 0 else 7  # "next X" always in the future week
        return (now + timedelta(days=days_ahead)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

    m = _THIS_WEEKDAY_RE.search(text)
    if m:
        target_wd = _WEEKDAYS[m.group(1).lower()]
        days_ahead = (target_wd - now.weekday() + 7) % 7
        return (now + timedelta(days=days_ahead)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

    m = _BARE_WEEKDAY_RE.search(text)
    if m:
        target_wd = _WEEKDAYS[m.group(1).lower()]
        days_ahead = (target_wd - now.weekday() + 7) % 7
        return (now + timedelta(days=days_ahead)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

    return None


def parse_user_datetime(
    user_text: str, tenant_tz: str = "Asia/Kolkata"
) -> Optional[tuple[str, int, int, str]]:
    """
    Parse a caller's free-text date/time reference deterministically.

    Returns (date_str "YYYY-MM-DD", start_mins, end_mins, display_time "06:00 PM")
    or None if the phrasing is genuinely ambiguous (no recognizable day
    reference, or no recognizable/unambiguous time reference). Callers should
    treat None as "ask the user a clarifying question", never guess further.

    `end_mins` is a DEFAULT_SLOT_MINUTES placeholder - see module docstring.
    """
    if not user_text or not user_text.strip():
        return None

    text = user_text.strip().lower()
    now = datetime.now(ZoneInfo(tenant_tz))

    target_date = _extract_target_date(text, now)
    if target_date is None:
        return None

    start_mins = _extract_time_mins(text)
    if start_mins is None:
        return None

    end_mins = min(start_mins + DEFAULT_SLOT_MINUTES, 24 * 60 - 1)

    date_str = target_date.strftime("%Y-%m-%d")
    display_dt = target_date.replace(hour=start_mins // 60, minute=start_mins % 60)
    display_time_str = display_dt.strftime("%I:%M %p")

    return date_str, start_mins, end_mins, display_time_str
