"""
Deterministic date/time parser for caller booking requests.

Parses relative expressions ("tomorrow at 6 PM", "next Monday at 5:30 PM",
including Hindi/Hinglish phrasing in BOTH romanized ("kal", "shaam ko")
AND Devanagari ("कल", "शाम को") script, since STT output is frequently
Devanagari-only), anchored to `datetime.now(ZoneInfo(tenant_tz))`.

Also parses absolute calendar dates ("25th July 2026", "25 जुलाई 2026",
"July 25", "25/07/2026") - the original version of this module only
supported relative-day and weekday references, so any caller who gave an
explicit date (the natural fallback when a bot keeps failing to understand
"tomorrow") was rejected every time.

IMPORTANT: this is intentionally NOT a general-purpose NLP date parser. It
only recognizes a bounded set of deterministic patterns. Anything genuinely
ambiguous (no recognizable day reference, or no recognizable time reference)
returns None so the caller can be asked a clarifying question instead of
silently guessing. Do NOT let the LLM free-form parse dates - always route
through this function first.

NOTE on end_time_mins: this function has no knowledge of the specific
doctor/service's `slot_duration_mins`, so it returns a DEFAULT_SLOT_MINUTES
placeholder end time. Callers (e.g. the booking flow in main.py) MUST
recompute `end_time_mins = start_time_mins + item.slot_duration_mins` using
the actual service duration before calling `check_slot_available` /
`create_appointment_async`. The value returned here is only meaningful if
the caller doesn't override it.
"""

import re
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

DEFAULT_SLOT_MINUTES = 30

# ---------------------------------------------------------------------------
# Devanagari digit normalization (STT sometimes emits ०-९ instead of 0-9)
# ---------------------------------------------------------------------------
_DEVANAGARI_DIGIT_MAP = str.maketrans("०१२३४५६७८९", "0123456789")


def _normalize_digits(text: str) -> str:
    return text.translate(_DEVANAGARI_DIGIT_MAP)


# ---------------------------------------------------------------------------
# Spoken number words (1-31), Latin transliteration + Devanagari.
# Used ONLY inside context-anchored regex slots (immediately before "baje"/
# "o'clock", or immediately beside a month name) - NEVER as a blanket
# find-and-replace over the whole utterance. This matters because several
# Latin transliterations (e.g. "do" = two, "teen" = three) collide with
# common English words ("do the booking", "teenager"); anchoring the match
# to its syntactic context (right before baje/o'clock/a month name) avoids
# those false positives.
# ---------------------------------------------------------------------------
_NUMBER_WORDS: dict[str, int] = {
    "ek": 1, "एक": 1,
    "do": 2, "दो": 2,
    "teen": 3, "तीन": 3,
    "char": 4, "chaar": 4, "चार": 4,
    "paanch": 5, "panch": 5, "पांच": 5, "पाँच": 5,
    "chhe": 6, "che": 6, "छह": 6, "छे": 6,
    "saat": 7, "सात": 7,
    "aath": 8, "आठ": 8,
    "nau": 9, "नौ": 9,
    "das": 10, "दस": 10,
    "gyarah": 11, "ग्यारह": 11,
    "barah": 12, "बारह": 12,
    "terah": 13, "तेरह": 13,
    "chaudah": 14, "चौदह": 14,
    "pandrah": 15, "पंद्रह": 15,
    "solah": 16, "सोलह": 16,
    "satrah": 17, "सत्रह": 17,
    "atharah": 18, "अठारह": 18,
    "unnis": 19, "उन्नीस": 19,
    "bees": 20, "bis": 20, "बीस": 20,
    "ikkis": 21, "इक्कीस": 21,
    "baees": 22, "bais": 22, "बाईस": 22,
    "teees": 23, "teis": 23, "तेईस": 23,
    "chaubees": 24, "चौबीस": 24,
    "pachchees": 25, "pachis": 25, "पच्चीस": 25,
    "chhabbees": 26, "छब्बीस": 26,
    "sattaees": 27, "सत्ताईस": 27,
    "atthaees": 28, "अट्ठाईस": 28,
    "unatees": 29, "उनतीस": 29,
    "tees": 30, "तीस": 30,
    "ikatees": 31, "इकतीस": 31,
}
_NUM_WORD_ALT = "|".join(re.escape(w) for w in sorted(_NUMBER_WORDS, key=len, reverse=True))
_NUM_TOKEN = rf"(?:\d{{1,2}}|{_NUM_WORD_ALT})"


def _to_int(token: Optional[str]) -> Optional[int]:
    if token is None:
        return None
    token = token.strip().lower()
    if token.isdigit():
        return int(token)
    return _NUMBER_WORDS.get(token)


# ---------------------------------------------------------------------------
# Weekdays - Latin transliteration + Devanagari
# ---------------------------------------------------------------------------
_WEEKDAYS = {
    "monday": 0, "somvar": 0, "mon": 0, "सोमवार": 0,
    "tuesday": 1, "mangalvar": 1, "tue": 1, "tues": 1, "मंगलवार": 1,
    "wednesday": 2, "budhvar": 2, "wed": 2, "बुधवार": 2,
    "thursday": 3, "guruvar": 3, "thu": 3, "thurs": 3, "गुरुवार": 3, "बृहस्पतिवार": 3,
    "friday": 4, "shukravar": 4, "fri": 4, "शुक्रवार": 4,
    "saturday": 5, "shanivar": 5, "sat": 5, "शनिवार": 5,
    "sunday": 6, "raviwar": 6, "ravivar": 6, "sun": 6, "रविवार": 6, "इतवार": 6,
}

# ---------------------------------------------------------------------------
# Relative-day keywords, ordered so longer/more specific phrases match first.
# ---------------------------------------------------------------------------
_RELATIVE_DAY_PATTERNS = [
    (r"\bday after tomorrow\b", 2),
    (r"\bparso\b", 2), (r"\bपरसों\b", 2),  # ambiguous day-before/after; booking context => future
    (r"\btomorrow\b", 1),
    (r"\bkal\b", 1), (r"\bकल\b", 1),
    (r"\btoday\b", 0),
    (r"\baaj\b", 0), (r"\bआज\b", 0),
]

# ---------------------------------------------------------------------------
# Time-of-day qualifiers (Latin + Devanagari)
# ---------------------------------------------------------------------------
_MORNING_WORDS = r"(?:subah|morning|सुबह)"
_AFTERNOON_WORDS = r"(?:dopahar|afternoon|दोपहर)"
_EVENING_WORDS = r"(?:shaam|sham|evening|शाम)"
_NIGHT_WORDS = r"(?:raat|night|रात)"
_QUALIFIER_ALT = rf"(?:{_MORNING_WORDS}|{_AFTERNOON_WORDS}|{_EVENING_WORDS}|{_NIGHT_WORDS})"

_TIME_AMPM_RE = re.compile(
    r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm|a\.m\.|p\.m\.)\b", re.IGNORECASE
)
_TIME_24H_RE = re.compile(r"\b([01]?\d|2[0-3]):([0-5]\d)\b")

# "<qualifier>? [ko/को]? <N>(:MM)? baje/बजे <qualifier>?"
_TIME_BAJE_RE = re.compile(
    rf"\b(?:({_QUALIFIER_ALT})\s*(?:ko|को)?\s*)?"
    rf"({_NUM_TOKEN})(?::(\d{{2}}))?\s*(?:baje|बजे)"
    rf"(?:\s*({_QUALIFIER_ALT}))?",
    re.IGNORECASE,
)

# "<N> o'clock" / "<N> oclock"
_TIME_OCLOCK_RE = re.compile(
    rf"\b({_NUM_TOKEN})\s*(?:o['’]?clock|oclock)\b", re.IGNORECASE
)

# "<qualifier> [ko/को]? <N>(:MM)" e.g. "shaam ko 6", "evening 6:30", "शाम 6"
_TIME_QUALIFIED_RE = re.compile(
    rf"\b({_QUALIFIER_ALT})\s*(?:ko|को)?\s*({_NUM_TOKEN})(?::(\d{{2}}))?\b",
    re.IGNORECASE,
)

# "<N> बजकर <MM> मिनट" e.g. "6 बजकर 30 मिनट" = 6:30
_TIME_BAJKAR_RE = re.compile(
    rf"\b({_NUM_TOKEN})\s*(?:बजकर|bajkar)\s*({_NUM_TOKEN})\s*(?:मिनट|minute|min)?\b",
    re.IGNORECASE,
)

_NEXT_WEEKDAY_RE = re.compile(
    r"\b(?:next|agle|agla|agli|अगले|अगला|अगली)\s+(" + "|".join(_WEEKDAYS.keys()) + r")\b",
    re.IGNORECASE,
)
_THIS_WEEKDAY_RE = re.compile(
    r"\b(?:this|is|इस)\s+(" + "|".join(_WEEKDAYS.keys()) + r")\b",
    re.IGNORECASE,
)
_BARE_WEEKDAY_RE = re.compile(
    r"\b(" + "|".join(_WEEKDAYS.keys()) + r")\b", re.IGNORECASE
)

# ---------------------------------------------------------------------------
# Absolute calendar dates: "25th July 2026", "25 जुलाई 2026", "July 25",
# "25/07/2026". Latin + Devanagari month names, with/without abbreviations.
# ---------------------------------------------------------------------------
_MONTHS: dict[str, int] = {
    "january": 1, "jan": 1, "जनवरी": 1,
    "february": 2, "feb": 2, "फरवरी": 2,
    "march": 3, "mar": 3, "मार्च": 3,
    "april": 4, "apr": 4, "अप्रैल": 4,
    "may": 5, "मई": 5,
    "june": 6, "jun": 6, "जून": 6,
    "july": 7, "jul": 7, "जुलाई": 7,
    "august": 8, "aug": 8, "अगस्त": 8,
    "september": 9, "sep": 9, "sept": 9, "सितंबर": 9, "सितम्बर": 9,
    "october": 10, "oct": 10, "अक्टूबर": 10,
    "november": 11, "nov": 11, "नवंबर": 11, "नवम्बर": 11,
    "december": 12, "dec": 12, "दिसंबर": 12, "दिसम्बर": 12,
}
_MONTH_ALT = "|".join(re.escape(m) for m in sorted(_MONTHS, key=len, reverse=True))

# "25th July 2026" / "25 July, 2026" / "25 जुलाई 2026" / "25 July"
_DATE_DAY_MONTH_YEAR_RE = re.compile(
    rf"\b({_NUM_TOKEN})(?:st|nd|rd|th)?\s+({_MONTH_ALT})\.?,?\s*(\d{{4}})?\b",
    re.IGNORECASE,
)
# "July 25th 2026" / "July 25, 2026" / "जुलाई 25"
_DATE_MONTH_DAY_YEAR_RE = re.compile(
    rf"\b({_MONTH_ALT})\.?\s+({_NUM_TOKEN})(?:st|nd|rd|th)?,?\s*(\d{{4}})?\b",
    re.IGNORECASE,
)
# "25/07/2026" or "25-07-2026" (assumes DD/MM/YYYY, the Indian convention;
# if the first number is >12 it's unambiguous DD/MM regardless of convention)
_DATE_NUMERIC_RE = re.compile(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})\b")


def _resolve_qualified_hour(hour: int, qualifier: Optional[str]) -> Optional[int]:
    """Convert a 1-12 (or already-24h) hour + optional Hindi/English qualifier
    into a 24-hour hour value. Returns None if genuinely ambiguous."""
    if hour > 23:
        return None
    if hour >= 13:
        return hour if hour <= 23 else None

    if qualifier:
        q = qualifier.lower()
        if re.fullmatch(_MORNING_WORDS, q, re.IGNORECASE):
            return 0 if hour == 12 else hour
        if (
            re.fullmatch(_AFTERNOON_WORDS, q, re.IGNORECASE)
            or re.fullmatch(_EVENING_WORDS, q, re.IGNORECASE)
            or re.fullmatch(_NIGHT_WORDS, q, re.IGNORECASE)
        ):
            return hour if hour == 12 else hour + 12

    return None


def _find_any_qualifier(text: str) -> Optional[str]:
    """Fallback: scan the WHOLE utterance for a time-of-day qualifier word,
    used when a matched time pattern (e.g. bare 'o'clock') didn't have one
    immediately attached. Lets phrasing like 'shaam ko... 6 o'clock' still
    resolve even when the qualifier and the number aren't adjacent."""
    m = re.search(_QUALIFIER_ALT, text, re.IGNORECASE)
    return m.group(0) if m else None


def _extract_time_mins(text: str) -> Optional[int]:
    """Extract start-of-day minutes (0-1439) from text, or None if ambiguous/missing.

    Tries each pattern in confidence order and falls through to the next
    pattern if a match is found but its hour can't be resolved unambiguously."""
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
        return int(m.group(1)) * 60 + int(m.group(2))

    # 3. "<N> बजकर <MM> मिनट" (e.g. "6 बजकर 30 मिनट").
    m = _TIME_BAJKAR_RE.search(text)
    if m:
        hour = _to_int(m.group(1))
        minute = _to_int(m.group(2))
        if hour is not None and minute is not None and minute <= 59:
            qualifier = _find_any_qualifier(text)
            resolved_hour = _resolve_qualified_hour(hour, qualifier)
            if resolved_hour is not None:
                return resolved_hour * 60 + minute

    # 4. "<N> baje / बजे" with an optional qualifier before or after.
    m = _TIME_BAJE_RE.search(text)
    if m:
        qualifier = m.group(1) or m.group(4) or _find_any_qualifier(text)
        hour = _to_int(m.group(2))
        minute = _to_int(m.group(3)) or 0
        if hour is not None:
            resolved_hour = _resolve_qualified_hour(hour, qualifier)
            if resolved_hour is not None and minute <= 59:
                return resolved_hour * 60 + minute

    # 5. "<qualifier> [ko/को] <N>(:MM)" e.g. "shaam ko 6", "शाम 6:30".
    m = _TIME_QUALIFIED_RE.search(text)
    if m:
        qualifier = m.group(1)
        hour = _to_int(m.group(2))
        minute = _to_int(m.group(3)) or 0
        if hour is not None:
            resolved_hour = _resolve_qualified_hour(hour, qualifier)
            if resolved_hour is not None and minute <= 59:
                return resolved_hour * 60 + minute

    # 6. "<N> o'clock" - only unambiguous if a qualifier appears anywhere
    # else in the utterance (bare "6 o'clock" alone is still ambiguous).
    m = _TIME_OCLOCK_RE.search(text)
    if m:
        hour = _to_int(m.group(1))
        if hour is not None:
            qualifier = _find_any_qualifier(text)
            resolved_hour = _resolve_qualified_hour(hour, qualifier)
            if resolved_hour is not None:
                return resolved_hour * 60

    return None


def _extract_absolute_date(text: str, now: datetime) -> Optional[datetime]:
    """Resolve an explicit calendar date ('25 July 2026', '25/07/2026', etc.)
    or None if no absolute-date pattern is present. If the year is omitted
    and the resulting date has already passed this year, roll forward to
    next year (a caller in December saying '5 January' means next January).
    If an EXPLICIT year is given and it's in the past, return None - we
    should never silently book a date the caller didn't actually mean."""

    def _build(day: int, month: int, year_str: Optional[str]) -> Optional[datetime]:
        if not (1 <= day <= 31 and 1 <= month <= 12):
            return None
        try:
            if year_str:
                year = int(year_str)
                if year < 100:
                    year += 2000
                candidate = now.replace(
                    year=year, month=month, day=day,
                    hour=0, minute=0, second=0, microsecond=0,
                )
                if candidate.date() < now.date():
                    return None  # explicit past date - don't silently accept
                return candidate
            # No year given: assume current year, roll to next year if past.
            candidate = now.replace(
                year=now.year, month=month, day=day,
                hour=0, minute=0, second=0, microsecond=0,
            )
            if candidate.date() < now.date():
                candidate = candidate.replace(year=now.year + 1)
            return candidate
        except ValueError:
            return None  # e.g. Feb 30

    m = _DATE_DAY_MONTH_YEAR_RE.search(text)
    if m:
        day = _to_int(m.group(1))
        month = _MONTHS.get(m.group(2).lower())
        if day is not None and month is not None:
            result = _build(day, month, m.group(3))
            if result is not None:
                return result

    m = _DATE_MONTH_DAY_YEAR_RE.search(text)
    if m:
        month = _MONTHS.get(m.group(1).lower())
        day = _to_int(m.group(2))
        if day is not None and month is not None:
            result = _build(day, month, m.group(3))
            if result is not None:
                return result

    m = _DATE_NUMERIC_RE.search(text)
    if m:
        first, second, year_str = int(m.group(1)), int(m.group(2)), m.group(3)
        # DD/MM assumption (India). If first > 12 it's unambiguous DD/MM
        # regardless; if second > 12 it must be MM/DD instead.
        if first > 12:
            day, month = first, second
        elif second > 12:
            day, month = second, first
        else:
            day, month = first, second  # ambiguous - default to DD/MM
        result = _build(day, month, year_str)
        if result is not None:
            return result

    return None


def _extract_target_date(text: str, now: datetime) -> Optional[datetime]:
    """Resolve the target calendar date (still at midnight) or None if
    ambiguous/missing. Absolute dates are checked first (most specific /
    least ambiguous), then relative-day keywords, then weekday references."""

    absolute = _extract_absolute_date(text, now)
    if absolute is not None:
        return absolute

    for pattern, day_offset in _RELATIVE_DAY_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return (now + timedelta(days=day_offset)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )

    m = _NEXT_WEEKDAY_RE.search(text)
    if m:
        target_wd = _WEEKDAYS[m.group(1).lower()]
        days_ahead = (target_wd - now.weekday() + 7) % 7
        days_ahead = days_ahead if days_ahead != 0 else 7
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

    # Lowercasing is a no-op on Devanagari (safe); digit normalization turns
    # any Devanagari numerals (०-९) into ASCII so the \d-based regexes above
    # can see them.
    text = _normalize_digits(user_text.strip().lower())
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
