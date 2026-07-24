"""
In-memory session state for active voice calls.

Single-instance, single-process (no Redis) - matches the existing app's
architecture, where per-connection state currently lives as local/nonlocal
variables inside the WebSocket handler closures in main.py. `SessionManager`
adds a central registry keyed by connection_id so other parts of the app
(booking flow, observability logging, emergency transfer, cleanup) can look
up and mutate a call's state without threading extra parameters through
every function.

NOTE on chat_history vs StreamingBrain's internal history: `StreamingBrain`
(brain/llm_service.py) already keeps its own rolling conversation deque used
to build the actual LLM API request payload - that is unchanged by this
module. `CallSession.chat_history` here is a SEPARATE, session-level record
used for cross-cutting concerns (structured JSON observability logs, and any
future admin/debug inspection) - it does not feed the LLM directly. Keeping
these separate avoids coupling the LLM client's request-building internals to
session bookkeeping.
"""

import time
from typing import Any, Optional

# Rolling window size for CallSession.chat_history (spec: "last 12").
CHAT_HISTORY_MAX_TURNS = 12


class CallSession:
    """
    Tracks in-memory state for a single active call/connection.

    - current_utterance_id: incremented on every new user utterance; used to
      invalidate in-flight TTS/response tasks on barge-in (a task compares its
      captured utterance id against the session's current one before writing
      audio frames or making side effects).
    - extracted_slots: accumulates booking-flow fields across turns (e.g.
      item_name, date_str, start_mins, end_mins, patient_name, patient_phone)
      as the LLM/slot-filling logic extracts them turn by turn. Not persisted
      anywhere else - lost if the process restarts (acceptable per in-memory
      single-instance decision).
    """

    def __init__(
        self,
        connection_id: str,
        tenant_id: str,
        call_id: Optional[str] = None,
    ) -> None:
        self.connection_id = connection_id
        self.tenant_id = tenant_id
        # call_id (e.g. Exotel stream_sid) often isn't known until after the
        # WebSocket handshake (arrives in a later "start" event) - defaults to
        # connection_id until set_call_id() is called.
        self.call_id = call_id or connection_id
        self.created_at = time.time()

        self.chat_history: list[dict[str, Any]] = []
        self.current_utterance_id: int = 0
        self.extracted_slots: dict[str, Any] = {}
        # Caller's own phone number, captured from the telephony 'start' event
        # payload (see _websocket_exotel_stream_impl in main.py). Used to
        # auto-fill the booking flow's phone field so the caller is never
        # asked for a number they're already calling from.
        self.caller_phone: Optional[str] = None

        # Monotonic counter used to build idempotency_key = f"{call_id}:{n}"
        # for booking attempts (see services/supabase_service.py).
        self._attempt_nonce_counter: int = 0

    def set_call_id(self, call_id: str) -> None:
        """Update call_id once the real telephony call id becomes known
        (e.g. Exotel's stream_sid arrives in the 'start' event, after
        connect)."""
        if call_id:
            self.call_id = call_id

    def set_caller_phone(self, phone: str) -> None:
        """Store the caller's own phone number captured from the telephony
        'start' event, once, so booking flows can reuse it instead of
        asking the caller to repeat their number."""
        if phone:
            self.caller_phone = phone

    def add_turn(self, role: str, text: str) -> None:
        """Append a turn to the rolling chat history (role: 'user' | 'assistant')."""
        self.chat_history.append({"role": role, "text": text, "ts": time.time()})
        if len(self.chat_history) > CHAT_HISTORY_MAX_TURNS:
            self.chat_history = self.chat_history[-CHAT_HISTORY_MAX_TURNS:]

    def next_utterance_id(self) -> int:
        """Start a new utterance - invalidates any previously in-flight task
        that's still checking is_current_utterance() against its captured id."""
        self.current_utterance_id += 1
        return self.current_utterance_id

    def is_current_utterance(self, utterance_id: int) -> bool:
        """True if `utterance_id` is still the active one (i.e. no barge-in
        has superseded it since it was captured)."""
        return utterance_id == self.current_utterance_id

    def next_attempt_nonce(self) -> str:
        """Fresh nonce for a new booking attempt's idempotency_key. A retry of
        the SAME attempt (e.g. reconnect during an insert) should reuse the
        nonce it was given, not call this again."""
        self._attempt_nonce_counter += 1
        return str(self._attempt_nonce_counter)

    def update_slots(self, **fields: Any) -> None:
        """Merge newly-extracted booking fields, ignoring None values (so a
        turn that didn't mention a field doesn't clobber a previously-filled
        one)."""
        self.extracted_slots.update(
            {k: v for k, v in fields.items() if v is not None}
        )

    def reset_slots(self) -> None:
        """Clear booking-flow state (e.g. after a successful booking or an
        abandoned flow)."""
        self.extracted_slots = {}

    def to_observability_dict(self) -> dict[str, Any]:
        """Compact snapshot for structured JSON logging (Step 5 observability)."""
        return {
            "connection_id": self.connection_id,
            "tenant_id": self.tenant_id,
            "call_id": self.call_id,
            "current_utterance_id": self.current_utterance_id,
            "extracted_slots": dict(self.extracted_slots),
            "turns": len(self.chat_history),
            "age_sec": round(time.time() - self.created_at, 2),
        }


class SessionManager:
    """Central in-memory registry of active CallSessions, keyed by connection_id."""

    def __init__(self) -> None:
        self._sessions: dict[str, CallSession] = {}

    def create(
        self,
        connection_id: str,
        tenant_id: str,
        call_id: Optional[str] = None,
    ) -> CallSession:
        session = CallSession(connection_id, tenant_id, call_id)
        self._sessions[connection_id] = session
        return session

    def get(self, connection_id: str) -> Optional[CallSession]:
        return self._sessions.get(connection_id)

    def remove(self, connection_id: str) -> None:
        """Idempotent - safe to call even if the session was never created or
        already removed (e.g. from a `finally` cleanup block)."""
        self._sessions.pop(connection_id, None)

    def active_count(self) -> int:
        return len(self._sessions)

    def all_sessions(self) -> list[CallSession]:
        return list(self._sessions.values())


# Global singleton - imported directly by main.py (Step 5). Unlike
# cache_service/voice_router/packet_scheduler (which are created inside
# main.py's lifespan() because they need async setup), SessionManager has no
# async initialization, so it's safe to instantiate at import time.
session_manager = SessionManager()
