"""
Pre-Deploy Edge Case Regression Suite.

Run this whole file whenever ANY new feature is added to the project, to
confirm no existing feature (DB constraints, in-memory session state,
booking-tag parsing, emergency fast-path, datetime parsing, config startup
validation) has broken.

    .venv\\Scripts\\python.exe -m pytest tests\\test_pre_deploy_edge_cases.py -v

Sections A and D talk to the REAL Supabase project configured in `.env`
(same project already exercised by test_step5_smoke.py /
test_supabase_connection.py) - this is required because the guarantees under
test (`uq_no_double_booking` partial unique index, `idempotency_key` unique
column, foreign keys) are Postgres-specific and cannot be validated against
a mock/sqlite DB. Every row these tests create is tagged with a unique
per-run marker (`PRETEST_<name>_<run_id>`) and deleted in an autouse
teardown fixture, win or lose. If Supabase is unreachable, sections A/C/D are
SKIPPED (not failed) automatically.

Sections B, E, F, G, and the pure-parsing half of C need no network access
and always run.

Several tests intentionally document CURRENT (not necessarily desired)
behavior for gaps identified in the checklist below - see the "KNOWN GAP"
docstrings on: test_A10 (doctor ON_LEAVE not enforced by check_slot_available),
test_D25c (working_hours not validated), test_D25f (missing patient name
silently defaults to "Caller"), test_C6 (missing phone silently books with a
blank phone number), and test_E2 (a "cannot breathe" false-positive
substring match). These are locked in as explicit, visible assertions - not
silently patched - per the checklist's instruction to "write the test either
way so the decision is explicit, not accidental."

===============================================================================
Original checklist this file implements (verbatim, for traceability):
===============================================================================

# Pre-Deploy Edge Case Checklist (no live call required)

Everything here is testable via unit tests, direct DB queries, or scripted
calls to internal functions - no Exotel/Sarvam phone call needed.

## A. Database - Supabase / `appointments` table
1. Double-booking is actually blocked at the DB level (23505 on second insert).
2. Cancelled slots free up correctly.
3. Idempotency key doesn't block legitimate re-bookings.
4. Same call_id + same attempt_nonce submitted twice -> DUPLICATE_RETRY_IGNORED.
5. Range-overlap check catches partial overlaps, not just exact matches.
6. Adjacent (non-overlapping) slots are allowed.
7. Query against a tenant/item/date with zero existing bookings returns True.
8. Concurrent booking race - exactly one CONFIRMED, other ALREADY_BOOKED.
9. Malformed/missing tenant_id - get_tenant_and_items returns None cleanly.
10. Doctor marked ON_LEAVE mid-cache-window - booking check reflects live DB state.

## B. CallSession / SessionManager (in-memory state)
1. next_utterance_id() monotonically increments and never resets.
2. is_current_utterance() correctly invalidates stale IDs.
3. chat_history rolling window actually caps at 12 (most recent).
4. update_slots() doesn't clobber existing values with None.
5. reset_slots() actually clears everything.
6. SessionManager.remove() is safe to call twice / on a nonexistent id.
7. SessionManager.active_count() reflects reality after concurrent create/remove.
8. caller_phone is None by default.
9. set_caller_phone("") does not overwrite a previously set real number.

## C. Booking tag parsing & business logic (main.py)
1. _parse_booking_tag_fields handles missing fields gracefully.
2. _parse_booking_tag_fields handles a value containing a literal "|" or "=".
3. _process_booking_tag with a non-matching item name -> need_service.
4. _process_booking_tag with an unparseable "when" phrase -> need_datetime.
5. End-to-end booking with patient_phone omitted but session.caller_phone set.
6. End-to-end booking with patient_phone omitted AND session.caller_phone None.

## D. Booking creation itself (create_appointment_async)
25a. Successful booking returns the actual DB row / fields back.
25b. item_id mismatch/None -> fails gracefully, no orphaned row.
25c. Booking for a slot outside working_hours - confirm whether validated.
25d. slot_duration_mins correctly drives end_mins.
25e. Two different tenants booking the same name/time do NOT conflict.
25f. patient_name missing entirely - confirm current fallback behavior.
25g. Booking succeeds but caller disconnects before confirmation is spoken -
     DB row must survive, and a retry must not create a duplicate.
25h. session.next_attempt_nonce() gives different nonces across two attempts.

## E. Emergency fast-path (core/emergency.py)
1. Every listed trigger phrase actually matches, including case variations.
2. Similar-but-different phrases do NOT false-positive (or are documented).
3. Emergency check happens before any LLM call.

## F. Datetime parsing (core/datetime_utils.py)
1. Relative dates anchored correctly across a day boundary.
2. Ambiguous/unparseable phrases return None cleanly.
3. Times near midnight compute start_mins/end_mins correctly (no wrap/negative).

## G. Config / startup sanity
1. Empty-string SUPABASE_URL/SUPABASE_KEY are rejected, not silently accepted.
2. _probe_supabase_service() returns a clean (False, message) tuple on failure.
===============================================================================
"""

import asyncio
import json
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from postgrest.exceptions import APIError
from pydantic import ValidationError

from indic_tts_runtime import main as m
from indic_tts_runtime.config import Settings
from indic_tts_runtime.core import datetime_utils
from indic_tts_runtime.core.datetime_utils import parse_user_datetime
from indic_tts_runtime.core.emergency import _EMERGENCY_KEYWORDS, check_emergency_fastpath
from indic_tts_runtime.core.session import CallSession, SessionManager, session_manager
from indic_tts_runtime.normalizer import MultilingualTextNormalizer
from indic_tts_runtime.services.stt_service import STTEvent
from indic_tts_runtime.services.supabase_service import SupabaseService

# Unique per test-run marker so repeated/parallel runs never collide and
# cleanup is trivially scoped by a LIKE pattern on item_name.
RUN_ID = uuid.uuid4().hex[:8]
TENANT_A = "CLINIC_001"
TENANT_B = "PARLOUR_001"


def _tag(name: str) -> str:
    return f"PRETEST_{name}_{RUN_ID}"


def _make_row(**overrides) -> dict:
    row = {
        "tenant_id": TENANT_A,
        "item_id": None,
        "item_name": _tag("default"),
        "date_str": "2099-01-01",
        "start_time_mins": 600,
        "end_time_mins": 630,
        "display_time_str": "10:00 AM",
        "patient_name": "Pretest Patient",
        "patient_phone": "9990000000",
        "idempotency_key": f"pretest:{uuid.uuid4().hex}",
        "status": "CONFIRMED",
    }
    row.update(overrides)
    return row


# ---------------------------------------------------------------------------
# Live Supabase fixture - FUNCTION-scoped (not session-scoped) on purpose:
# pytest-asyncio gives each async test function its own event loop, and
# Supabase's async httpx/HTTP2 connection is bound to the loop it was created
# on - reusing one client across loops raises a cryptic
# "AttributeError: 'NoneType' object has no attribute 'send'" deep in
# asyncio's proactor transport on Windows. A fresh client per test avoids
# this at the cost of one extra connectivity probe per test.
# ---------------------------------------------------------------------------
@pytest_asyncio.fixture
async def supabase_svc():
    svc = SupabaseService()
    ok, msg = await svc.check_connectivity()
    if not ok:
        pytest.skip(f"Live Supabase unreachable - skipping DB-dependent tests: {msg}")
    yield svc
    await svc.close()


async def _delete_pretest_rows(svc: SupabaseService) -> None:
    client = await svc._get_client()
    try:
        await (
            client.table("appointments")
            .delete()
            .like("item_name", f"PRETEST_%{RUN_ID}%")
            .execute()
        )
    except Exception:
        pass


# =============================================================================
# Section A - Database / appointments table (live Supabase)
# =============================================================================
class TestSectionA_Database:
    @pytest_asyncio.fixture(autouse=True)
    async def _cleanup(self, supabase_svc):
        yield
        await _delete_pretest_rows(supabase_svc)

    @pytest.mark.asyncio
    async def test_A1_double_booking_blocked_at_db_level(self, supabase_svc):
        client = await supabase_svc._get_client()
        item_name = _tag("A1")
        row1 = _make_row(
            item_name=item_name, date_str="2099-02-01",
            start_time_mins=540, end_time_mins=570,
        )
        row2 = dict(row1, idempotency_key=f"pretest:{uuid.uuid4().hex}")

        await client.table("appointments").insert(row1).execute()
        with pytest.raises(APIError) as exc_info:
            await client.table("appointments").insert(row2).execute()
        assert getattr(exc_info.value, "code", None) == "23505"

    @pytest.mark.asyncio
    async def test_A2_cancelled_slot_frees_up(self, supabase_svc):
        client = await supabase_svc._get_client()
        item_name = _tag("A2")
        row = _make_row(item_name=item_name, date_str="2099-02-02", start_time_mins=600, end_time_mins=630)
        insert_resp = await client.table("appointments").insert(row).execute()
        appt_id = insert_resp.data[0]["id"]

        await client.table("appointments").update({"status": "CANCELLED"}).eq("id", appt_id).execute()

        row2 = dict(row, idempotency_key=f"pretest:{uuid.uuid4().hex}")
        resp2 = await client.table("appointments").insert(row2).execute()
        assert resp2.data, "Re-booking the same slot after cancellation must succeed"

    @pytest.mark.asyncio
    async def test_A3_idempotency_key_does_not_block_legit_rebooking(self, supabase_svc):
        item_name = _tag("A3")
        date_str = "2099-02-03"
        start, end = 660, 690

        r1 = await supabase_svc.create_appointment_async(
            tenant_id=TENANT_A, item_name=item_name, item_id=None, date_str=date_str,
            start_mins=start, end_mins=end, display_time_str="11:00 AM",
            patient_name="P1", patient_phone="1", call_id=f"call-{uuid.uuid4().hex}", attempt_nonce="1",
        )
        assert r1["status"] == "CONFIRMED"
        appt_id = r1["appointment"]["id"]

        client = await supabase_svc._get_client()
        await client.table("appointments").update({"status": "CANCELLED"}).eq("id", appt_id).execute()

        # A DIFFERENT call_id re-books the exact same slot - must succeed.
        r2 = await supabase_svc.create_appointment_async(
            tenant_id=TENANT_A, item_name=item_name, item_id=None, date_str=date_str,
            start_mins=start, end_mins=end, display_time_str="11:00 AM",
            patient_name="P2", patient_phone="2", call_id=f"call-{uuid.uuid4().hex}", attempt_nonce="1",
        )
        assert r2["status"] == "CONFIRMED"

    @pytest.mark.asyncio
    async def test_A4_duplicate_retry_same_call_id_and_nonce_ignored(self, supabase_svc):
        item_name = _tag("A4")
        call_id = f"call-{uuid.uuid4().hex}"
        kwargs = dict(
            tenant_id=TENANT_A, item_name=item_name, item_id=None, date_str="2099-02-04",
            start_mins=720, end_mins=750, display_time_str="12:00 PM",
            patient_name="P", patient_phone="1", call_id=call_id, attempt_nonce="1",
        )
        r1 = await supabase_svc.create_appointment_async(**kwargs)
        assert r1["status"] == "CONFIRMED"
        r2 = await supabase_svc.create_appointment_async(**kwargs)
        assert r2["status"] == "DUPLICATE_RETRY_IGNORED"

    @pytest.mark.asyncio
    async def test_A5_partial_overlap_detected(self, supabase_svc):
        item_name = _tag("A5")
        date_str = "2099-02-05"
        await supabase_svc.create_appointment_async(
            tenant_id=TENANT_A, item_name=item_name, item_id=None, date_str=date_str,
            start_mins=1080, end_mins=1110, display_time_str="06:00 PM",
            patient_name="P", patient_phone="1", call_id=f"call-{uuid.uuid4().hex}", attempt_nonce="1",
        )
        available, _ = await supabase_svc.check_slot_available(
            tenant_id=TENANT_A, item_name=item_name, date_str=date_str,
            proposed_start_mins=1095, proposed_end_mins=1125,
        )
        assert available is False, "18:15-18:45 genuinely overlaps an existing 18:00-18:30 booking"

    @pytest.mark.asyncio
    async def test_A6_adjacent_slots_allowed(self, supabase_svc):
        item_name = _tag("A6")
        date_str = "2099-02-06"
        await supabase_svc.create_appointment_async(
            tenant_id=TENANT_A, item_name=item_name, item_id=None, date_str=date_str,
            start_mins=1080, end_mins=1110, display_time_str="06:00 PM",
            patient_name="P", patient_phone="1", call_id=f"call-{uuid.uuid4().hex}", attempt_nonce="1",
        )
        available, _ = await supabase_svc.check_slot_available(
            tenant_id=TENANT_A, item_name=item_name, date_str=date_str,
            proposed_start_mins=1110, proposed_end_mins=1140,
        )
        assert available is True, "Back-to-back slots (end == start) must not count as overlap"

    @pytest.mark.asyncio
    async def test_A7_zero_existing_bookings_returns_true(self, supabase_svc):
        item_name = _tag("A7_never_booked")
        available, _ = await supabase_svc.check_slot_available(
            tenant_id=TENANT_A, item_name=item_name, date_str="2099-02-07",
            proposed_start_mins=100, proposed_end_mins=130,
        )
        assert available is True

    @pytest.mark.asyncio
    async def test_A8_concurrent_booking_race_exactly_one_wins(self, supabase_svc):
        item_name = _tag("A8")
        date_str = "2099-02-08"
        kwargs_common = dict(
            tenant_id=TENANT_A, item_name=item_name, item_id=None, date_str=date_str,
            start_mins=900, end_mins=930, display_time_str="03:00 PM",
            patient_name="P", patient_phone="1",
        )
        r1, r2 = await asyncio.gather(
            supabase_svc.create_appointment_async(
                **kwargs_common, call_id=f"call-{uuid.uuid4().hex}", attempt_nonce="1"
            ),
            supabase_svc.create_appointment_async(
                **kwargs_common, call_id=f"call-{uuid.uuid4().hex}", attempt_nonce="1"
            ),
        )
        statuses = sorted([r1["status"], r2["status"]])
        assert statuses == ["ALREADY_BOOKED", "CONFIRMED"], (
            f"Expected exactly one CONFIRMED and one ALREADY_BOOKED, got {statuses}"
        )

    @pytest.mark.asyncio
    async def test_A9_missing_tenant_returns_none_and_falls_back(self, supabase_svc):
        result = await supabase_svc.get_tenant_and_items("NONEXISTENT_TENANT_XYZ")
        assert result is None

        prev = m.supabase_service
        m.supabase_service = supabase_svc
        try:
            cfg = await m._resolve_tenant_config("NONEXISTENT_TENANT_XYZ")
            assert cfg["source"] == "fallback_hardcoded"
            assert cfg["items"] == []
        finally:
            m.supabase_service = prev

    @pytest.mark.asyncio
    async def test_A10_on_leave_status_not_enforced_by_check_slot_available(self, supabase_svc):
        """
        KNOWN GAP (documented, not fixed here): check_slot_available() only
        queries the `appointments` table's time-overlap - it never reads
        doctors_or_services.status. So an ON_LEAVE doctor with a genuinely
        free slot still passes check_slot_available and could be booked.
        This locks in that CURRENT behavior so a future fix (checking doctor
        status at booking time) is a deliberate, visible change.
        """
        client = await supabase_svc._get_client()
        doctor_resp = (
            await client.table("doctors_or_services")
            .select("id, status")
            .eq("tenant_id", TENANT_A)
            .eq("name", "Dr. Sharma")
            .maybe_single()
            .execute()
        )
        doctor_row = doctor_resp.data
        assert doctor_row, "Expected the Dr. Sharma seed row to exist in CLINIC_001"
        original_status = doctor_row["status"]

        try:
            await (
                client.table("doctors_or_services")
                .update({"status": "ON_LEAVE"})
                .eq("id", doctor_row["id"])
                .execute()
            )

            available, _ = await supabase_svc.check_slot_available(
                tenant_id=TENANT_A, item_name="Dr. Sharma", date_str="2099-03-01",
                proposed_start_mins=1080, proposed_end_mins=1110,
            )
            assert available is True, (
                "Documents the known gap: ON_LEAVE status is not enforced by "
                "check_slot_available (only appointment time-overlap is)."
            )
        finally:
            await (
                client.table("doctors_or_services")
                .update({"status": original_status})
                .eq("id", doctor_row["id"])
                .execute()
            )


# =============================================================================
# Section B - CallSession / SessionManager (pure in-memory, no network)
# =============================================================================
class TestSectionB_Session:
    def test_B1_next_utterance_id_monotonically_increments(self):
        session = CallSession(connection_id="b1", tenant_id=TENANT_A)
        seen = [session.next_utterance_id() for _ in range(50)]
        assert seen == sorted(seen)
        assert len(set(seen)) == 50
        assert seen[-1] == 50

    def test_B2_is_current_utterance_invalidates_stale_ids(self):
        session = CallSession(connection_id="b2", tenant_id=TENANT_A)
        old_id = session.next_utterance_id()
        assert session.is_current_utterance(old_id) is True
        session.next_utterance_id()
        assert session.is_current_utterance(old_id) is False

    def test_B3_chat_history_caps_at_12_keeping_most_recent(self):
        session = CallSession(connection_id="b3", tenant_id=TENANT_A)
        for i in range(20):
            session.add_turn("user", f"turn-{i}")
        assert len(session.chat_history) == 12
        texts = [t["text"] for t in session.chat_history]
        assert texts == [f"turn-{i}" for i in range(8, 20)]

    def test_B4_update_slots_does_not_clobber_with_none(self):
        session = CallSession(connection_id="b4", tenant_id=TENANT_A)
        session.update_slots(item_name="Dr. Sharma")
        session.update_slots(item_name=None, date_str="2026-08-01")
        assert session.extracted_slots["item_name"] == "Dr. Sharma"
        assert session.extracted_slots["date_str"] == "2026-08-01"

    def test_B5_reset_slots_clears_everything(self):
        session = CallSession(connection_id="b5", tenant_id=TENANT_A)
        session.update_slots(item_name="X", date_str="Y", patient_name="Z")
        session.reset_slots()
        assert session.extracted_slots == {}

    def test_B6_session_manager_remove_is_idempotent(self):
        sm = SessionManager()
        sm.create("b6", TENANT_A)
        sm.remove("b6")
        sm.remove("b6")  # must not raise
        sm.remove("never-existed")  # must not raise

    def test_B7_active_count_reflects_concurrent_create_remove(self):
        sm = SessionManager()
        for i in range(5):
            sm.create(f"conn-{i}", TENANT_A)
        assert sm.active_count() == 5
        sm.remove("conn-0")
        sm.remove("conn-1")
        assert sm.active_count() == 3

    def test_B8_caller_phone_none_by_default(self):
        session = CallSession(connection_id="b8", tenant_id=TENANT_A)
        assert session.caller_phone is None
        session.set_caller_phone("+919999999999")
        assert session.caller_phone == "+919999999999"

    def test_B9_set_caller_phone_empty_string_does_not_overwrite(self):
        session = CallSession(connection_id="b9", tenant_id=TENANT_A)
        session.set_caller_phone("+911111111111")
        session.set_caller_phone("")
        assert session.caller_phone == "+911111111111"


# =============================================================================
# Section C - Booking tag parsing (pure) + live end-to-end phone fallback
# =============================================================================
class TestSectionC_ParsingPure:
    def test_C1_parse_booking_tag_fields_handles_missing_fields(self):
        fields = m._parse_booking_tag_fields("item=Dr. Sharma|when=tomorrow 6pm")
        assert fields == {"item": "Dr. Sharma", "when": "tomorrow 6pm"}
        assert "name" not in fields
        assert "phone" not in fields

    def test_C2_parse_booking_tag_fields_equals_ok_pipe_is_a_known_limitation(self):
        # A literal "=" inside a value IS handled correctly - partition()
        # only splits on the FIRST "=".
        fields = m._parse_booking_tag_fields("item=X|name=Ravi=Kumar")
        assert fields["name"] == "Ravi=Kumar"

        # KNOWN LIMITATION: a literal "|" inside a value is NOT safe, since
        # "|" is the field separator itself - the tail after the "|" silently
        # becomes its own (usually discarded) part instead of staying joined.
        fields2 = m._parse_booking_tag_fields("item=X|name=Kumar|Jr")
        assert fields2["name"] == "Kumar"
        assert "Jr" not in fields2.values()

    @pytest.mark.asyncio
    async def test_C3_process_booking_tag_no_matching_item_returns_need_service(self):
        session = CallSession(connection_id="c3", tenant_id="default")
        tenant_config = {
            "items": [{"name": "Dr. Sharma", "id": "1", "slot_duration_mins": 30}],
            "timezone": "Asia/Kolkata",
        }
        tag_body = "item=Dr. XYZQ Nonexistent|when=tomorrow at 5pm|name=Test"
        response = await m._process_booking_tag(tag_body, session, tenant_config, "en-IN")
        assert response == m._localized("need_service", "en-IN")

    @pytest.mark.asyncio
    async def test_C4_process_booking_tag_unparseable_when_returns_need_datetime(self):
        session = CallSession(connection_id="c4", tenant_id="default")
        tenant_config = {
            "items": [{"name": "Dr. Sharma", "id": "1", "slot_duration_mins": 30}],
            "timezone": "Asia/Kolkata",
        }
        tag_body = "item=Dr. Sharma|when=sometime next week maybe|name=Test"
        response = await m._process_booking_tag(tag_body, session, tenant_config, "en-IN")
        assert response == m._localized("need_datetime", "en-IN")


class TestSectionC_LiveBookingDB:
    @pytest_asyncio.fixture(autouse=True)
    async def _cleanup(self, supabase_svc):
        yield
        await _delete_pretest_rows(supabase_svc)

    @pytest.mark.asyncio
    async def test_C5_omitted_phone_falls_back_to_session_caller_phone_in_db(self, supabase_svc):
        prev = m.supabase_service
        m.supabase_service = supabase_svc
        try:
            item_name = _tag("C5")
            session = CallSession(connection_id=f"conn-{uuid.uuid4().hex}", tenant_id=TENANT_A)
            session.set_caller_phone("+919876500000")
            tenant_config = {
                "items": [{"name": item_name, "id": None, "slot_duration_mins": 30}],
                "timezone": "Asia/Kolkata",
            }
            tag_body = f"item={item_name}|when=tomorrow at 6pm|name=Test Caller"  # no phone= field
            response = await m._process_booking_tag(tag_body, session, tenant_config, "en-IN")
            assert "confirmed" in response.lower(), response

            client = await supabase_svc._get_client()
            check = await client.table("appointments").select("patient_phone").eq("item_name", item_name).execute()
            assert check.data
            assert check.data[0]["patient_phone"] == "+919876500000"
        finally:
            m.supabase_service = prev

    @pytest.mark.asyncio
    async def test_C6_omitted_phone_and_no_caller_phone_known_gap(self, supabase_svc):
        """
        KNOWN GAP: when neither the tag's phone= field nor
        session.caller_phone is available, _process_booking_tag falls back to
        patient_phone="" - and Postgres' `patient_phone TEXT NOT NULL`
        constraint is satisfied by an empty string (only NULL is rejected),
        so the booking SUCCEEDS silently with a blank phone number instead of
        failing gracefully or asking the caller for one. Locking in the
        actual current behavior so a future fix (require non-blank phone) is
        a deliberate, visible change.
        """
        prev = m.supabase_service
        m.supabase_service = supabase_svc
        try:
            item_name = _tag("C6")
            session = CallSession(connection_id=f"conn-{uuid.uuid4().hex}", tenant_id=TENANT_A)
            assert session.caller_phone is None
            tenant_config = {
                "items": [{"name": item_name, "id": None, "slot_duration_mins": 30}],
                "timezone": "Asia/Kolkata",
            }
            tag_body = f"item={item_name}|when=tomorrow at 7pm|name=Test Caller"  # no phone=
            response = await m._process_booking_tag(tag_body, session, tenant_config, "en-IN")

            # Must always be a clean localized string - never an unhandled
            # exception/500 bubbling out of _process_booking_tag.
            assert isinstance(response, str) and response

            client = await supabase_svc._get_client()
            check = await client.table("appointments").select("patient_phone").eq("item_name", item_name).execute()
            assert check.data, "Documents that the booking currently succeeds despite no phone at all"
            assert check.data[0]["patient_phone"] == ""
        finally:
            m.supabase_service = prev


# =============================================================================
# Section D - Booking creation itself (create_appointment_async), live DB
# =============================================================================
class TestSectionD_BookingCreation:
    @pytest_asyncio.fixture(autouse=True)
    async def _cleanup(self, supabase_svc):
        yield
        await _delete_pretest_rows(supabase_svc)

    @pytest.mark.asyncio
    async def test_D25a_successful_booking_returns_real_db_fields(self, supabase_svc):
        item_name = _tag("D25a")
        result = await supabase_svc.create_appointment_async(
            tenant_id=TENANT_A, item_name=item_name, item_id=None, date_str="2099-04-01",
            start_mins=600, end_mins=630, display_time_str="10:00 AM",
            patient_name="P", patient_phone="1", call_id=f"call-{uuid.uuid4().hex}", attempt_nonce="1",
        )
        assert result["status"] == "CONFIRMED"
        appt = result["appointment"]
        assert appt is not None and appt["id"]
        assert appt["date_str"] == "2099-04-01"
        assert appt["start_time_mins"] == 600
        assert appt["display_time_str"] == "10:00 AM"

    @pytest.mark.asyncio
    async def test_D25b_bad_item_id_fails_gracefully_not_orphaned_row(self, supabase_svc):
        prev = m.supabase_service
        m.supabase_service = supabase_svc
        try:
            item_name = _tag("D25b")
            fake_item_id = str(uuid.uuid4())  # valid UUID format, does not exist -> FK violation
            session = CallSession(connection_id=f"conn-{uuid.uuid4().hex}", tenant_id=TENANT_A)
            tenant_config = {
                "items": [{"name": item_name, "id": fake_item_id, "slot_duration_mins": 30}],
                "timezone": "Asia/Kolkata",
            }
            tag_body = f"item={item_name}|when=tomorrow at 5pm|name=Test|phone=9998887777"
            response = await m._process_booking_tag(tag_body, session, tenant_config, "en-IN")
            assert response == m._localized("booking_error", "en-IN")

            client = await supabase_svc._get_client()
            check = await client.table("appointments").select("id").eq("item_name", item_name).execute()
            assert not check.data, "Expected no orphaned row after an FK violation on item_id"
        finally:
            m.supabase_service = prev

    @pytest.mark.asyncio
    async def test_D25c_out_of_hours_booking_not_validated_known_gap(self, supabase_svc):
        """
        KNOWN GAP: _process_booking_tag never checks the matched item's
        working_hours against the parsed booking time. Booking 09:00 for an
        item whose working_hours would exclude that time still succeeds.
        Documents the current (unvalidated) behavior explicitly.
        """
        prev = m.supabase_service
        m.supabase_service = supabase_svc
        try:
            item_name = _tag("D25c")
            session = CallSession(connection_id=f"conn-{uuid.uuid4().hex}", tenant_id=TENANT_A)
            tenant_config = {
                "items": [{
                    "name": item_name, "id": None, "slot_duration_mins": 30,
                    "working_hours": "17:00-22:00",
                }],
                "timezone": "Asia/Kolkata",
            }
            tag_body = f"item={item_name}|when=tomorrow at 9am|name=Test|phone=9998887777"
            response = await m._process_booking_tag(tag_body, session, tenant_config, "en-IN")
            assert "confirmed" in response.lower(), (
                f"Documents that out-of-working-hours booking is currently NOT "
                f"rejected. Bot response: {response!r}"
            )
        finally:
            m.supabase_service = prev

    @pytest.mark.asyncio
    async def test_D25d_slot_duration_mins_drives_end_mins(self, supabase_svc):
        prev = m.supabase_service
        m.supabase_service = supabase_svc
        try:
            item_name = _tag("D25d")
            session = CallSession(connection_id=f"conn-{uuid.uuid4().hex}", tenant_id=TENANT_A)
            tenant_config = {
                "items": [{"name": item_name, "id": None, "slot_duration_mins": 45}],
                "timezone": "Asia/Kolkata",
            }
            tag_body = f"item={item_name}|when=tomorrow at 10am|name=Test|phone=9998887777"
            response = await m._process_booking_tag(tag_body, session, tenant_config, "en-IN")
            assert "confirmed" in response.lower(), response

            client = await supabase_svc._get_client()
            check = (
                await client.table("appointments")
                .select("start_time_mins, end_time_mins")
                .eq("item_name", item_name)
                .execute()
            )
            assert check.data
            row = check.data[0]
            assert row["start_time_mins"] == 600  # 10:00 AM
            assert row["end_time_mins"] == 645  # 10:45 AM (45 min duration, not a default 30)
        finally:
            m.supabase_service = prev

    @pytest.mark.asyncio
    async def test_D25e_same_name_time_different_tenants_no_conflict(self, supabase_svc):
        item_name = _tag("D25e_Consultation")
        date_str = "2099-05-01"
        r1 = await supabase_svc.create_appointment_async(
            tenant_id=TENANT_A, item_name=item_name, item_id=None, date_str=date_str,
            start_mins=1080, end_mins=1110, display_time_str="06:00 PM",
            patient_name="P1", patient_phone="1", call_id=f"call-{uuid.uuid4().hex}", attempt_nonce="1",
        )
        r2 = await supabase_svc.create_appointment_async(
            tenant_id=TENANT_B, item_name=item_name, item_id=None, date_str=date_str,
            start_mins=1080, end_mins=1110, display_time_str="06:00 PM",
            patient_name="P2", patient_phone="2", call_id=f"call-{uuid.uuid4().hex}", attempt_nonce="1",
        )
        assert r1["status"] == "CONFIRMED"
        assert r2["status"] == "CONFIRMED", "Unique index must be scoped per-tenant, not global"

    @pytest.mark.asyncio
    async def test_D25f_missing_patient_name_defaults_to_caller_known_gap(self, supabase_svc):
        """
        KNOWN GAP: _process_booking_tag never checks for a missing `name=`
        field before booking - it silently falls back to `patient_name or
        "Caller"` and writes that placeholder to the DB instead of asking the
        caller for their name. Locks in the current behavior; if the product
        decision changes to require a name, update this test to expect a
        clarification response instead.
        """
        prev = m.supabase_service
        m.supabase_service = supabase_svc
        try:
            item_name = _tag("D25f")
            session = CallSession(connection_id=f"conn-{uuid.uuid4().hex}", tenant_id=TENANT_A)
            tenant_config = {
                "items": [{"name": item_name, "id": None, "slot_duration_mins": 30}],
                "timezone": "Asia/Kolkata",
            }
            tag_body = f"item={item_name}|when=tomorrow at 3pm"  # no name= at all
            response = await m._process_booking_tag(tag_body, session, tenant_config, "en-IN")
            assert "confirmed" in response.lower(), response

            client = await supabase_svc._get_client()
            check = await client.table("appointments").select("patient_name").eq("item_name", item_name).execute()
            assert check.data
            assert check.data[0]["patient_name"] == "Caller"
        finally:
            m.supabase_service = prev

    @pytest.mark.asyncio
    async def test_D25g_booking_survives_disconnect_and_no_duplicate_on_retry(self, supabase_svc):
        prev = m.supabase_service
        m.supabase_service = supabase_svc
        try:
            item_name = _tag("D25g")
            tenant_config = {
                "items": [{"name": item_name, "id": None, "slot_duration_mins": 30}],
                "timezone": "Asia/Kolkata",
            }
            tag_body = f"item={item_name}|when=tomorrow at 4pm|name=First Caller|phone=1111111111"

            session1 = CallSession(connection_id=f"conn-{uuid.uuid4().hex}", tenant_id=TENANT_A)
            response1 = await m._process_booking_tag(tag_body, session1, tenant_config, "en-IN")
            assert "confirmed" in response1.lower(), response1

            # Simulate the WebSocket dropping right after CONFIRMED: the row
            # must survive (never rolled back) - exactly one CONFIRMED row.
            client = await supabase_svc._get_client()
            rows = (
                await client.table("appointments")
                .select("id, status")
                .eq("item_name", item_name)
                .eq("status", "CONFIRMED")
                .execute()
            )
            assert len(rows.data) == 1

            # Simulate Exotel retrying the connection with a brand new
            # session/call_id attempting the SAME booking again.
            session2 = CallSession(connection_id=f"conn-{uuid.uuid4().hex}", tenant_id=TENANT_A)
            response2 = await m._process_booking_tag(tag_body, session2, tenant_config, "en-IN")
            assert response2 == m._localized("slot_taken", "en-IN")

            rows_after = (
                await client.table("appointments")
                .select("id")
                .eq("item_name", item_name)
                .eq("status", "CONFIRMED")
                .execute()
            )
            assert len(rows_after.data) == 1, "Retry must not create a duplicate booking"
        finally:
            m.supabase_service = prev

    def test_D25h_next_attempt_nonce_differs_across_two_bookings_same_session(self):
        session = CallSession(connection_id="d25h", tenant_id=TENANT_A)
        n1 = session.next_attempt_nonce()
        n2 = session.next_attempt_nonce()
        assert n1 != n2
        assert {n1, n2} == {"1", "2"}


# =============================================================================
# Section E - Emergency fast-path (core/emergency.py)
# =============================================================================
class TestSectionE_Emergency:
    @pytest.mark.parametrize("keyword", _EMERGENCY_KEYWORDS)
    def test_E1_every_trigger_phrase_matches(self, keyword):
        plain_phrase = keyword.replace("'?", "'")
        assert check_emergency_fastpath(f"caller says {plain_phrase} right now") is True

    def test_E1_case_variations_match(self):
        assert check_emergency_fastpath("Chest Pain") is True
        assert check_emergency_fastpath("CHEST PAIN") is True
        assert check_emergency_fastpath("chest pain") is True

    def test_E1_hindi_hinglish_phrases_match(self):
        assert check_emergency_fastpath("mujhe dil ka daura pad raha hai") is True
        assert check_emergency_fastpath("saans nahi aa rahi hai please help") is True
        assert check_emergency_fastpath("bachao mera accident ho gaya") is True

    def test_E2_negative_phrase_does_not_false_positive(self):
        assert check_emergency_fastpath("I need to buy a chest of drawers") is False
        assert check_emergency_fastpath("what time do you open tomorrow") is False

    def test_E2_known_false_positive_gap_cannot_breathe_substring(self):
        """
        KNOWN GAP: the "cannot breathe" keyword has no negative lookahead, so
        an innocuous phrase containing that literal substring false-positives
        into the emergency fast-path. Documented explicitly rather than
        silently tightened - narrowing the regex risks a false NEGATIVE on a
        genuine emergency, which this module's own docstring calls out as the
        strictly worse failure mode.
        """
        assert check_emergency_fastpath("I cannot breathe easy in this heat") is True

    @pytest.mark.asyncio
    async def test_E3_emergency_bypasses_llm_call_entirely(self):
        class FakeSTTClientEmergency:
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
                yield STTEvent(
                    event_type="final_transcript",
                    transcript="I think he is having a heart attack please help",
                )
                await asyncio.sleep(3600)

        class ExplodingBrain:
            def __init__(self, system_prompt=None, **kwargs):
                pass

            async def prewarm(self):
                pass

            async def stream_response(self, user_text):
                raise AssertionError("LLM stream_response must NEVER be called for an emergency transcript")
                yield  # pragma: no cover - unreachable, keeps this an async generator

        class FakeTTSClientEmergency:
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

        class FakeWebSocketEmergency:
            def __init__(self):
                self._sent_start = False
                self.headers = {}
                self.client = None

            async def accept(self):
                pass

            async def receive(self):
                if not self._sent_start:
                    self._sent_start = True
                    start_event = {
                        "event": "start",
                        "stream_sid": "sim_stream_emergency",
                        "start": {"stream_sid": "sim_stream_emergency"},
                    }
                    return {"text": json.dumps(start_event)}
                await asyncio.Event().wait()

            async def send_json(self, data):
                pass

            async def close(self, code=1000, reason=""):
                pass

        prev_stt, prev_tts, prev_brain = m.SarvamSaarasSTTClient, m.SarvamWebSocketClient, m.StreamingBrain
        prev_normalizer, prev_supabase = m.text_normalizer, m.supabase_service
        m.SarvamSaarasSTTClient = FakeSTTClientEmergency
        m.SarvamWebSocketClient = FakeTTSClientEmergency
        m.StreamingBrain = ExplodingBrain
        m.text_normalizer = MultilingualTextNormalizer()
        m.supabase_service = None

        task = None
        try:
            fake_ws = FakeWebSocketEmergency()
            task = asyncio.create_task(m._websocket_exotel_stream_impl(fake_ws, tenant_id="default"))
            # The transcript handler debounces final transcripts for
            # DEBOUNCE_SECONDS (1.0s) before dispatching to
            # process_transcript_to_response, so we must wait past that
            # window for the emergency fast-path to actually run.
            await asyncio.sleep(2.0)

            sessions = [s for s in session_manager.all_sessions() if s.call_id == "sim_stream_emergency"]
            assert sessions, "Expected a session to be created for this call"
            session = sessions[0]
            assistant_turns = [t["text"].lower() for t in session.chat_history if t["role"] == "assistant"]
            assert any("emergency" in txt or "connecting you" in txt for txt in assistant_turns), (
                f"Expected an emergency handover phrase, got: {assistant_turns}"
            )
        finally:
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            for s in list(session_manager.all_sessions()):
                if s.call_id == "sim_stream_emergency":
                    session_manager.remove(s.connection_id)
            m.SarvamSaarasSTTClient = prev_stt
            m.SarvamWebSocketClient = prev_tts
            m.StreamingBrain = prev_brain
            m.text_normalizer = prev_normalizer
            m.supabase_service = prev_supabase


# =============================================================================
# Section F - Datetime parsing (core/datetime_utils.py)
# =============================================================================
class TestSectionF_DatetimeParsing:
    def test_F1_day_boundary_anchored_correctly(self, monkeypatch):
        fixed_now = datetime(2026, 7, 24, 23, 58, 0, tzinfo=ZoneInfo("Asia/Kolkata"))

        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return fixed_now

        monkeypatch.setattr(datetime_utils, "datetime", FixedDateTime)
        result = parse_user_datetime("tomorrow at 6pm", tenant_tz="Asia/Kolkata")
        assert result is not None
        date_str, start_mins, _end_mins, _display = result
        assert date_str == "2026-07-25", "11:58 PM 'tomorrow' must resolve to the genuinely next calendar day"
        assert start_mins == 18 * 60

    @pytest.mark.parametrize("phrase", ["sometime", "later", "idk"])
    def test_F2_ambiguous_phrases_return_none_cleanly(self, phrase):
        assert parse_user_datetime(phrase) is None

    def test_F3_near_midnight_no_wrap_or_negative(self):
        result = parse_user_datetime("tomorrow at 11:45 pm")
        assert result is not None
        _date_str, start_mins, end_mins, _display = result
        assert start_mins == 23 * 60 + 45
        assert end_mins >= start_mins
        assert 0 <= end_mins <= 24 * 60 - 1, "end_mins must not wrap past midnight or go negative"


# =============================================================================
# Section G - Config / startup sanity
# =============================================================================
class TestSectionG_ConfigStartup:
    def test_G1_empty_string_supabase_url_rejected(self):
        with pytest.raises(ValidationError):
            Settings(sarvam_api_key="x", supabase_url="", supabase_key="y")

    def test_G1_empty_string_supabase_key_rejected(self):
        with pytest.raises(ValidationError):
            Settings(sarvam_api_key="x", supabase_url="https://example.supabase.co", supabase_key="")

    @pytest.mark.asyncio
    async def test_G2_probe_supabase_service_clean_tuple_on_failure(self):
        class ExplodingSupabase:
            async def check_connectivity(self):
                raise RuntimeError("simulated network failure")

        prev = m.supabase_service
        m.supabase_service = ExplodingSupabase()
        try:
            ok, message = await m._probe_supabase_service()
            assert ok is False
            assert "simulated network failure" in message
        finally:
            m.supabase_service = prev

    @pytest.mark.asyncio
    async def test_G2_probe_supabase_service_not_initialized(self):
        prev = m.supabase_service
        m.supabase_service = None
        try:
            ok, message = await m._probe_supabase_service()
            assert ok is False
            assert message == "not_initialized"
        finally:
            m.supabase_service = prev
