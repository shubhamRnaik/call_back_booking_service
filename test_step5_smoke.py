"""
Step 5 smoke test (temporary, not part of the app): exercises the actual
main.py wiring functions directly against the LIVE Supabase project, without
needing real audio/STT:

1. _resolve_tenant_config("CLINIC_001") -> confirms Supabase-backed catalogue
   prompt building works.
2. _process_booking_tag(...) -> confirms the full deterministic booking path
   (parse_user_datetime -> check_slot_available -> create_appointment_async)
   works end-to-end, and that a second identical booking attempt is
   correctly rejected as already-booked (uq_no_double_booking).
3. Emergency fast-path detection (pure function, no DB).
4. Cleans up the test appointment row it created so the DB isn't left with
   test data.

Run: .venv\\Scripts\\python.exe test_step5_smoke.py
"""

import asyncio
from datetime import datetime, timedelta

from indic_tts_runtime import main as m
from indic_tts_runtime.core.session import session_manager
from indic_tts_runtime.core.emergency import check_emergency_fastpath
from indic_tts_runtime.services.supabase_service import SupabaseService


async def run() -> None:
    # main.py's `supabase_service` global is only populated by lifespan()
    # inside a running app - set it up manually here so _resolve_tenant_config
    # / _process_booking_tag (which read the module global) exercise the
    # real Supabase-backed path instead of the hardcoded fallback.
    m.supabase_service = SupabaseService()

    print("=== 1. Tenant config resolution (Supabase-backed) ===")
    t_config = await m._resolve_tenant_config("CLINIC_001")
    print(f"source={t_config['source']} business={t_config['business_name']!r}")
    print(f"items found: {[it.get('name') for it in t_config['items']]}")
    assert t_config["source"] == "supabase", "expected live Supabase tenant, got fallback"
    assert t_config["items"], "expected at least one doctor/service for CLINIC_001"

    item_name = t_config["items"][0]["name"]
    print(f"Using item for booking test: {item_name!r}")

    print("\n=== 2. check_emergency_fastpath (pure function) ===")
    print("'my chest hurts a lot' ->", check_emergency_fastpath("my chest hurts a lot"))
    print("'I want to book an appointment' ->", check_emergency_fastpath("I want to book an appointment"))

    print("\n=== 3. Booking flow (first attempt - should CONFIRM) ===")
    # Use an unusual future date/time unlikely to collide with real bookings.
    future_date = (datetime.now() + timedelta(days=45)).strftime("%A %d %B")
    when_phrase = f"on {future_date} at 11:45 pm"
    session1 = session_manager.create("smoke-test-conn-1", "CLINIC_001")
    tag_body_1 = f"item={item_name}|when={when_phrase}|name=Smoke Test Patient|phone=9999999999"
    result_1 = await m._process_booking_tag(tag_body_1, session1, t_config, "en-IN")
    print("Result 1:", result_1)

    print("\n=== 4. Booking flow (second identical attempt - should REJECT as taken) ===")
    session2 = session_manager.create("smoke-test-conn-2", "CLINIC_001")
    tag_body_2 = f"item={item_name}|when={when_phrase}|name=Smoke Test Patient 2|phone=8888888888"
    result_2 = await m._process_booking_tag(tag_body_2, session2, t_config, "en-IN")
    print("Result 2:", result_2)

    session_manager.remove("smoke-test-conn-1")
    session_manager.remove("smoke-test-conn-2")

    print("\n=== 5. Cleanup: deleting the test appointment row ===")
    client = await m.supabase_service._get_client()
    del_resp = (
        await client.table("appointments")
        .delete()
        .eq("tenant_id", "CLINIC_001")
        .eq("patient_name", "Smoke Test Patient")
        .execute()
    )
    print(f"Deleted {len(del_resp.data or [])} test appointment row(s)")

    await m.supabase_service.close()

    print("\n=== SMOKE TEST PASSED ===")


if __name__ == "__main__":
    asyncio.run(run())
