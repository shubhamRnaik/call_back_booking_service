"""
Async Supabase service: tenant/catalogue lookups with a TTL cache, a
range-based slot-overlap UX pre-check, and atomic idempotent appointment
booking.

Uses the official `supabase-py` async client (`acreate_client`), which talks
to Supabase's PostgREST Data API. Booking correctness against concurrent
callers does NOT depend on this service's Python-level logic - it depends on
the Postgres partial unique index `uq_no_double_booking` defined in
database/setup.sql. This service only turns the resulting unique-violation
error into a clean status the caller can react to.
"""

import logging
import time
from typing import Any, Optional

from postgrest.exceptions import APIError
from supabase import acreate_client, AsyncClient

from ..config import settings

logger = logging.getLogger(__name__)

# Postgres error code for unique_violation (used for BOTH the
# uq_no_double_booking partial index and the idempotency_key unique column -
# the two are told apart by inspecting the error message/details).
_PG_UNIQUE_VIOLATION = "23505"


class SupabaseService:
    """
    Async Supabase-backed data access for tenants, doctors/services, and
    appointments.

    Tenant/catalogue reads are cached for `settings.tenant_cache_ttl_sec`
    (default 5 min) to avoid a DB round-trip on every call setup.

    KNOWN TRADEOFF (documented per spec, not accidental): a doctor/service
    marked ON_LEAVE mid-cache-window may still be described as available in
    conversational Q&A for up to the TTL, because the cached snapshot isn't
    invalidated early. This is acceptable because the actual booking write
    always goes through `check_slot_available` + `create_appointment_async`,
    which are NEVER cached and always hit the live DB - so a booking cannot
    succeed against a slot that's actually unavailable, it just means the
    bot might describe a doctor as bookable for a few minutes after they've
    gone on leave, until the cache expires or a booking attempt corrects it.
    """

    def __init__(self) -> None:
        self._client: Optional[AsyncClient] = None
        self._tenant_cache: dict[str, tuple[float, dict]] = {}

    async def _get_client(self) -> AsyncClient:
        if self._client is None:
            self._client = await acreate_client(
                settings.supabase_url, settings.supabase_key
            )
        return self._client

    async def check_connectivity(self) -> tuple[bool, str]:
        """Lightweight startup probe - used by main.py's lifespan fail-fast check."""
        try:
            client = await self._get_client()
            await client.table("tenants").select("tenant_id").limit(1).execute()
            return True, "ok"
        except Exception as exc:
            logger.error(f"Supabase connectivity probe failed: {exc}")
            return False, str(exc)

    async def get_tenant_and_items(self, tenant_id: str) -> Optional[dict]:
        """
        Fetch tenant profile + its doctors/services, cached for
        `tenant_cache_ttl_sec` seconds. Returns None if the tenant doesn't
        exist.
        """
        now = time.time()
        cached = self._tenant_cache.get(tenant_id)
        if cached and (now - cached[0]) < settings.tenant_cache_ttl_sec:
            return cached[1]

        client = await self._get_client()

        tenant_resp = (
            await client.table("tenants")
            .select("*")
            .eq("tenant_id", tenant_id)
            .maybe_single()
            .execute()
        )
        tenant_row = tenant_resp.data if tenant_resp else None
        if not tenant_row:
            return None

        items_resp = (
            await client.table("doctors_or_services")
            .select("*")
            .eq("tenant_id", tenant_id)
            .execute()
        )
        items = items_resp.data or []

        result = {"tenant": tenant_row, "items": items}
        self._tenant_cache[tenant_id] = (now, result)
        return result

    def invalidate_tenant_cache(self, tenant_id: str) -> None:
        """Force the next get_tenant_and_items() call to hit the live DB."""
        self._tenant_cache.pop(tenant_id, None)

    async def check_slot_available(
        self,
        tenant_id: str,
        item_name: str,
        date_str: str,
        proposed_start_mins: int,
        proposed_end_mins: int,
    ) -> tuple[bool, str]:
        """
        UX PRE-CHECK ONLY - narrows the conversation ("that time's taken, try
        7 instead"). This is NOT the source of truth for correctness; the
        `uq_no_double_booking` partial unique index in database/setup.sql is
        the actual race-condition guarantee, enforced at INSERT time in
        create_appointment_async(). Always live (never cached).
        """
        client = await self._get_client()

        resp = (
            await client.table("appointments")
            .select("id", count="exact")
            .eq("tenant_id", tenant_id)
            .eq("item_name", item_name)
            .eq("date_str", date_str)
            .eq("status", "CONFIRMED")
            .lt("start_time_mins", proposed_end_mins)
            .gt("end_time_mins", proposed_start_mins)
            .execute()
        )

        overlap_count = resp.count if resp.count is not None else len(resp.data or [])
        if overlap_count > 0:
            return False, "Slot overlaps with an existing booking."
        return True, "Slot is available."

    async def create_appointment_async(
        self,
        tenant_id: str,
        item_name: str,
        item_id: Optional[str],
        date_str: str,
        start_mins: int,
        end_mins: int,
        display_time_str: str,
        patient_name: str,
        patient_phone: str,
        call_id: str,
        attempt_nonce: str,
    ) -> dict[str, Any]:
        """
        Atomic, idempotent booking write.

        idempotency_key is scoped to THIS booking ATTEMPT (call_id +
        attempt_nonce), so retries of the SAME attempt (e.g. a dropped
        connection retry) are deduped via the idempotency_key unique
        constraint, while a genuinely new call booking the same
        (tenant, item, date, time) after a prior cancellation is NOT blocked
        by it - that case is only blocked if a CONFIRMED row still occupies
        the slot, via uq_no_double_booking.
        """
        idempotency_key = f"{call_id}:{attempt_nonce}"
        client = await self._get_client()

        row = {
            "tenant_id": tenant_id,
            "item_id": item_id,
            "item_name": item_name,
            "date_str": date_str,
            "start_time_mins": start_mins,
            "end_time_mins": end_mins,
            "display_time_str": display_time_str,
            "patient_name": patient_name,
            "patient_phone": patient_phone,
            "idempotency_key": idempotency_key,
            "status": "CONFIRMED",
        }

        try:
            resp = await client.table("appointments").insert(row).execute()
            return {"status": "CONFIRMED", "appointment": (resp.data or [None])[0]}
        except APIError as exc:
            pg_code = getattr(exc, "code", None)
            message = f"{getattr(exc, 'message', '')} {getattr(exc, 'details', '')}".lower()

            if pg_code == _PG_UNIQUE_VIOLATION:
                if "uq_no_double_booking" in message:
                    logger.info(
                        f"Booking rejected (slot already taken): tenant={tenant_id} "
                        f"item={item_name} date={date_str} start={start_mins}"
                    )
                    return {"status": "ALREADY_BOOKED"}
                if "idempotency_key" in message:
                    logger.info(
                        f"Duplicate booking retry ignored: idempotency_key={idempotency_key}"
                    )
                    return {"status": "DUPLICATE_RETRY_IGNORED"}

            logger.error(f"Unexpected Supabase APIError on booking insert: {exc}")
            raise

    async def close(self) -> None:
        """No persistent connection to close for the PostgREST-based client,
        kept for symmetry with other services' lifecycle management."""
        self._client = None
