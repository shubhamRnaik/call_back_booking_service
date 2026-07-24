"""
Step 2 sanity check (temporary, not part of the app): verifies that
SupabaseService actually reaches the live Supabase project and can fetch the
CLINIC_001 tenant + its doctors/services.

Run: .venv\\Scripts\\python.exe test_supabase_connection.py
"""

import asyncio

from indic_tts_runtime.services.supabase_service import SupabaseService


async def main() -> None:
    svc = SupabaseService()

    ok, msg = await svc.check_connectivity()
    print(f"connectivity: ok={ok} msg={msg}")

    result = await svc.get_tenant_and_items("CLINIC_001")
    print("get_tenant_and_items('CLINIC_001') ->")
    print(result)

    await svc.close()


if __name__ == "__main__":
    asyncio.run(main())
