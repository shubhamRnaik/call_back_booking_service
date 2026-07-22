# Latency and Reliability Hardening TODO

This checklist tracks production-safe improvements for the live voice pipeline.
Each step should be implemented and validated independently.

## Completed

- [x] Step 1: Fix undefined service reference in REST stream endpoint.
  - File: indic_tts_runtime/main.py
  - Change: replaced `sarvam_service` check with `sarvam_ws_client`.
  - Why: prevents runtime NameError in /api/v1/stream-voice.

- [x] Step 1: Remove duplicate root route conflict.
  - File: indic_tts_runtime/main.py
  - Change: moved API info endpoint from `/` to `/api` and renamed handler to `api_info`.
  - Why: avoids ambiguous route behavior and protects `/` UI redirect.

## In Progress Queue

- [x] Step 2: Upgrade LLM request format to structured message arrays.
  - File: indic_tts_runtime/brain/llm_service.py
  - Scope:
    - Replace plain concatenated conversation text with role-based arrays.
    - Keep current provider fallback behavior intact.
    - Preserve existing error handling and Hindi fallbacks.
  - Expected impact: better provider-side prompt caching and lower TTFT variance.

- [x] Step 3: Tighten LLM generation bounds for voice calls.
  - File: indic_tts_runtime/brain/llm_service.py
  - Scope:
    - Reduce context window from 6 turns to 4 turns.
    - Reduce max output tokens from 100 to 50.
    - Keep temperature conservative for concise responses.
  - Expected impact: lower LLM tail latency and faster call closure.

- [x] Step 4: Add speculative clause streaming in active /ws/v1/voice-call path.
  - File: indic_tts_runtime/main.py
  - Scope:
    - Start TTS as soon as chunker emits first clause or threshold chunk.
    - Keep utterance_id stale-guard and barge-in cancellation behavior.
    - Flush once per response after last chunk, not per token.
  - Expected impact: reduce perceived response delay by overlapping LLM and TTS.

- [x] Step 5: Add explicit latency metrics for overlap pipeline.
  - File: indic_tts_runtime/main.py
  - Scope:
    - Track llm_first_chunk_ms, tts_first_audio_ms, and overlap savings.
    - Emit metrics to websocket client for traceability.
  - Progress: llm_first_chunk_ms metric is already emitted.
  - Expected impact: measurable proof of latency improvements.

- [x] Step 6: Improve protocol safety around duplicate audio loops.
  - File: indic_tts_runtime/services/sarvam_service.py
  - Scope:
    - Keep existing repeating-pattern guard.
    - Add optional hard timeout per utterance stream.
  - Expected impact: prevent long-running loops in edge cases.

- [x] Step 7: Prompt specialization for Beauty Parlour assistant.
  - File: indic_tts_runtime/brain/prompts.py
  - Scope:
    - Add domain prompt and short prompt variant.
    - Keep multilingual behavior deterministic for Kannada/Hindi rules.
  - Expected impact: better business-task completion and fewer off-topic turns.

## Validation Checklist (run after each step)

- [x] python -m py_compile indic_tts_runtime/main.py indic_tts_runtime/brain/llm_service.py indic_tts_runtime/chunker.py indic_tts_runtime/services/sarvam_service.py
- [ ] Smoke test /api/v1/health endpoint
- [ ] Voice-call websocket manual test with barge-in
- [ ] Compare before vs after metrics for TTFT and first audio latency

Note: validation tests intentionally deferred until implementation completion.

## Production Priority Hardening

- [x] Priority 0: API/WS authentication controls.
  - Files: indic_tts_runtime/config.py, indic_tts_runtime/main.py
  - Change: added configurable API key enforcement for REST and websocket flows.
  - Notes: enabled only when `SECURITY_ENABLED=true`; probes remain unauthenticated.

- [x] Priority 0: Basic rate limits and websocket concurrency caps.
  - Files: indic_tts_runtime/config.py, indic_tts_runtime/main.py
  - Change: per-IP REST/WS connect rate limits and global/per-IP WS active connection limits.

- [x] Priority 0: Health split into liveness + readiness.
  - File: indic_tts_runtime/main.py
  - Change: added `/api/v1/live` and `/api/v1/ready`; `/api/v1/health` now readiness alias.

- [x] Priority 1: Retry policy with jitter for unstable external calls.
  - File: indic_tts_runtime/main.py
  - Change: added shared async retry helper and used it in STT/TTS connection/text/flush paths.

- [x] Priority 1: Operational metrics enrichment.
  - File: indic_tts_runtime/main.py
  - Change: added auth/rate-limit/retry/rejection counters and latency percentile snapshots.

- [x] Priority 1: Startup/runtime guards for websocket handlers.
  - File: indic_tts_runtime/main.py
  - Change: explicit service initialization checks and WS session duration/utterance limits.

- [x] Priority 2: Hot-path cleanup and endpoint metadata updates.
  - File: indic_tts_runtime/main.py
  - Change: removed redundant imports and updated advertised API endpoints.
