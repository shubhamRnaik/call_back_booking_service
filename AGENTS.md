# Indic TTS Runtime Engine - AI Agent Guidelines

This workspace contains a production-grade, ultra-low-latency (<220ms TTFB) streaming voice assistant for Indian languages. It integrates streaming STT (Sarvam), LLM brain (Gemini 2.0 / OpenAI GPT-4o-mini), and streaming TTS (Sarvam Bulbul V3) over WebSockets and telephony interfaces.

## Quick Reference & Commands

- **Environment Setup**: Python 3.10+ in `.venv/`.
  ```powershell
  pip install -r requirements.txt
  ```
- **Run Main Application Server**:
  ```powershell
  python -m indic_tts_runtime.main
  # Or with Uvicorn reload:
  uvicorn indic_tts_runtime.main:app --host 0.0.0.0 --port 8000 --reload
  ```
- **Run Tests**:
  ```powershell
  pytest
  python test_pipeline.py
  python test_e2e_live.py
  python tests/diagnose_sarvam_api.py
  ```

## Codebase Architecture & Boundaries

- [indic_tts_runtime/main.py](indic_tts_runtime/main.py): FastAPI WebSocket orchestrator handling `/ws/v1/voice-call` and `/ws/v1/stream-voice`.
- [indic_tts_runtime/brain/llm_service.py](indic_tts_runtime/brain/llm_service.py): Streaming LLM response generator with Gemini fallback to OpenAI.
- [indic_tts_runtime/brain/prompts.py](indic_tts_runtime/brain/prompts.py): Concise, single-sentence response system prompts.
- [indic_tts_runtime/services/sarvam_service.py](indic_tts_runtime/services/sarvam_service.py): Sarvam Bulbul V3 WebSocket streaming TTS client.
- [indic_tts_runtime/services/stt_service.py](indic_tts_runtime/services/stt_service.py): Streaming Speech-to-Text service.
- [indic_tts_runtime/services/cache_service.py](indic_tts_runtime/services/cache_service.py): Static phrase audio cache.
- [indic_tts_runtime/chunker.py](indic_tts_runtime/chunker.py): Token stream accumulator for clause-bounded text chunks.
- [indic_tts_runtime/core/router.py](indic_tts_runtime/core/router.py): Cache vs live synthesis router.
- [indic_tts_runtime/core/scheduler.py](indic_tts_runtime/core/scheduler.py): Packet timing regulator (40ms chunks).

## Critical Project Conventions & Protocols

1. **Audio Sample Rates**:
   - **TTS Output**: Always `22050 Hz` (Linear16 PCM / WAV). Defined in [indic_tts_runtime/config.py](indic_tts_runtime/config.py).
   - **STT Input**: `16000 Hz` (PCM 16-bit mono).
   - **Telephony**: Downsampled to `8000 Hz` for Exotel gateway integration.
   - *Warning*: Do not mix up 8kHz telephony transport with 22.05kHz Sarvam TTS output; setting clients to 8kHz playback causes 2.75x slowdown and pitch degradation. See [doc/SAMPLE_RATE_FIX_SUMMARY.md](doc/SAMPLE_RATE_FIX_SUMMARY.md).

2. **Single-Chunk Synthesis Protocol**:
   - Accumulate LLM output tokens fully per turn before sending to Sarvam TTS via a single `send_text_chunk()` + `send_flush()`.
   - Never stream multiple partial chunks over a single Sarvam WebSocket session, as Sarvam's buffer accumulator re-synthesizes prior text chunks causing audio repetition loops. See [/memories/repo/sarvam_tts_loop_bug.md](/memories/repo/sarvam_tts_loop_bug.md) and [/memories/repo/single_chunk_synthesis.md](/memories/repo/single_chunk_synthesis.md).

3. **Per-Utterance Connection Strategy**:
   - Establish a fresh `SarvamWebSocketClient` in parallel with LLM token generation to eliminate connection latency while preventing socket state pollution. See [/memories/repo/per_utterance_tts_connection.md](/memories/repo/per_utterance_tts_connection.md).

4. **Call Termination Flow (`[END_CALL]`)**:
   - LLM appends `[END_CALL]` to signal intent to hang up.
   - [indic_tts_runtime/main.py](indic_tts_runtime/main.py) strips `[END_CALL]`, synthesizes and streams remaining audio, waits `1.2s` for client playback drain, sends a JSON `hangup` message, and closes the WebSocket. See [/memories/repo/automated_call_closure.md](/memories/repo/automated_call_closure.md).

5. **Half-Duplex Audio Guard**:
   - Suppress incoming audio streams from client microphone while TTS audio is playing (`is_speaking = True`) to avoid echo loops.

## Common Pitfalls & Troubleshooting

- **Sarvam 403 Forbidden**: Check API key permissions and streaming scope. See [/memories/repo/sarvam_api_issue.md](/memories/repo/sarvam_api_issue.md) and [tests/diagnose_sarvam_api.py](tests/diagnose_sarvam_api.py).
- **8kHz vs 22.05kHz Playback**: Ensure client decoder matches 22050 Hz PCM output. See [doc/SAMPLE_RATE_FIX_SUMMARY.md](doc/SAMPLE_RATE_FIX_SUMMARY.md).

## Detailed Documentation Links

- Overview: [README.md](README.md) & [doc/PROJECT_SUMMARY.md](doc/PROJECT_SUMMARY.md)
- Implementation Deep-Dive: [doc/IMPLEMENTATION_GUIDE.md](doc/IMPLEMENTATION_GUIDE.md)
- Quick Reference: [doc/QUICK_REFERENCE.md](doc/QUICK_REFERENCE.md)
