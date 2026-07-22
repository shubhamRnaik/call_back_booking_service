# 🎤 Indic Voice Bot - Phase 3 Complete Implementation

## What's Included

A **production-grade end-to-end voice bot** with streaming STT, LLM brain, multilingual TTS, and real-time audio scheduling.

```
┌─────────────────────────────────────────────────────────────┐
│                    VOICE BOT PIPELINE                       │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  Microphone (16kHz)                                          │
│      ↓                                                       │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ STT: Sarvam Saaras V3 Streaming (WebSocket)         │  │
│  │ • Input: 16-bit PCM at 16kHz                         │  │
│  │ • Output: Transcript + Language ID                   │  │
│  │ • Features: VAD, Code-mixing, Real-time             │  │
│  └──────────────────────────────────────────────────────┘  │
│      ↓                                                       │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ BRAIN: Gemini 1.5 Flash Streaming                   │  │
│  │ • System Prompt: Ultra-concise, code-mixing          │  │
│  │ • Max 100 tokens (30 sec speech)                     │  │
│  │ • Context window: 6 conversation turns              │  │
│  │ • TTFT tracking: Latency metrics                    │  │
│  └──────────────────────────────────────────────────────┘  │
│      ↓                                                       │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ PIPELINE: Chunker → Normalizer (8 languages)        │  │
│  │ • Intelligent text chunking (5-7 words/chunk)        │  │
│  │ • Number/currency/time expansion                     │  │
│  └──────────────────────────────────────────────────────┘  │
│      ↓                                                       │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ TTS: Sarvam Bulbul V3 Streaming (WebSocket)         │  │
│  │ • Input: Normalized text + language code             │  │
│  │ • Output: 8-bit PCM at 8kHz                          │  │
│  │ • Features: Multiple speakers, pace control          │  │
│  └──────────────────────────────────────────────────────┘  │
│      ↓                                                       │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ SCHEDULER: Jitter-free packet delivery (20ms)        │  │
│  │ • Buffer management                                  │  │
│  │ • Barge-in interruption support                      │  │
│  └──────────────────────────────────────────────────────┘  │
│      ↓                                                       │
│  Speaker (8kHz)                                             │
│                                                               │
└─────────────────────────────────────────────────────────────┘

INSTANT BARGE-IN: User can interrupt at ANY time
SOFTWARE ECHO GATE: Prevents speaker bleed into mic
REAL-TIME METRICS: STT latency, Brain TTFT, E2E TTFB
```

## Quick Start

### 1. Install Dependencies
```bash
# Install Python packages
pip install -r requirements.txt

# On Windows (for PyAudio):
# Download binary from: https://github.com/intxaurtza/pyaudio-wheels/releases
# pip install pyaudio-0.2.13-cp311-cp311-win_amd64.whl

# On Linux:
sudo apt-get install libasound2-dev portaudio19-dev

# On macOS:
brew install portaudio
```

### 2. Configure API Keys
```bash
# Edit .env file
cat > .env << 'EOF'
SARVAM_API_KEY=your_sarvam_api_key_here
GEMINI_API_KEY=your_gemini_api_key_here
DEFAULT_LANGUAGE_CODE=hi-IN
STT_SAMPLE_RATE=16000
TTS_SAMPLE_RATE=8000
EOF
```

Get API keys from:
- **Sarvam API**: https://sarvam.ai/ (STT + TTS)
- **Gemini API**: https://aistudio.google.com/app/apikeys (LLM)

### 3. Run the Voice Bot
```bash
python test_end_to_end_voice_bot.py
```

Output:
```
================================================================================
🎤 INDIC VOICE BOT - End-to-End Test
================================================================================
[INFO] Voice I/O Manager initialized (Input: 16000Hz, Output: 8000Hz)
[INFO] Full Voice Orchestrator initialized
[INFO] Initializing Voice Orchestrator...
[INFO] ✓ Connected to Sarvam STT service
[INFO] ✓ Connected to Sarvam AI TTS Streaming
[INFO] ✓ Voice Orchestrator started successfully

✨ Voice bot ready! Speak into your microphone...
Press Ctrl+C to exit

[🟢] [READY                ]
```

### 4. Test Interaction
```
🟢 READY
You: "Namaste, mujhe aapke hindi courses ki information chahiye"
🗣️  USER_SPEAKING
🧠 THINKING                          TTFB: 325ms
📝 [STT] Namaste, mujhe aapke hindi courses ki information chahiye (hi-IN)
🤖 [BRAIN] Haanji, hum aapko bahut acha hindi course offer karte hain! 
🔊 SPEAKING                          TTFB: 280ms
[Speaker plays response in Hindi]
🟢 LISTENING
```

## Architecture

### Files & Components

```
indic_tts_runtime/
├── services/
│   ├── stt_service.py              # Layer 1: Sarvam STT (16kHz streaming)
│   ├── cache_service.py            # Caching layer
│   └── sarvam_service.py           # TTS service (8kHz streaming)
├── brain/
│   ├── prompts.py                  # Layer 2: System prompts (concise, code-mix)
│   └── llm_service.py              # Gemini 1.5 Flash streaming
├── core/
│   ├── full_orchestrator.py        # MASTER: Orchestrates all layers
│   ├── router.py                   # FastAPI routes
│   └── scheduler.py                # Layer 5: Jitter-free audio scheduling
├── chunker.py                      # Layer 3: Text chunking (5-7 words)
├── normalizer.py                   # Layer 4: Text normalization (8 languages)
├── config.py                       # Settings & validation
└── main.py                         # FastAPI app

test_end_to_end_voice_bot.py        # Unified live test with PyAudio + UI

doc/
├── PHASE3_IMPLEMENTATION.md        # Detailed technical guide
├── PROJECT_SUMMARY.md              # Project overview
├── QUICK_REFERENCE.md              # API reference
└── STRUCTURE.txt                   # File structure
```

## Key Features

### 🎯 Instant Barge-In
User can interrupt agent response at ANY point:
1. STT detects user speech (`speech_started` VAD)
2. Orchestrator cancels LLM token generation
3. Scheduler flushes output buffer
4. TTS receives `send_flush()` command
5. Agent stops immediately, ready for new input

### 🔇 Software Echo Gate
Prevents speaker audio from triggering STT:
- Microphone suppressed while agent is speaking
- Users can still barge-in if they speak loudly (volume > threshold)
- No self-interruption loops

### 📊 Real-Time Metrics
```
STT Latency    = Speech End → Transcript (typically 300-800ms)
Brain TTFT     = Transcript → First Gemini token (typically 200-400ms)
E2E TTFB       = Speech End → First audio to speaker (target: < 1000ms)
```

### 🌍 Multilingual Support
- **STT**: Detects language automatically (Hindi, Tamil, Telugu, etc.)
- **Brain**: Code-mixes to match user pattern
- **TTS**: Normalizes and speaks in detected language
- **Normalizer**: Expands numbers/currency/time in 8 Indian languages

### 🚀 Streaming Architecture
All layers stream data for **low latency**:
- STT yields transcripts as they complete
- LLM yields tokens as they arrive
- Chunker streams chunks as punctuation appears
- TTS streams audio packets as synthesis completes
- No waiting for full responses

### 🔄 Concurrent Processing
- STT input stream runs in background
- LLM tokens processed as they arrive
- TTS synthesis happens during chunking
- Scheduler regulates delivery to speaker
- **No blocking operations**

## Performance Targets

| Metric | Target | Typical |
|--------|--------|---------|
| STT Latency | < 800ms | 300-500ms |
| Brain TTFT | < 400ms | 250-350ms |
| E2E TTFB | < 1000ms | 800-1200ms |
| Response Length | 30 seconds max | 15-20 seconds |
| Interruption Latency | < 200ms | 100-150ms |

## API Reference

### SarvamSaarasSTTClient
```python
stt = SarvamSaarasSTTClient(api_key)
await stt.connect()
await stt.send_audio_chunk(audio_bytes)
async for event in stt.stream_transcripts():
    print(event.transcript, event.language_code)
```

### StreamingBrain
```python
brain = StreamingBrain(api_key)
async for token in brain.stream_response("User input"):
    print(token, end="", flush=True)
```

### FullVoiceOrchestrator
```python
orch = FullVoiceOrchestrator()
await orch.start()
orch.set_status_callback(lambda s: print(f"Status: {s}"))
await orch.process_audio_stream(audio_stream)
```

## Environment Variables

```env
# API Keys
SARVAM_API_KEY=sk_XXXXXXX...
GEMINI_API_KEY=AIzaSy_XXXXXXX...

# Audio Configuration
STT_SAMPLE_RATE=16000              # Microphone input
TTS_SAMPLE_RATE=8000               # Speaker output
DEFAULT_LANGUAGE_CODE=hi-IN        # Default language
DEFAULT_SAMPLE_RATE=8000           # Audio pipeline default
PACKET_DURATION_MS=20              # Scheduler packet size

# Server Configuration
SERVER_HOST=0.0.0.0
SERVER_PORT=8000
LOG_LEVEL=INFO

# Performance
TARGET_TTFB_MS=220.0               # Target end-to-end latency
ENABLE_METRICS=true                # Enable metrics tracking
CACHE_ENABLED=true                 # Enable caching
```

## Troubleshooting

### "KeyError: 'GEMINI_API_KEY'"
- Ensure `.env` file exists with `GEMINI_API_KEY`
- Run `python -c "from indic_tts_runtime.config import settings; print(settings.gemini_api_key)"`

### "Failed to connect to Sarvam STT"
- Check `SARVAM_API_KEY` is valid
- Verify internet connectivity
- Test: `curl -I wss://api.sarvam.ai/speech-to-text/ws`

### PyAudio not found on Windows
```bash
pip install pipwin
pipwin install pyaudio
```

### Low latency issues
1. **Reduce context window**: Fewer conversation turns → faster LLM
2. **Shorter system prompt**: Less tokens → faster processing
3. **Increase packet size**: Trade latency for jitter (scheduler)
4. **Local LLM**: Replace Gemini with local Ollama/LLaMA for instant TTFT

### Microphone not detected
```python
import pyaudio
p = pyaudio.PyAudio()
for i in range(p.get_device_count()):
    info = p.get_device_info_by_index(i)
    print(f"{i}: {info['name']}")
```

## Advanced Usage

### Custom System Prompt
```python
from indic_tts_runtime.brain import prompts

# Edit prompts.py
prompts.SYSTEM_PROMPT = """Your custom prompt here"""

# Or override at runtime:
brain = StreamingBrain()
# Modify before calling stream_response()
```

### Language-Specific Configuration
```python
orchestrator = FullVoiceOrchestrator(
    default_language_code="ta-IN"  # Tamil instead of Hindi
)
```

### Multiple Concurrent Sessions
```python
# Each orchestrator instance is independent
bot1 = FullVoiceOrchestrator(default_language_code="hi-IN")
bot2 = FullVoiceOrchestrator(default_language_code="ta-IN")

await asyncio.gather(
    bot1.process_audio_stream(stream1),
    bot2.process_audio_stream(stream2)
)
```

### Caching Responses
```python
from indic_tts_runtime.services.cache_service import CacheService

cache = CacheService()
cached_response = await cache.get(user_text)
if cached_response:
    # Use cached TTS audio
else:
    # Generate new response
    response = await brain.stream_response(user_text)
    await cache.set(user_text, response)
```

## Deployment

### Docker
```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
EXPOSE 8000

CMD ["python", "-m", "uvicorn", "indic_tts_runtime.main:app", "--host", "0.0.0.0"]
```

### Production Checklist
- [ ] API keys stored in secure vault (not .env)
- [ ] Metrics logged to monitoring system
- [ ] Error handling for all external API calls
- [ ] Rate limiting per user
- [ ] Audio logging (with consent)
- [ ] Fallback responses for API failures
- [ ] Regular API key rotation
- [ ] Load testing completed

## Contributing

1. Create a feature branch
2. Implement changes
3. Test with `test_end_to_end_voice_bot.py`
4. Submit PR with metrics

## Support

- **Documentation**: See `doc/PHASE3_IMPLEMENTATION.md`
- **Issues**: Check Sarvam/Gemini API documentation
- **Examples**: See `test_end_to_end_voice_bot.py`

## License

Proprietary - Indic Voice Bot Project

---

**🚀 Ready to deploy? Run `python test_end_to_end_voice_bot.py` to get started!**
