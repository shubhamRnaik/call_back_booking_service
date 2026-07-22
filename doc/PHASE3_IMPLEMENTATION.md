# Phase 3: Full End-to-End Voice Bot Implementation Guide

## 🎯 Overview

You now have a complete, production-grade end-to-end voice bot system that handles:

```
Microphone (16kHz)
        ↓
STT Service (Sarvam Saaras V3)
        ↓
Text Normalization
        ↓
LLM Brain (Gemini 1.5 Flash)
        ↓
Text Chunking
        ↓
Multilingual Normalization
        ↓
TTS Service (Sarvam Bulbul V3)
        ↓
Packet Scheduler (8kHz)
        ↓
Speaker Output
```

## 📋 Architecture Layers

### Layer 1: Speech Recognition (STT)
**Service**: `indic_tts_runtime/services/stt_service.py`

- **Class**: `SarvamSaarasSTTClient`
- **Technology**: WebSocket streaming via Sarvam Saaras V3
- **Input**: 16kHz PCM audio from microphone
- **Output**: Final transcripts with language identification (LID)
- **Key Features**:
  - VAD (Voice Activity Detection) signals
  - Code-mixing support (Hinglish, Tanglish, etc.)
  - Real-time transcript streaming
  - Callback system for speech events

**Usage**:
```python
from indic_tts_runtime.services.stt_service import SarvamSaarasSTTClient

stt = SarvamSaarasSTTClient(api_key="your_sarvam_api_key")
await stt.connect()

async for event in stt.stream_transcripts():
    if event.event_type == "final_transcript":
        print(f"User said: {event.transcript} ({event.language_code})")
```

### Layer 2: Brain (LLM)
**Services**: `indic_tts_runtime/brain/llm_service.py` & `indic_tts_runtime/brain/prompts.py`

- **Class**: `StreamingBrain`
- **Technology**: Gemini 1.5 Flash via google-genai SDK
- **Input**: User transcript
- **Output**: Streamed response tokens
- **Key Features**:
  - Ultra-concise responses (1-2 sentences, 100 tokens max)
  - Conversational openers (Haanji, Acha, Sure, Bilkul)
  - Code-mixing to match user's language
  - Rolling 6-turn context window
  - TTFT (Time To First Token) tracking

**System Prompt Philosophy**:
```
You are an ultra-fast Indian phone agent. 
- Be concise: 1-2 sentences MAX.
- Match the caller's language (Hinglish, Tanglish, Teluglish).
- Start naturally: "Haanji...", "Acha...", "Sure...".
- Always ask a question or suggest next action.
```

**Usage**:
```python
from indic_tts_runtime.brain.llm_service import StreamingBrain

brain = StreamingBrain(api_key="your_gemini_api_key")

async for token in brain.stream_response("Mujhe product pricing batao"):
    print(token, end="", flush=True)
```

### Layer 3: Text Processing Pipeline
**Components**:
- **Chunker** (`indic_tts_runtime/chunker.py`): Groups tokens into TTS-friendly chunks (5-7 words)
- **Normalizer** (`indic_tts_runtime/normalizer.py`): Expands numbers, currency, time (8 Indian languages)

### Layer 4: Text-to-Speech (TTS)
**Service**: `indic_tts_runtime/services/sarvam_service.py`

- **Class**: `SarvamWebSocketClient`
- **Technology**: WebSocket streaming via Sarvam Bulbul V3
- **Input**: Normalized text + language code
- **Output**: 8kHz PCM audio to speaker
- **Key Features**:
  - Multiple language support
  - Speaker profiles (shubh, meera, etc.)
  - Pace control (0.5-2.0x)
  - Instant flush for barge-in

### Layer 5: Audio Scheduling & Output
**Service**: `indic_tts_runtime/core/scheduler.py`

- **Class**: `PacketScheduler`
- **Purpose**: Jitter-free packet delivery (20ms packets @ 8kHz)
- **Key Features**:
  - Consistent packet sizes (320 bytes for 20ms)
  - Barge-in interruption support
  - Buffer management

## 🔧 The Orchestrator

**File**: `indic_tts_runtime/core/full_orchestrator.py`

The `FullVoiceOrchestrator` class ties everything together:

```python
from indic_tts_runtime.core.full_orchestrator import FullVoiceOrchestrator

# Initialize
orchestrator = FullVoiceOrchestrator(
    default_language_code="hi-IN"
)

# Start services
await orchestrator.start()

# Process audio stream
await orchestrator.process_audio_stream(audio_generator)

# Stop
await orchestrator.stop()
```

### Key Features:
1. **Transcript Guard Filter**: Ignores < 3 char transcripts (noise filtering)
2. **Instant Barge-In**: User can interrupt agent response at any time
3. **Real-Time Metrics**:
   - STT Latency: Speech end → Transcript
   - Brain TTFT: Transcript → First Gemini token
   - E2E TTFB: Speech end → First audio to speaker
4. **Parallel Processing**: STT, LLM, TTS run concurrently
5. **Callback System**:
   - `set_status_callback()`: Status updates (LISTENING, THINKING, SPEAKING)
   - `set_transcript_callback()`: Transcript reception
   - `set_response_callback()`: Response generation
   - `set_metrics_callback()`: Real-time metrics

## 🎙️ End-to-End Test

**File**: `test_end_to_end_voice_bot.py`

A complete, runnable terminal script demonstrating the full voice bot:

### AudioIOManager
- Handles PyAudio microphone input (16kHz) and speaker output (8kHz)
- **Software Echo Gate**: Mutes microphone input while agent is speaking (unless volume exceeds barge-in threshold)
- Background threads for concurrent I/O

### VoiceBotUI
Real-time CLI status indicator:
```
[🟢] [LISTENING         ]
[🗣️ ] [USER_SPEAKING    ]
[🧠] [THINKING         ] TTFB: 325ms
[🔊] [SPEAKING         ] TTFB: 280ms
```

### Running the Test

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set API keys in .env
export SARVAM_API_KEY="your_key"
export GEMINI_API_KEY="your_key"

# 3. Run the test
python test_end_to_end_voice_bot.py

# 4. Speak into your microphone
# 5. Listen to the agent respond
# 6. Press Ctrl+C to exit
```

## 🔌 API Key Setup

### Sarvam API Key
1. Get from Sarvam AI dashboard
2. Needed for both STT (Saaras V3) and TTS (Bulbul V3)
3. Example: `sk_4ug4yr7p_0x1CqkoqUMqDKNpuvd6zt0gI`

### Gemini API Key
1. Get from Google AI Studio (https://aistudio.google.com/app/apikeys)
2. Enable Gemini API
3. Example format: `AIzaSy_XXX...`

### .env Configuration
```
SARVAM_API_KEY=your_sarvam_key
GEMINI_API_KEY=your_gemini_key
DEFAULT_LANGUAGE_CODE=hi-IN
STT_SAMPLE_RATE=16000
TTS_SAMPLE_RATE=8000
```

## 📊 Latency Metrics

The system tracks three critical latencies:

### 1. STT Latency
- From speech end → final transcript received
- Typical: 300-800ms (network + processing)

### 2. Brain TTFT (Time To First Token)
- From transcript sent → first Gemini token
- Typical: 200-400ms (LLM latency)

### 3. E2E TTFB (Time To First Byte)
- From speech end → first audio packet to speaker
- Typical: 800-1200ms total
- Goal: < 1000ms for natural conversation

### Full Response Time
- Speech end → User hears agent response
- Typical: 2-4 seconds (depends on response length)

## 🛠️ Troubleshooting

### "Connection refused" to Sarvam API
- Check `SARVAM_API_KEY` is valid
- Verify internet connectivity
- Check firewall allows WebSocket

### "No module named google"
```bash
pip install google-genai
```

### PyAudio errors on Windows
```bash
# Install binary wheel
pip install pipwin
pipwin install pyaudio
```

### "No microphone detected"
- Check microphone is connected
- Verify PyAudio can see it: `python -c "import pyaudio; p = pyaudio.PyAudio(); print(p.get_device_count())"`
- On Linux: install `libasound2-dev` and `portaudio19-dev`

### Gemini API errors
- Check `GEMINI_API_KEY` is correct
- Verify API is enabled in Google Cloud Console
- Check quota limits haven't been exceeded

## 🔄 Interrupt Handling (Barge-In)

When user starts speaking while agent is responding:

1. **Detection**: STT VAD emits `speech_started` event
2. **Flush**: `PacketScheduler.flush()` clears output queue
3. **Cancel**: `TTS.send_flush()` cancels server-side synthesis
4. **Resume**: Brain task cancelled, ready for new input

**Software Echo Gate**:
- Microphone is suppressed while `is_agent_speaking = True`
- Users can still barge-in if they speak loudly (volume > threshold)
- Prevents speaker audio from triggering self-interruption loop

## 🚀 Production Deployment

### Recommended Improvements
1. **Caching**: Cache common phrases (greeting, hours, rates)
2. **Context**: Persist conversation history across sessions
3. **Monitoring**: Log metrics to analytics backend
4. **Fallback**: Handle API outages gracefully
5. **Language Auto-Detect**: Auto-select TTS language based on STT LID

### Performance Optimization
1. **Concurrent Connections**: Reuse STT/TTS WebSockets across requests
2. **Audio Compression**: Use codec option (MP3/OGG) for bandwidth
3. **Token Budgeting**: Limit response length to max 30 seconds
4. **Model Selection**: Explore Gemini 1.5 Pro for complex reasoning

### Security
1. Never commit API keys to git (use `.env`)
2. Rotate Sarvam/Gemini API keys regularly
3. Implement rate limiting per user
4. Log and audit all API calls
5. Sanitize user input before LLM processing

## 📚 Class Reference

### SarvamSaarasSTTClient
```python
# Initialize
stt = SarvamSaarasSTTClient(api_key)

# Lifecycle
await stt.connect()
await stt.send_audio_chunk(bytes)
await stt.signal_end_of_stream()

# Streaming
async for event in stt.stream_transcripts():
    # STTEvent: event_type, transcript, language_code, confidence

# Callbacks
stt.set_speech_started_callback(callback)
stt.set_speech_ended_callback(callback)
stt.set_transcript_callback(callback)

# Cleanup
await stt.disconnect()
```

### StreamingBrain
```python
# Initialize
brain = StreamingBrain(api_key)

# Generate response
async for token in brain.stream_response(user_text):
    # Process token

# Context management
brain.clear_history()
history = brain.get_conversation_history()
stats = brain.get_stats()
```

### FullVoiceOrchestrator
```python
# Initialize
orchestrator = FullVoiceOrchestrator(default_language_code)

# Lifecycle
await orchestrator.start()
await orchestrator.process_audio_stream(audio_generator)
await orchestrator.stop()

# Callbacks
orchestrator.set_status_callback(callback)
orchestrator.set_transcript_callback(callback)
orchestrator.set_response_callback(callback)

# Metrics
metrics = orchestrator.get_metrics()
status = orchestrator.get_status()
```

## 🎓 Further Learning

1. **WebSocket Protocol**: Check RFC 6455
2. **Audio Formats**: Learn about PCM, sampling rates, bit depths
3. **LLM Streaming**: Understanding token streaming vs. full responses
4. **Voice UI**: Best practices for voice interface design
5. **Indian Languages**: Code-mixing patterns and linguistic nuances

---

**Ready to deploy? Start with `test_end_to_end_voice_bot.py` to verify all components work together!**
