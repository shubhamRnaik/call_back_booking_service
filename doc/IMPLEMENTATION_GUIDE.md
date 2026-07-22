# Implementation Guide: Indic TTS Runtime Engine

> Complete technical reference for the production-grade Text-to-Speech engine architecture, components, and operational details.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Component Deep Dive](#component-deep-dive)
3. [Data Flow & Request Lifecycle](#data-flow--request-lifecycle)
4. [Performance Optimization](#performance-optimization)
5. [Deployment & Scaling](#deployment--scaling)
6. [Troubleshooting & Monitoring](#troubleshooting--monitoring)

---

## Architecture Overview

### Multi-Layer Design (Layers 3 & 4)

```
Layer 4: Audio Streaming
├─ Binary WAV/PCM delivery
├─ Streaming response chunking
└─ Real-time packet delivery

Layer 3: Voice Orchestration & Routing
├─ Multi-engine routing logic
├─ Cache management
├─ Fallback handling
└─ Request orchestration

Layer 2: Core TTS Engines
├─ Sarvam Bulbul V3 (primary)
├─ Fallback audio generation
└─ Connection management

Layer 1: External Integrations
└─ Sarvam AI HTTP API
```

### Component Responsibilities (SRP)

| Component | Responsibility | File |
|-----------|-----------------|------|
| **CacheService** | Fast static phrase lookup | `services/cache_service.py` |
| **SarvamService** | Primary TTS synthesis | `services/sarvam_service.py` |
| **VoiceRouter** | Request routing orchestration | `core/router.py` |
| **PacketScheduler** | Audio stream regulation | `core/scheduler.py` |
| **FastAPI App** | HTTP API & orchestration | `main.py` |
| **Config** | Environment validation | `config.py` |
| **Schemas** | Request/response validation | `schemas.py` |

---

## Component Deep Dive

### 1. CacheService (`services/cache_service.py`)

#### Purpose
Fast O(1) lookup for pre-rendered static phrases. Eliminates network latency for common responses.

#### Implementation Details

**Phrase Dictionary:**
```python
self._phrase_map: dict[str, str] = {
    "haanji": "haanji.wav",           # "yes"
    "namaste": "namaste.wav",         # greeting
    "theek hai": "theek_hai.wav",     # "ok"
    # ... more phrases
}
```

**Cache File Format:**
- **Codec:** WAV (wave module compatible)
- **Sample Rate:** 8000 Hz (telephony standard)
- **Channels:** 1 (mono)
- **Bit Depth:** 16-bit signed PCM
- **Duration:** 1-3 seconds per phrase

**Lookup Flow:**
```
Input Text
    ↓
Normalize (strip, lowercase)
    ↓
Check in _phrase_map
    ├─ Hit: Load WAV from disk
    │   ├─ Read file
    │   ├─ Create BytesIO stream
    │   └─ Extract metadata
    └─ Miss: Return None
```

**Performance Characteristics:**
- **Lookup Time:** O(1) dictionary lookup
- **Read Time:** ~20-30ms (disk I/O)
- **Memory Impact:** ~1-2 MB per 100 phrases (cached WAV files)
- **Hit Rate Target:** 30-40% (depends on phrase set)

#### API

```python
# Lookup cached phrase
result = cache_service.lookup_phrase("haanji")
if result:
    audio_stream, metadata = result
    # Use audio_stream (BytesIO)

# Register new phrase (for dynamic caching)
cache_service.register_phrase("custom_text", audio_bytes)

# Get statistics
stats = cache_service.get_cache_stats()
# Returns: {cache_enabled, cache_dir, total_phrases, cached_files, total_size_bytes}
```

---

### 2. SarvamService (`services/sarvam_service.py`)

#### Purpose
Production-grade async client for Sarvam AI's Bulbul V3 TTS engine. Manages persistent connections and streaming synthesis.

#### Connection Management

**Persistent Session:**
```python
# Single aiohttp.ClientSession per service instance
self._session = aiohttp.ClientSession(
    connector=aiohttp.TCPConnector(
        limit=10,              # Total connections
        limit_per_host=5,      # Per-host limit
        ttl_dns_cache=300,     # DNS cache TTL
        ssl=None               # Default SSL
    ),
    timeout=aiohttp.ClientTimeout(total=30)
)
```

**Benefits:**
- TCP connection reuse
- Persistent keep-alive
- DNS caching
- Connection pooling
- Reduced handshake latency

**Session Lifecycle:**
- Created at app startup
- Reused for all requests
- Auto-reinitialize after 24 hours
- Explicitly closed at shutdown

#### API Payload Construction

**Request Payload:**
```python
{
    "text": "Aapka checkup fee teen sau rupaye hai",
    "language": "hi-IN",
    "speaker": "shubh",
    "pace": 0.95,
    "audio_format": "wav",
    "sample_rate": 8000,
    "codec": "linear16"  # 16-bit PCM
}
```

**Authorization:**
```
Header: Authorization: Bearer {SARVAM_API_KEY}
```

#### Streaming Implementation

**Streaming Method:**
```python
async def synthesize_stream(text, language, speaker, pace):
    """Yields audio chunks as they arrive."""
    async with session.post(...) as response:
        async for chunk in response.content.iter_chunked(8192):
            yield chunk  # Stream chunks to caller
```

**Advantages:**
- Real-time audio delivery
- Memory efficient (no buffering entire response)
- Progressive playback
- Reduced TTFB impact

#### Error Handling

**Implemented Strategies:**
1. **Status Code Checking** (200 OK vs others)
2. **Timeout Management** (asyncio.TimeoutError)
3. **Connection Retries** (session health checks)
4. **Explicit Error Logging** (detailed error messages)

```python
try:
    async for chunk in self.synthesize_stream(...):
        yield chunk
except asyncio.TimeoutError:
    raise ConnectionError("Sarvam API request timed out")
except Exception as e:
    raise ConnectionError(f"Failed to stream: {str(e)}")
```

---

### 3. VoiceRouter (`core/router.py`)

#### Purpose
Intelligent routing orchestrator that decides which engine synthesizes each request.

#### Routing Strategy

**Cache-First Strategy:**
```
Input Request
    ↓
Cache Lookup
    ├─ Hit (match found)
    │   └─ Return cached audio
    └─ Miss (no match)
        ↓
Sarvam Synthesis
    ├─ Success
    │   └─ Return synthesized audio
    └─ Failure
        ↓
Fallback Audio
    └─ Return silence (3 seconds)
```

#### Request Tracing

**Routing Log Structure:**
```python
routing_log = {
    "text": text[:50],
    "language": "hi-IN",
    "speaker": "shubh",
    "attempted_sources": ["cache", "cache_miss", "sarvam"],
    "success": True,
    "final_source": "sarvam"
}
```

**Example Log Flow:**
```
Text: "Aapka checkup fee teen sau rupaye hai"
├─ Attempted: cache
├─ Result: cache_miss (not in dictionary)
├─ Attempted: sarvam
├─ Result: sarvam (200 OK, 142ms)
└─ Final Source: sarvam ✓
```

#### Fallback Audio Generation

**Fallback Characteristics:**
```python
# 3-second silence in WAV format
duration_seconds = 3
sample_rate = 8000
num_samples = sample_rate * duration_seconds  # 24,000 samples
audio_data = struct.pack('<h', 0) * num_samples  # Zero amplitude

# Creates valid WAV file with:
# - 1 channel (mono)
# - 16-bit samples
# - 8000 Hz sample rate
```

**Fallback vs Error:**
- **Fallback Used:** When primary engine unreachable
- **Error Raised:** When both primary and fallback fail
- **Logging:** Explicit warning that fallback was triggered

---

### 4. PacketScheduler (`core/scheduler.py`)

#### Purpose
Eliminates network jitter by regulating audio stream into consistent-sized time-based packets.

#### Packet Calculation

**Formula:**
```
packet_size_bytes = (sample_rate × channels × bytes_per_sample × duration_ms) / 1000

For default config (8kHz mono, 16-bit, 40ms):
= (8000 × 1 × 2 × 40) / 1000
= 640 bytes per packet
```

**Timing:**
- **40ms packets** at 8kHz = 320 samples per packet
- **Total latency added:** ~40ms max (one packet buffer)
- **Throughput:** Constant 200 kbps (8000 × 1 × 2 × 8 bits)

#### Buffer Management

**Ring Buffer Implementation:**
```python
self._buffer: deque[bytes] = deque()  # FIFO queue of incoming chunks
self._buffer_size: int = 0            # Cumulative byte count

# Accumulate chunks
while incoming_size < packet_size:
    buffer.append(chunk)
    buffer_size += len(chunk)

# Extract packet when sufficient data
packet = buffer.extract(packet_size_bytes)
```

**Example:**
```
Incoming Chunks:  [100B], [150B], [200B], [300B]
                   ↓
Buffer Accumulation
                   ↓
When buffer ≥ 640B: Extract 640B packet
                   ↓
Output: [640B packet]
Remaining in buffer: [210B]
```

#### Streaming with Regulation

**Async Generator Pattern:**
```python
async def schedule_stream(incoming_stream):
    async for chunk in incoming_stream:
        buffer.append(chunk)
        
        # Emit complete packets
        while buffer_size >= packet_size:
            packet = extract_packet()
            yield packet  # Emit regulated packet
    
    # Flush remainder
    if buffer_size > 0:
        yield flush_buffer()
```

#### Statistics Tracking

**Collected Metrics:**
- `packet_duration_ms`: 40
- `packet_size_bytes`: 640
- `packets_emitted`: 142
- `total_bytes_processed`: 91,520
- `buffer_size_bytes`: 0 (at rest)
- `elapsed_seconds`: 2.34
- `throughput_mbps`: 0.31

---

### 5. FastAPI Orchestrator (`main.py`)

#### Application Lifecycle

**Startup Phase:**
```python
@asynccontextmanager
async def lifespan(app):
    # Startup
    cache_service = CacheService()                  # ~5ms
    sarvam_service = SarvamService()
    await sarvam_service.initialize()               # ~50ms
    voice_router = VoiceRouter(...)                 # ~1ms
    packet_scheduler = PacketScheduler()            # ~1ms
    
    yield  # App runs here
    
    # Shutdown
    await sarvam_service.close()                    # ~10ms
```

**Total Startup Time:** ~70ms

#### Request Lifecycle

**Request Processing Flow:**

```
POST /api/v1/stream-voice
    │
    ├─ 1. Parse & Validate (schemas.TTSRequest)
    │     └─ Pydantic validation (~1ms)
    │
    ├─ 2. Generate Request ID (uuid4)
    │     └─ Unique tracking (~0.1ms)
    │
    ├─ 3. Record Start Time (perf_counter)
    │     └─ Monotonic timer (~0.01ms)
    │
    ├─ 4. Route to VoiceRouter
    │     └─ Cache lookup (if hit: ~30ms, if miss: ~1ms)
    │     └─ Sarvam synthesis (if needed: ~150ms)
    │     └─ Result: (audio_stream, metadata)
    │
    ├─ 5. Measure TTFB (perf_counter delta)
    │     └─ TTFB = elapsed_ms (~30-200ms)
    │
    ├─ 6. Update Metrics
    │     └─ Increment counters (~0.1ms)
    │
    ├─ 7. Initialize PacketScheduler
    │     └─ Create buffer & state (~1ms)
    │
    ├─ 8. Create Streaming Generator
    │     └─ Async generator (~0.5ms)
    │
    └─ 9. Return StreamingResponse
          ├─ HTTP 200 OK
          ├─ Headers (X-Request-ID, X-TTFB-Ms, etc.)
          └─ Body: streaming audio bytes
```

#### TTFB Measurement Methodology

**Monotonic Timer Used:**
```python
import time

# At request start (after validation)
ttfb_start = time.perf_counter()  # Monotonic, not affected by system clock

# After first audio byte ready
ttfb_end = time.perf_counter()

ttfb_ms = (ttfb_end - ttfb_start) * 1000  # Convert to milliseconds
```

**Measured From:**
- Request parsing complete
- Through Voice Router (cache or Sarvam)
- Through Packet Scheduler initialization
- To first yielded audio byte

**NOT Measured:**
- Network transmission latency
- Client-side processing
- System clock adjustments

**Typical Values:**
- Cache hit: 25-45ms
- Sarvam synthesis: 150-220ms
- Fallback generation: 5-15ms

#### Endpoints

**1. POST `/api/v1/stream-voice`**

**Request:**
```json
{
  "text": "Aapka checkup fee teen sau rupaye hai",
  "target_language_code": "hi-IN",
  "speaker": "shubh",
  "pace": 0.95
}
```

**Response:**
- Status: 200 OK
- Content-Type: audio/wav
- Body: Binary WAV stream

**Response Headers:**
```
X-Request-ID: req_a1b2c3d4e5
X-TTFB-Ms: 142.34
X-Audio-Source: sarvam
Content-Disposition: attachment; filename="voice_req_a1b2c3d4e5.wav"
```

**2. GET `/api/v1/health`**

**Response:**
```json
{
  "status": "healthy",
  "cache_available": true,
  "sarvam_api_reachable": true,
  "uptime_seconds": 3456.78
}
```

**Status Values:**
- `healthy`: All components operational
- `degraded`: Some components unavailable
- `unhealthy`: Critical failure

**3. GET `/api/v1/metrics`**

**Response (Abbreviated):**
```json
{
  "uptime_seconds": 3456.78,
  "total_requests": 142,
  "successful_requests": 139,
  "failed_requests": 3,
  "success_rate_percent": 97.88,
  "average_ttfb_ms": 118.45,
  "target_ttfb_ms": 220,
  "ttfb_slo_met": true,
  "cache_hits": 54,
  "sarvam_hits": 85
}
```

---

## Data Flow & Request Lifecycle

### Complete Request Trace

**Example: "Aapka checkup fee teen sau rupaye hai"**

```
T+0ms:    Request received
          POST /api/v1/stream-voice
          Body: {text: "Aapka checkup fee...", language: "hi-IN", ...}
          
T+1ms:    Pydantic validation
          ✓ Text length OK
          ✓ Language code valid
          ✓ Speaker exists
          ✓ Pace in range [0.5-2.0]
          
T+2ms:    Request ID generated: req_abc123def456
          Start TTFB measurement
          
T+3ms:    Route via VoiceRouter
          ├─ Cache lookup for "aapka checkup fee teen sau rupaye hai"
          └─ Result: MISS (not in cache)
          
T+4ms:    Sarvam service selected
          Build payload:
          {
            text: "Aapka checkup fee teen sau rupaye hai",
            language: "hi-IN",
            speaker: "shubh",
            pace: 0.95,
            sample_rate: 8000,
            codec: "linear16"
          }
          
T+5ms:    HTTP POST to Sarvam API
          POST https://api.sarvam.ai/api/v1/text-to-speech
          Authorization: Bearer <API_KEY>
          
T+155ms:  Sarvam API responds with first chunk
          Status: 200 OK
          First audio chunk: 8192 bytes
          
T+156ms:  TTFB captured: 156ms
          ✓ Under 220ms target
          Update metrics:
          - total_requests++
          - successful_requests++
          - total_ttfb_ms += 156
          - sarvam_hits++
          
T+157ms:  Packet Scheduler initialized
          packet_size = 640 bytes
          packet_duration = 40ms
          
T+158ms:  Streaming response returned
          HTTP 200 OK
          Headers:
            X-Request-ID: req_abc123def456
            X-TTFB-Ms: 156.23
            X-Audio-Source: sarvam
            Content-Type: audio/wav
          
T+158ms+: Stream generator runs
          Accumulate chunks in buffer
          When buffer ≥ 640 bytes:
            Extract packet
            Yield 640 bytes
            (~40ms per packet)
            
          Repeat until:
          - All incoming chunks received
          - Buffer flushed
          
T+3000ms+: Stream complete
          Final WAV bytes sent to client
```

---

## Performance Optimization

### TTFB Optimization Strategies

| Strategy | Implementation | Impact |
|----------|-----------------|--------|
| **Cache Pre-warming** | Load phrases at startup | -100ms (cache hits) |
| **Connection Pooling** | aiohttp TCPConnector | -50ms (handshake) |
| **DNS Caching** | ttl_dns_cache=300 | -10ms (DNS lookup) |
| **Async I/O** | FastAPI + asyncio | -50ms (blocking) |
| **Monotonic Timing** | time.perf_counter() | Accurate measurement |

### Theoretical TTFB Breakdown

**Cache Hit (30-45ms):**
```
Validation:         1ms
Router lookup:      1ms
Cache disk read:   15ms
BytesIO creation:   3ms
Packet scheduler:   2ms
Response headers:   8ms
────────────────────────
Total:             30ms
```

**Sarvam Synthesis (150-220ms):**
```
Validation:         1ms
Router setup:       1ms
Sarvam request:    10ms
Network latency:   40ms
API processing:    80ms
First chunk:       15ms
Packet scheduler:   2ms
Response headers:   8ms
────────────────────────
Total:            157ms (avg)
```

### Memory Optimization

**Per-Request Memory:**
- Input text: ~500B (max 5000 chars)
- Metadata dict: ~200B
- Audio buffer (scheduler): 640B (one packet)
- Temporary objects: ~500B
- **Total:** ~1.8 KB per request

**Global Memory:**
- Cache service: ~1 MB (10-100 phrases)
- HTTP session: ~2 MB (connection pool)
- Scheduler instances: ~500 B (reused)
- **Total:** ~3.5 MB baseline

### Throughput Analysis

**Concurrent Requests:**
```
Connection pool size: 10 total
Per-host limit: 5

Support ~100 concurrent requests
(10 connections × 10 requests per connection)

Bandwidth at max capacity:
100 requests × 640 bytes/packet × 25 packets/second
= 1.6 MB/s upstream to client
```

---

## Deployment & Scaling

### Docker Containerization

**Dockerfile:**
```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY indic_tts_runtime/ ./indic_tts_runtime/
COPY .env .

EXPOSE 8000
CMD ["python", "-m", "indic_tts_runtime.main"]
```

**Build & Run:**
```bash
docker build -t indic-tts:latest .

docker run \
  -p 8000:8000 \
  --env-file .env \
  -v ./test_outputs:/app/test_outputs \
  indic-tts:latest
```

### Kubernetes Deployment

**Pod Spec (deployment.yaml):**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: indic-tts
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: tts-engine
        image: indic-tts:latest
        ports:
        - containerPort: 8000
        env:
        - name: SARVAM_API_KEY
          valueFrom:
            secretKeyRef:
              name: tts-secrets
              key: api-key
        livenessProbe:
          httpGet:
            path: /api/v1/health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /api/v1/health
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 5
```

### Load Balancing

**Nginx Config:**
```nginx
upstream tts_backend {
    least_conn;  # Load balancing strategy
    server tts-1:8000 weight=1;
    server tts-2:8000 weight=1;
    server tts-3:8000 weight=1;
}

server {
    listen 80;
    location /api/ {
        proxy_pass http://tts_backend;
        proxy_buffering off;  # Disable for streaming
        proxy_request_buffering off;
        
        # Timeouts
        proxy_connect_timeout 10s;
        proxy_send_timeout 30s;
        proxy_read_timeout 30s;
    }
}
```

---

## Troubleshooting & Monitoring

### Common Issues

| Issue | Diagnosis | Solution |
|-------|-----------|----------|
| TTFB > 220ms | Check Sarvam API latency | Add phrases to cache |
| Cache miss errors | Verify phrase in dictionary | Add phrase to `_phrase_map` |
| 500 Internal Error | Check service initialization | Review startup logs |
| Memory leak | Monitor process RSS | Check for circular references |
| Timeout errors | Network latency | Increase timeout, check connectivity |

### Debug Logging

**Enable DEBUG level:**
```bash
# In .env
LOG_LEVEL=DEBUG

# Or via environment
export LOG_LEVEL=DEBUG
python -m indic_tts_runtime.main
```

**Sample DEBUG output:**
```
DEBUG: cache_service.py - Cache lookup for phrase: "aapka checkup fee..."
DEBUG: router.py - Cache MISS - proceeding to primary
DEBUG: sarvam_service.py - Synthesizing text via Sarvam API: "Aapka check..."
DEBUG: scheduler.py - Packet extracted: 640 bytes (packet #1)
```

### Monitoring Queries

**Prometheus Metrics (if integrated):**
```promql
# Average TTFB
rate(tts_total_ttfb_ms[5m]) / rate(tts_requests_total[5m])

# Cache hit rate
rate(tts_cache_hits[5m]) / rate(tts_requests_total[5m])

# Error rate
rate(tts_failed_requests[5m]) / rate(tts_requests_total[5m])

# P95 latency (estimated)
histogram_quantile(0.95, rate(tts_ttfb_seconds_bucket[5m]))
```

### Health Check Script

```bash
#!/bin/bash
# Monitor TTS engine health every 30 seconds

while true; do
    STATUS=$(curl -s http://localhost:8000/api/v1/health | jq .status)
    METRICS=$(curl -s http://localhost:8000/api/v1/metrics)
    
    AVG_TTFB=$(echo $METRICS | jq .average_ttfb_ms)
    CACHE_HITS=$(echo $METRICS | jq .cache_hits)
    SUCCESS_RATE=$(echo $METRICS | jq .success_rate_percent)
    
    echo "[$(date)] Status: $STATUS | TTFB: ${AVG_TTFB}ms | Cache Hits: $CACHE_HITS | Success: ${SUCCESS_RATE}%"
    sleep 30
done
```

---

## Conclusion

This implementation provides a **production-grade, highly optimized** TTS engine with:

✅ **Low Latency:** Dynamic TTFB target < 220ms  
✅ **Intelligent Routing:** Cache-first strategy with fallback  
✅ **Streaming Efficiency:** 40ms packet regulation, jitter elimination  
✅ **Connection Optimization:** Persistent pooled connections  
✅ **Clean Architecture:** SRP, type hints, async/await  
✅ **Observable:** Comprehensive metrics & logging  

Ready for production deployment with Kubernetes, Docker, and cloud platforms.

---

**Last Updated:** 2024
**Version:** 1.0.0
