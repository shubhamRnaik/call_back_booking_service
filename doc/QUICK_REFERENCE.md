# Quick Reference: Code Snippets & Usage Patterns

> Quick lookup guide for common operations and code patterns in the TTS engine.

---

## Table of Contents

1. [Installation & Setup](#installation--setup)
2. [Running the Server](#running-the-server)
3. [API Usage Examples](#api-usage-examples)
4. [Service Integration](#service-integration)
5. [Configuration Patterns](#configuration-patterns)
6. [Debugging Tips](#debugging-tips)

---

## Installation & Setup

### Option 1: Manual Setup

```bash
# Clone/navigate to project
cd ~/projects/indic_tts_runtime

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
nano indic_tts_runtime/.env
# Update SARVAM_API_KEY

# Run initial setup check
python setup.py

# Start server
cd indic_tts_runtime
python main.py
```

### Option 2: Automated Setup

```bash
# Run setup script (checks dependencies, creates cache)
python setup.py

# If all checks pass:
# → Start server: cd indic_tts_runtime && python main.py
# → Run tests: python test_pipeline.py
```

### Option 3: Docker

```bash
# Build image
docker build -t indic-tts:v1 .

# Run container
docker run -p 8000:8000 \
  --env-file indic_tts_runtime/.env \
  -v $(pwd)/test_outputs:/app/test_outputs \
  indic-tts:v1

# Access on http://localhost:8000
```

---

## Running the Server

### Standard Start

```bash
cd indic_tts_runtime
python main.py

# Output:
# === TTS Engine Startup ===
# ✓ Cache Service initialized
# ✓ Sarvam Service initialized with persistent connection
# ✓ Voice Router initialized
# ✓ Packet Scheduler initialized
# === All services initialized successfully ===
# INFO:     Uvicorn running on http://0.0.0.0:8000
```

### With Custom Host/Port

```bash
# Via environment
export SERVER_HOST=127.0.0.1
export SERVER_PORT=9000
python main.py  # Runs on http://127.0.0.1:9000

# Or modify .env
# SERVER_HOST=127.0.0.1
# SERVER_PORT=9000
```

### With Debug Logging

```bash
# Set log level
export LOG_LEVEL=DEBUG
python main.py

# See detailed debug output:
# DEBUG: cache_service.py - Cache lookup for phrase...
# DEBUG: sarvam_service.py - Synthesizing text via Sarvam API...
# DEBUG: scheduler.py - Packet extracted: 640 bytes
```

### Graceful Shutdown

```bash
# Ctrl+C to shutdown
# Server will:
# - Stop accepting new requests
# - Close Sarvam API session
# - Release resources
# - Print shutdown log
```

---

## API Usage Examples

### cURL Examples

**Synthesize Text (Cache Hit)**
```bash
curl -X POST http://localhost:8000/api/v1/stream-voice \
  -H "Content-Type: application/json" \
  -d '{
    "text": "Haanji",
    "target_language_code": "hi-IN",
    "speaker": "shubh",
    "pace": 0.95
  }' \
  -D - \
  --output haanji.wav
```

**Synthesize Text (Dynamic)**
```bash
curl -X POST http://localhost:8000/api/v1/stream-voice \
  -H "Content-Type: application/json" \
  -d '{
    "text": "Aapka checkup fee teen sau rupaye hai, shaam char baje confirm karu?",
    "target_language_code": "hi-IN",
    "speaker": "shubh",
    "pace": 0.95
  }' \
  --output appointment.wav

# Check headers
curl -X POST http://localhost:8000/api/v1/stream-voice \
  -H "Content-Type: application/json" \
  -d '{"text": "Namaste"}' \
  -i > /dev/null  # Shows headers only
```

**Health Check**
```bash
curl http://localhost:8000/api/v1/health | jq .

# Output:
# {
#   "status": "healthy",
#   "cache_available": true,
#   "sarvam_api_reachable": true,
#   "uptime_seconds": 245.67
# }
```

**Get Metrics**
```bash
curl http://localhost:8000/api/v1/metrics | jq .

# Extract specific metric
curl http://localhost:8000/api/v1/metrics | jq .average_ttfb_ms

# Output: 142.34
```

### Python Client Example

```python
import aiohttp
import asyncio
from pathlib import Path

async def synthesize_tts(text: str):
    """Simple TTS client."""
    url = "http://localhost:8000/api/v1/stream-voice"
    payload = {
        "text": text,
        "target_language_code": "hi-IN",
        "speaker": "shubh",
        "pace": 0.95
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            if response.status == 200:
                # Extract metadata
                request_id = response.headers.get("X-Request-ID")
                ttfb_ms = response.headers.get("X-TTFB-Ms")
                source = response.headers.get("X-Audio-Source")
                
                print(f"Request ID: {request_id}")
                print(f"TTFB: {ttfb_ms}ms")
                print(f"Source: {source}")
                
                # Save audio
                audio = await response.read()
                output = Path(f"output_{request_id}.wav")
                output.write_bytes(audio)
                print(f"Saved: {output}")
            else:
                print(f"Error: {response.status}")

# Run
asyncio.run(synthesize_tts("Haanji"))
```

### JavaScript/Fetch Example

```javascript
async function synthesizeTTS(text) {
    const response = await fetch('http://localhost:8000/api/v1/stream-voice', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            text: text,
            target_language_code: 'hi-IN',
            speaker: 'shubh',
            pace: 0.95
        })
    });

    if (response.ok) {
        const requestId = response.headers.get('X-Request-ID');
        const ttfbMs = response.headers.get('X-TTFB-Ms');
        
        console.log(`Request: ${requestId}, TTFB: ${ttfbMs}ms`);
        
        // Stream to audio element
        const audioBlob = await response.blob();
        const audioUrl = URL.createObjectURL(audioBlob);
        
        const audio = new Audio(audioUrl);
        audio.play();
    }
}

synthesizeTTS("Aapka appointment confirm hai");
```

---

## Service Integration

### Using CacheService Directly

```python
from services.cache_service import CacheService

# Initialize
cache = CacheService()

# Lookup phrase
result = cache.lookup_phrase("namaste")
if result:
    audio_stream, metadata = result
    print(f"Cache hit! Duration: {metadata['duration_ms']}ms")
    audio_bytes = audio_stream.read()
else:
    print("Cache miss - try Sarvam")

# Register new phrase
cache.register_phrase("custom_greeting", audio_bytes)

# Get statistics
stats = cache.get_cache_stats()
print(f"Total phrases: {stats['total_phrases']}")
print(f"Cache size: {stats['total_size_mb']} MB")

# Clear cache (production: use with caution)
cache.clear_cache()
```

### Using SarvamService Directly

```python
import asyncio
from services.sarvam_service import SarvamService
from schemas import LanguageCode, SpeakerProfile

async def main():
    # Initialize
    sarvam = SarvamService()
    await sarvam.initialize()
    
    try:
        # Stream synthesis
        async for chunk in sarvam.synthesize_stream(
            text="Namaste, aap kaisa ho?",
            language=LanguageCode.HINDI,
            speaker=SpeakerProfile.SHUBH,
            pace=0.95
        ):
            print(f"Received chunk: {len(chunk)} bytes")
        
        # Full synthesis
        audio_stream, metadata = await sarvam.synthesize_full(
            text="Aapka checkup fee teen sau rupaye hai",
            language=LanguageCode.HINDI,
            speaker=SpeakerProfile.MEERA,
            pace=0.90
        )
        
        print(f"Duration: {metadata['duration_ms']}ms")
        print(f"Size: {metadata['size_bytes']} bytes")
        
        # Health check
        is_healthy = await sarvam.health_check()
        print(f"Sarvam API healthy: {is_healthy}")
        
    finally:
        await sarvam.close()

asyncio.run(main())
```

### Using VoiceRouter Directly

```python
import asyncio
from core.router import VoiceRouter, RoutingStrategy
from services.cache_service import CacheService
from services.sarvam_service import SarvamService
from schemas import LanguageCode, SpeakerProfile

async def main():
    # Setup services
    cache = CacheService()
    sarvam = SarvamService()
    await sarvam.initialize()
    
    # Create router
    router = VoiceRouter(
        cache_service=cache,
        sarvam_service=sarvam,
        strategy=RoutingStrategy.CACHE_FIRST
    )
    
    try:
        # Route single request
        audio_stream, metadata = await router.route_and_synthesize(
            text="Haanji",
            language=LanguageCode.HINDI,
            speaker=SpeakerProfile.SHUBH
        )
        
        print(f"Source: {metadata['source']}")
        print(f"TTFB expected: 30ms (cache) or 150ms (sarvam)")
        
        # Get routing config
        config = router.get_routing_config()
        print(f"Routing strategy: {config['routing_strategy']}")
        
    finally:
        await sarvam.close()

asyncio.run(main())
```

### Using PacketScheduler Directly

```python
import asyncio
from core.scheduler import PacketScheduler
from io import BytesIO

async def main():
    scheduler = PacketScheduler(
        packet_duration_ms=40,
        sample_rate=8000
    )
    
    # Schedule audio bytes
    audio_bytes = b'\x00' * 100000  # 100KB of silence
    
    packet_count = 0
    async for packet in scheduler.schedule_bytes_stream(audio_bytes):
        packet_count += 1
        print(f"Packet {packet_count}: {len(packet)} bytes")
    
    # Get stats
    stats = scheduler.get_scheduler_stats()
    print(f"Total packets: {stats['packets_emitted']}")
    print(f"Throughput: {stats['throughput_mbps']} Mbps")
    
    # Reset for next stream
    scheduler.reset()

asyncio.run(main())
```

---

## Configuration Patterns

### .env Template

```bash
# Sarvam AI Configuration
SARVAM_API_KEY=your_api_key_here
SARVAM_API_URL=https://api.sarvam.ai/api/v1/text-to-speech

# Audio Configuration
DEFAULT_SAMPLE_RATE=8000
DEFAULT_AUDIO_CODEC=linear16
PACKET_DURATION_MS=40

# Cache Configuration
CACHE_DIR=database/cache
CACHE_ENABLED=true

# Server Configuration
SERVER_HOST=0.0.0.0
SERVER_PORT=8000
LOG_LEVEL=INFO

# Performance Targets
TARGET_TTFB_MS=220
ENABLE_METRICS=true
```

### Production .env (Hardened)

```bash
# Use environment variables or secrets manager
SARVAM_API_KEY=${SARVAM_API_KEY}  # From env var

# More restrictive settings
CACHE_DIR=/var/cache/tts
CACHE_ENABLED=true

SERVER_HOST=127.0.0.1  # Bind to localhost
SERVER_PORT=8000

LOG_LEVEL=WARNING  # Less verbose
TARGET_TTFB_MS=200  # Stricter SLO
ENABLE_METRICS=true
```

### Testing .env

```bash
SARVAM_API_KEY=test_key_12345

CACHE_DIR=test_cache
CACHE_ENABLED=true

SERVER_HOST=127.0.0.1
SERVER_PORT=9000

LOG_LEVEL=DEBUG  # Verbose for debugging
TARGET_TTFB_MS=500  # Relaxed for testing
ENABLE_METRICS=true
```

---

## Debugging Tips

### Enable Verbose Logging

```python
# In main.py, add:
import sys
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] %(message)s',
    stream=sys.stdout
)
```

### Monitor Request Flow

```bash
# Terminal 1: Run server with debug logging
export LOG_LEVEL=DEBUG
cd indic_tts_runtime
python main.py

# Terminal 2: Make request and watch logs
curl -X POST http://localhost:8000/api/v1/stream-voice \
  -H "Content-Type: application/json" \
  -d '{"text": "Haanji"}' \
  -o test.wav

# Observe in Terminal 1:
# INFO - [req_xxx] ← New TTS request: "Haanji"...
# DEBUG - [req_xxx] → Routing through Voice Router...
# DEBUG - [req_xxx] Cache lookup for phrase: "haanji"
# INFO - [req_xxx] Cache HIT
# INFO - [req_xxx] ⏱ TTFB: 35.23ms
```

### Check Cache Contents

```python
from indic_tts_runtime.services.cache_service import CacheService

cache = CacheService()
stats = cache.get_cache_stats()

print(f"Cache enabled: {stats['cache_enabled']}")
print(f"Cache directory: {stats['cache_dir']}")
print(f"Phrases in map: {stats['total_phrases']}")
print(f"Files on disk: {stats['cached_files']}")
print(f"Total size: {stats['total_size_mb']} MB")

# List all cached phrases
import os
cache_dir = cache.cache_dir
for filename in os.listdir(cache_dir):
    filepath = os.path.join(cache_dir, filename)
    size = os.path.getsize(filepath)
    print(f"  {filename}: {size} bytes")
```

### Test Sarvam Connectivity

```python
import asyncio
from indic_tts_runtime.services.sarvam_service import SarvamService

async def test_sarvam():
    sarvam = SarvamService()
    await sarvam.initialize()
    
    try:
        is_healthy = await sarvam.health_check()
        print(f"Sarvam API reachable: {is_healthy}")
        
        stats = sarvam.get_connection_stats()
        print(f"Session initialized: {stats['session_initialized']}")
        print(f"API URL: {stats['api_url']}")
        
    finally:
        await sarvam.close()

asyncio.run(test_sarvam())
```

### Profile Request Timing

```python
import time
import requests

url = "http://localhost:8000/api/v1/stream-voice"

for i in range(5):
    start = time.perf_counter()
    
    response = requests.post(
        url,
        json={
            "text": "Test message",
            "target_language_code": "hi-IN"
        }
    )
    
    elapsed = (time.perf_counter() - start) * 1000
    ttfb = response.headers.get("X-TTFB-Ms", "N/A")
    
    print(f"Request {i+1}: Total={elapsed:.2f}ms, TTFB={ttfb}ms")
```

### Monitor Memory Usage

```bash
# Terminal 1: Start server
cd indic_tts_runtime
python main.py

# Terminal 2: Monitor memory every 5 seconds
while true; do
  PID=$(pgrep -f "python main.py")
  MEM=$(ps -p $PID -o rss= 2>/dev/null)
  if [ ! -z "$MEM" ]; then
    echo "$(date '+%H:%M:%S') - Memory: $((MEM / 1024)) MB"
  fi
  sleep 5
done
```

---

## Testing Checklist

- [ ] Run `python setup.py` - all checks pass
- [ ] Start server: `cd indic_tts_runtime && python main.py`
- [ ] Health check: `curl http://localhost:8000/api/v1/health`
- [ ] Cache hit test: `curl -X POST ... -d '{"text": "Haanji"}'`
- [ ] Dynamic synthesis: Full Hindi sentence
- [ ] Check metrics: `curl http://localhost:8000/api/v1/metrics`
- [ ] Run test pipeline: `python test_pipeline.py`
- [ ] Verify audio files created in `test_outputs/`
- [ ] Check TTFB < 220ms in metrics
- [ ] Verify cache hit rate > 0%

---

**Last Updated:** 2024
