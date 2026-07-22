"""
Telephony Audio Codec & Resampler Utility for Exotel Cloud Telephony integration.
Handles conversions between telephony audio formats (8kHz 8-bit mu-law PCMU, 8kHz 16-bit PCM)
and internal voice pipeline formats (16kHz PCM STT, 24kHz/22.05kHz PCM TTS).
"""

import base64
import logging
import numpy as np

logger = logging.getLogger(__name__)

# Pre-computed G.711 mu-law lookup tables for fast audioop-free conversions
def _build_mulaw_dec_table() -> np.ndarray:
    table = np.zeros(256, dtype=np.int16)
    for i in range(256):
        u = ~i & 0xFF
        sign = u & 0x80
        exponent = (u >> 4) & 0x07
        mantissa = u & 0x0F
        sample = (((mantissa << 1) + 33) << exponent) - 33
        if sign:
            sample = -sample
        table[i] = np.int16(sample << 2)
    return table

def _build_mulaw_enc_table() -> np.ndarray:
    table = np.zeros(65536, dtype=np.uint8)
    BIAS = 0x84
    CLIP = 32635
    exp_lut = [0x4000, 0x2000, 0x1000, 0x0800, 0x0400, 0x0200, 0x0100, 0x0080]
    for idx in range(65536):
        val = idx - 65536 if idx >= 32768 else idx
        sign = 0
        if val < 0:
            val = -val
            sign = 0x80
        if val > CLIP:
            val = CLIP
        val += BIAS
        exponent = 7
        for exp, mask in enumerate(exp_lut):
            if val & mask:
                exponent = 7 - exp
                break
        mantissa = (val >> (exponent + 3)) & 0x0F
        table[idx] = ~(sign | (exponent << 4) | mantissa) & 0xFF
    return table

MULAW_DEC_TABLE = _build_mulaw_dec_table()
MULAW_ENC_TABLE = _build_mulaw_enc_table()


def mulaw_to_pcm16(mulaw_bytes: bytes) -> bytes:
    """Convert 8-bit mu-law bytes to 16-bit signed linear PCM bytes."""
    if not mulaw_bytes:
        return b""
    arr = np.frombuffer(mulaw_bytes, dtype=np.uint8)
    return MULAW_DEC_TABLE[arr].tobytes()


def pcm16_to_mulaw(pcm_bytes: bytes) -> bytes:
    """Convert 16-bit signed linear PCM bytes to 8-bit mu-law bytes."""
    if not pcm_bytes:
        return b""
    arr = np.frombuffer(pcm_bytes, dtype=np.int16)
    uarr = arr.view(np.uint16)
    return MULAW_ENC_TABLE[uarr].tobytes()


def resample_pcm(pcm_bytes: bytes, orig_sr: int, target_sr: int) -> bytes:
    """Resample 16-bit signed PCM audio bytes from orig_sr to target_sr using linear interpolation."""
    if orig_sr == target_sr or not pcm_bytes:
        return pcm_bytes
    samples = np.frombuffer(pcm_bytes, dtype=np.int16)
    if len(samples) == 0:
        return b""
    num_target_samples = int(round(len(samples) * target_sr / orig_sr))
    if num_target_samples <= 0:
        return b""
    orig_indices = np.linspace(0, len(samples) - 1, num_target_samples)
    resampled = np.interp(orig_indices, np.arange(len(samples)), samples)
    return resampled.astype(np.int16).tobytes()


def telephony_to_stt_pcm(
    base64_payload: str,
    source_codec: str = "mulaw",
    source_sr: int = 8000,
    target_sr: int = 16000,
) -> bytes:
    """
    Decodes base64 telephony payload and converts to 16kHz 16-bit PCM for STT.
    
    1. Decodes base64 string payload.
    2. If source_codec is 'mulaw' / 'pcmu': converts 8kHz 8-bit mu-law to 8kHz 16-bit Linear PCM.
    3. Resamples 8kHz PCM up to 16kHz PCM for SarvamSaarasSTTClient.
    """
    if not base64_payload:
        return b""
    
    try:
        raw_bytes = base64.b64decode(base64_payload)
    except Exception as e:
        logger.error(f"Failed to decode base64 telephony audio payload: {e}")
        return b""

    codec_clean = (source_codec or "mulaw").lower().strip()
    if codec_clean in ("mulaw", "pcmu", "ulaw", "audio/x-mulaw"):
        pcm_8k = mulaw_to_pcm16(raw_bytes)
    else:
        pcm_8k = raw_bytes

    pcm_16k = resample_pcm(pcm_8k, orig_sr=source_sr, target_sr=target_sr)
    return pcm_16k


def tts_pcm_to_telephony(
    pcm_bytes: bytes,
    source_sr: int = 22050,
    target_codec: str = "mulaw",
    target_sr: int = 8000,
) -> str:
    """
    Converts TTS output 16-bit PCM (e.g. 24kHz/22.05kHz) to telephony payload.
    
    1. Resamples TTS PCM down to target_sr (default 8kHz PCM).
    2. If target_codec is 'mulaw' / 'pcmu': converts 8kHz 16-bit PCM to 8kHz 8-bit mu-law.
    3. Returns base64-encoded string ready for Exotel payload.
    """
    if not pcm_bytes:
        return ""

    pcm_8k = resample_pcm(pcm_bytes, orig_sr=source_sr, target_sr=target_sr)

    codec_clean = (target_codec or "mulaw").lower().strip()
    if codec_clean in ("mulaw", "pcmu", "ulaw", "audio/x-mulaw"):
        telephony_bytes = pcm16_to_mulaw(pcm_8k)
    else:
        telephony_bytes = pcm_8k

    return base64.b64encode(telephony_bytes).decode("ascii")
