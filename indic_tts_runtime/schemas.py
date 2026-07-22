"""
API request and response schemas using Pydantic v2.
Provides data validation and serialization for TTS pipeline.
"""

from pydantic import BaseModel, Field, validator
from typing import Optional
from enum import Enum


class LanguageCode(str, Enum):
    """Supported language codes for TTS."""
    HINDI = "hi-IN"
    ENGLISH = "en-IN"
    TAMIL = "ta-IN"
    TELUGU = "te-IN"
    KANNADA = "kn-IN"
    MARATHI = "mr-IN"
    GUJARATI = "gu-IN"
    MALAYALAM = "ml-IN"


class SpeakerProfile(str, Enum):
    """Available speaker profiles."""
    SHUBH = "shubh"
    MEERA = "meera"
    KARAN = "karan"
    PRIYA = "priya"
    AMRIT = "amrit"


class TTSRequest(BaseModel):
    """
    Request schema for Text-to-Speech synthesis.
    Validates input text, language, speaker, and pace parameters.
    """

    text: str = Field(
        ...,
        min_length=1,
        max_length=5000,
        description="Raw text to synthesize into speech"
    )
    
    target_language_code: LanguageCode = Field(
        default=LanguageCode.HINDI,
        description="Target language code (ISO 639-1 with region)"
    )
    
    speaker: SpeakerProfile = Field(
        default=SpeakerProfile.SHUBH,
        description="Voice profile for synthesis"
    )
    
    pace: float = Field(
        default=0.95,
        ge=0.5,
        le=2.0,
        description="Speech pace multiplier (0.5 = slow, 1.0 = normal, 2.0 = fast)"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "text": "Namaste, aap kaisa ho?",
                "target_language_code": "hi-IN",
                "speaker": "shubh",
                "pace": 0.95
            }
        }

    @validator("text")
    def validate_text_clean(cls, v: str) -> str:
        """Ensure text is clean and safe."""
        v = v.strip()
        if not v:
            raise ValueError("Text cannot be empty after stripping whitespace")
        if len(v) > 5000:
            raise ValueError("Text exceeds maximum length of 5000 characters")
        return v


class TTSResponse(BaseModel):
    """
    Response schema for TTS synthesis results.
    Contains status, metadata, and audio delivery information.
    """

    request_id: str = Field(
        ...,
        description="Unique identifier for this TTS request"
    )
    
    status: str = Field(
        default="success",
        description="Status of the request (success, processing, error)"
    )
    
    audio_format: str = Field(
        default="audio/wav",
        description="MIME type of audio content"
    )
    
    sample_rate: int = Field(
        default=8000,
        description="Audio sample rate in Hz"
    )
    
    duration_ms: Optional[float] = Field(
        default=None,
        description="Expected duration of audio in milliseconds"
    )
    
    ttfb_ms: float = Field(
        ...,
        description="Time-to-First-Byte measured in milliseconds"
    )
    
    source: str = Field(
        ...,
        description="Audio source (cache, sarvam, fallback)"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "request_id": "req_1234567890abcdef",
                "status": "success",
                "audio_format": "audio/wav",
                "sample_rate": 8000,
                "duration_ms": 5420.0,
                "ttfb_ms": 45.23,
                "source": "cache"
            }
        }


class ErrorResponse(BaseModel):
    """
    Error response schema.
    Provides structured error information to clients.
    """

    error_code: str = Field(
        ...,
        description="Error code identifier"
    )
    
    message: str = Field(
        ...,
        description="Human-readable error message"
    )
    
    details: Optional[dict] = Field(
        default=None,
        description="Additional error details"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "error_code": "CACHE_MISS_NETWORK_ERROR",
                "message": "Failed to synthesize text and no cache available",
                "details": {"attempted_sources": ["cache", "sarvam"]}
            }
        }


class HealthCheckResponse(BaseModel):
    """
    Health check response schema.
    Indicates system status and component availability.
    """

    status: str = Field(default="healthy", description="Overall system status")
    cache_available: bool = Field(description="Cache service availability")
    sarvam_api_reachable: bool = Field(description="Sarvam API connectivity")
    stt_service_reachable: bool = Field(default=False, description="STT service connectivity")
    llm_service_reachable: bool = Field(default=False, description="LLM/brain service connectivity")
    tts_service_reachable: bool = Field(default=False, description="TTS service connectivity")
    uptime_seconds: float = Field(description="Service uptime in seconds")
    details: Optional[dict] = Field(default=None, description="Additional health details")
