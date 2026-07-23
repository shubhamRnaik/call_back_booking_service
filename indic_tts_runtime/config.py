"""
Configuration module using Pydantic BaseSettings for environment validation.
Loads, validates, and provides type-safe access to all environment variables.
"""

from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field, validator
import os


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables with validation.
    Ensures all configuration is type-safe and validated at startup.
    """

    # Sarvam AI Configuration
    sarvam_api_key: str = Field(..., env="SARVAM_API_KEY")
    sarvam_api_url: str = Field(
        default="https://api.sarvam.ai/text-to-speech",
        env="SARVAM_API_URL"
    )

    # Voice Assistant Identity
    service_name: str = Field(default="Voice Assistant", env="SERVICE_NAME")

    # LLM Provider Configuration
    llm_provider: str = Field(default="auto", env="LLM_PROVIDER")

    # OpenAI Configuration
    openai_api_key: Optional[str] = Field(default=None, env="OPENAI_API_KEY")
    openai_model: str = Field(default="gpt-4o-mini", env="OPENAI_MODEL")

    # Gemini Configuration
    gemini_api_key: Optional[str] = Field(default=None, env="GEMINI_API_KEY")
    gemini_model: str = Field(default="gemini-2.0-flash", env="GEMINI_MODEL")

    # Audio Configuration
    # ⚠️ IMPORTANT: Sarvam Bulbul V3 outputs at 22050 Hz (not 8000 Hz)
    # Using 8000 Hz causes ~2.75x slowdown and robotic voice
    default_sample_rate: int = Field(default=22050, env="DEFAULT_SAMPLE_RATE")
    default_audio_codec: str = Field(
        default="linear16", env="DEFAULT_AUDIO_CODEC"
    )
    packet_duration_ms: int = Field(default=40, env="PACKET_DURATION_MS")
    stt_sample_rate: int = Field(default=16000, env="STT_SAMPLE_RATE")
    tts_sample_rate: int = Field(default=22050, env="TTS_SAMPLE_RATE")
    default_language_code: str = Field(
        default="hi-IN", env="DEFAULT_LANGUAGE_CODE"
    )

    # Cache Configuration
    cache_dir: str = Field(default="database/cache", env="CACHE_DIR")
    cache_enabled: bool = Field(default=True, env="CACHE_ENABLED")

    # Server Configuration
    server_host: str = Field(default="0.0.0.0", env="SERVER_HOST")
    server_port: int = Field(default=8000, env="SERVER_PORT")
    log_level: str = Field(default="DEBUG", env="LOG_LEVEL")

    # Performance Targets
    target_ttfb_ms: float = Field(default=220.0, env="TARGET_TTFB_MS")
    enable_metrics: bool = Field(default=True, env="ENABLE_METRICS")

    # Security and Rate Limiting
    security_enabled: bool = Field(default=False, env="SECURITY_ENABLED")
    service_api_key: Optional[str] = Field(default=None, env="SERVICE_API_KEY")
    rest_rate_limit_per_min: int = Field(default=120, env="REST_RATE_LIMIT_PER_MIN")
    ws_connect_rate_limit_per_min: int = Field(default=30, env="WS_CONNECT_RATE_LIMIT_PER_MIN")

    # WebSocket Session Limits
    max_ws_connections_total: int = Field(default=200, env="MAX_WS_CONNECTIONS_TOTAL")
    max_ws_connections_per_ip: int = Field(default=8, env="MAX_WS_CONNECTIONS_PER_IP")
    max_ws_session_seconds: int = Field(default=900, env="MAX_WS_SESSION_SECONDS")
    max_ws_utterances: int = Field(default=120, env="MAX_WS_UTTERANCES")

    # Retry Strategy
    retry_max_attempts: int = Field(default=3, env="RETRY_MAX_ATTEMPTS")
    retry_base_delay_ms: int = Field(default=150, env="RETRY_BASE_DELAY_MS")
    retry_jitter_ms: int = Field(default=120, env="RETRY_JITTER_MS")

    # LLM timeout tuning
    llm_api_timeout_sec: float = Field(default=12.0, env="LLM_API_TIMEOUT_SEC")

    class Config:
        env_file = ".env"
        case_sensitive = False

    @validator("llm_provider")
    def validate_llm_provider(cls, v: str) -> str:
        """Validate and normalize LLM provider selection."""
        if not v:
            return "auto"
        provider = v.strip().lower()
        if provider not in {"auto", "openai", "gemini"}:
            raise ValueError(
                "LLM provider must be one of "
                "{'auto', 'openai', 'gemini'}, "
                f"got {v}"
            )
        return provider

    @validator("default_sample_rate")
    def validate_sample_rate(cls, v: int) -> int:
        """Validate that sample rate is a common audio rate."""
        valid_rates = {8000, 16000, 22050, 44100, 48000}
        if v not in valid_rates:
            raise ValueError(
                f"Sample rate must be one of {valid_rates}, got {v}"
            )
        return v

    @validator("default_audio_codec")
    def validate_audio_codec(cls, v: str) -> str:
        """Validate that audio codec is supported."""
        valid_codecs = {"linear16", "pcm", "wav"}
        if v.lower() not in valid_codecs:
            raise ValueError(
                f"Audio codec must be one of {valid_codecs}, got {v}"
            )
        return v.lower()

    @validator("packet_duration_ms")
    def validate_packet_duration(cls, v: int) -> int:
        """Validate that packet duration is reasonable."""
        if v < 10 or v > 100:
            raise ValueError(
                f"Packet duration must be between 10ms and 100ms, got {v}ms"
            )
        return v

    @validator("target_ttfb_ms")
    def validate_target_ttfb(cls, v: float) -> float:
        """Validate that target TTFB is realistic."""
        if v < 50 or v > 1000:
            raise ValueError(
                f"Target TTFB must be between 50ms and 1000ms, got {v}ms"
            )
        return v

    @validator("stt_sample_rate")
    def validate_stt_sample_rate(cls, v: int) -> int:
        """Validate that STT sample rate is reasonable (16kHz standard)."""
        valid_rates = {8000, 16000, 22050, 44100, 48000}
        if v not in valid_rates:
            raise ValueError(
                f"STT sample rate must be one of {valid_rates}, got {v}"
            )
        return v

    @validator("tts_sample_rate")
    def validate_tts_sample_rate(cls, v: int) -> int:
        """Validate that TTS sample rate is reasonable."""
        valid_rates = {8000, 16000, 22050, 44100, 48000}
        if v not in valid_rates:
            raise ValueError(
                f"TTS sample rate must be one of {valid_rates}, got {v}"
            )
        return v

    @validator("default_language_code")
    def validate_language_code(cls, v: str) -> str:
        """Validate language code format (e.g., hi-IN, ta-IN)."""
        valid_languages = {
            "hi-IN", "ta-IN", "te-IN", "kn-IN", "mr-IN",
            "bn-IN", "ml-IN", "gu-IN", "en-IN", "en-US"
        }
        if v not in valid_languages:
            logger = __import__('logging').getLogger(__name__)
            logger.warning(
                f"Language code '{v}' not in standard list, but allowing"
            )
        return v

    @property
    def cache_directory_path(self) -> str:
        """Get absolute path to cache directory."""
        return os.path.abspath(self.cache_dir)

    @property
    def effective_llm_provider(self) -> str:
        """Choose the active LLM provider based on configuration."""
        provider = (self.llm_provider or "").strip().lower()
        if provider in {"openai", "gemini"}:
            return provider
        if self.openai_api_key:
            return "openai"
        if self.gemini_api_key:
            return "gemini"
        return "gemini"


# Global settings instance
settings = Settings()
