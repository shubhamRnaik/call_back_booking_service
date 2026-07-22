from indic_tts_runtime.config import Settings


def test_default_gemini_model_uses_supported_flash_model():
    settings = Settings(_env_file=None, sarvam_api_key="test-key")
    assert settings.gemini_model == "gemini-2.0-flash"
