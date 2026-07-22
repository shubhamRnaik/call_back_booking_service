"""
Multilingual Text Normalizer: Converts raw input text to phonetically normalized forms.
Supports 8 Indian languages with currency expansion, number/time conversion, and fault tolerance.
"""

import re
import logging
from typing import Optional
from indic_numtowords import num2words

logger = logging.getLogger(__name__)


class MultilingualTextNormalizer:
    """
    Normalizes text for TTS synthesis across multiple Indian languages.
    Handles currency expansion, number/time conversion, and fault tolerance.
    """

    # Language code mapping: ISO code -> indic_numtowords lang code
    LANGUAGE_MAP = {
        "hi": "hi",      # Hindi
        "ta": "ta",      # Tamil
        "te": "te",      # Telugu
        "kn": "kn",      # Kannada
        "mr": "mr",      # Marathi
        "bn": "bn",      # Bengali
        "ml": "ml",      # Malayalam
        "gu": "gu",      # Gujarati
        "en": "en",      # English
    }

    # Currency suffixes by language
    CURRENCY_SUFFIXES = {
        "hi": "रुपये",           # Rupees in Hindi
        "ta": "ரூபாய்",          # Rupees in Tamil
        "te": "రూపాయలు",        # Rupees in Telugu
        "kn": "ರೂಪಾಯಗಳು",       # Rupees in Kannada
        "mr": "रुपये",           # Rupees in Marathi
        "bn": "টাকা",            # Taka in Bengali
        "ml": "രൂപ",            # Rupees in Malayalam
        "gu": "રુપિયા",          # Rupees in Gujarati
        "en": "rupees",         # Rupees in English
    }

    # Currency regex patterns
    CURRENCY_PATTERNS = [
        r'₹\s*([0-9,]+(?:\.[0-9]{2})?)',      # ₹450 or ₹450.50
        r'Rs\.\s*([0-9,]+(?:\.[0-9]{2})?)',   # Rs.450
        r'INR\s+([0-9,]+(?:\.[0-9]{2})?)',    # INR 450
        r'([0-9,]+(?:\.[0-9]{2})?)\s*(?:₹|Rs|INR)',  # 450 ₹
    ]

    # Time regex patterns (handles: 11 AM, 2:30 PM, 14:30, etc.)
    TIME_PATTERNS = [
        r'([0-9]{1,2}):([0-9]{2})\s*(AM|PM|am|pm)',   # 2:30 PM
        r'([0-9]{1,2})\s*(AM|PM|am|pm)',               # 2 PM
        r'([0-9]{1,2}):([0-9]{2})',                    # 14:30 (24-hr)
    ]

    # Punctuation to preserve for pause indicators
    PAUSE_PUNCTUATION = {'.', ',', '?', '|', '!', ';', ':'}

    def __init__(self, default_language: str = "hi"):
        """
        Initialize normalizer with default language.
        
        Args:
            default_language: ISO language code (default: "hi" for Hindi)
        """
        if default_language not in self.LANGUAGE_MAP:
            logger.warning(
                f"Language '{default_language}' not supported. Defaulting to 'hi'"
            )
            self.default_language = "hi"
        else:
            self.default_language = default_language

    def normalize(self, text: str, target_language_code: str = "hi-IN") -> str:
        """
        Normalize text for TTS synthesis.
        
        Args:
            text: Raw input text
            target_language_code: Target language code (e.g., "hi-IN", "ta-IN")
            
        Returns:
            Normalized text safe for TTS synthesis
        """
        try:
            # Extract language code from full code (e.g., "hi-IN" -> "hi")
            iso_lang = self._extract_iso_lang(target_language_code)
            
            if iso_lang not in self.LANGUAGE_MAP:
                logger.warning(
                    f"Language code '{target_language_code}' not supported. "
                    f"Returning original text."
                )
                return text

            # Pipeline: Currency -> Time -> Numbers -> Clean
            normalized = text
            normalized = self._expand_currency(normalized, iso_lang)
            normalized = self._expand_time(normalized, iso_lang)
            normalized = self._expand_numbers(normalized, iso_lang)
            normalized = self._clean_unprintables(normalized)
            
            return normalized

        except Exception as e:
            logger.error(f"Normalization error for '{text[:50]}': {e}")
            return text

    def _extract_iso_lang(self, language_code: str) -> str:
        """
        Extract ISO language code from full language code.
        
        Args:
            language_code: Full language code (e.g., "hi-IN")
            
        Returns:
            ISO language code (e.g., "hi")
        """
        # Split on hyphen and take first part, convert to lowercase
        iso_lang = language_code.split('-')[0].lower()
        return iso_lang

    def _expand_currency(self, text: str, iso_lang: str) -> str:
        """
        Expand currency symbols to language-specific words.
        
        Args:
            text: Input text
            iso_lang: ISO language code
            
        Returns:
            Text with currency symbols expanded
        """
        try:
            suffix = self.CURRENCY_SUFFIXES.get(iso_lang, "rupees")
            
            for pattern in self.CURRENCY_PATTERNS:
                def replace_currency(match):
                    # Extract the amount from the match
                    amount_str = match.group(1)
                    # Remove commas and convert to number
                    amount = float(amount_str.replace(',', ''))
                    
                    try:
                        # Convert number to language words
                        num_lang = self.LANGUAGE_MAP.get(iso_lang, "en")
                        amount_words = num2words(int(amount), lang=num_lang)
                        return f"{amount_words} {suffix}"
                    except Exception as e:
                        logger.debug(f"Failed to convert currency {amount_str}: {e}")
                        return match.group(0)  # Return original if conversion fails

                text = re.sub(pattern, replace_currency, text, flags=re.IGNORECASE)
            
            return text

        except Exception as e:
            logger.debug(f"Currency expansion error: {e}")
            return text

    def _expand_time(self, text: str, iso_lang: str) -> str:
        """
        Expand time formats to language-specific words.
        Handles: "2:30 PM", "14:30", "2 PM"
        
        Args:
            text: Input text
            iso_lang: ISO language code
            
        Returns:
            Text with times expanded
        """
        try:
            num_lang = self.LANGUAGE_MAP.get(iso_lang, "en")

            # Pattern 1: "HH:MM AM/PM" (e.g., "2:30 PM")
            pattern1 = r'([0-9]{1,2}):([0-9]{2})\s*(AM|PM|am|pm)'
            
            def replace_12hr_time(match):
                hour = int(match.group(1))
                minute = int(match.group(2))
                period = match.group(3).upper()
                
                try:
                    hour_words = num2words(hour, lang=num_lang)
                    if minute == 0:
                        return f"{hour_words} {period}"
                    else:
                        minute_words = num2words(minute, lang=num_lang)
                        return f"{hour_words} {minute_words} {period}"
                except Exception as e:
                    logger.debug(f"Failed to convert 12-hr time: {e}")
                    return match.group(0)

            text = re.sub(pattern1, replace_12hr_time, text)

            # Pattern 2: "HH:MM" in 24-hr format (e.g., "14:30")
            pattern2 = r'([0-9]{1,2}):([0-9]{2})(?!\s*(?:AM|PM|am|pm))'
            
            def replace_24hr_time(match):
                hour = int(match.group(1))
                minute = int(match.group(2))
                
                try:
                    hour_words = num2words(hour, lang=num_lang)
                    if minute == 0:
                        return hour_words
                    else:
                        minute_words = num2words(minute, lang=num_lang)
                        return f"{hour_words} {minute_words}"
                except Exception as e:
                    logger.debug(f"Failed to convert 24-hr time: {e}")
                    return match.group(0)

            text = re.sub(pattern2, replace_24hr_time, text)

            # Pattern 3: "H AM/PM" (e.g., "2 PM")
            pattern3 = r'([0-9]{1,2})\s+(AM|PM|am|pm)'
            
            def replace_hr_only(match):
                hour = int(match.group(1))
                period = match.group(2).upper()
                
                try:
                    hour_words = num2words(hour, lang=num_lang)
                    return f"{hour_words} {period}"
                except Exception as e:
                    logger.debug(f"Failed to convert hour: {e}")
                    return match.group(0)

            text = re.sub(pattern3, replace_hr_only, text)

            return text

        except Exception as e:
            logger.debug(f"Time expansion error: {e}")
            return text

    def _expand_numbers(self, text: str, iso_lang: str) -> str:
        """
        Expand remaining numeric digits to language words.
        
        Args:
            text: Input text
            iso_lang: ISO language code
            
        Returns:
            Text with numbers expanded
        """
        try:
            num_lang = self.LANGUAGE_MAP.get(iso_lang, "en")

            # Pattern: Find all standalone numbers (not part of other patterns)
            pattern = r'\b([0-9]+)\b'
            
            def replace_number(match):
                num_str = match.group(1)
                try:
                    num = int(num_str)
                    # Convert number to language words
                    return num2words(num, lang=num_lang)
                except Exception as e:
                    logger.debug(f"Failed to convert number {num_str}: {e}")
                    return match.group(0)  # Return original if conversion fails

            text = re.sub(pattern, replace_number, text)

            return text

        except Exception as e:
            logger.debug(f"Number expansion error: {e}")
            return text

    def _clean_unprintables(self, text: str) -> str:
        """
        Remove unprintable characters while preserving pause punctuation.
        
        Args:
            text: Input text
            
        Returns:
            Cleaned text
        """
        try:
            cleaned = ""
            
            for char in text:
                # Keep letters, digits, spaces, and pause punctuation
                if (
                    char.isalnum() or 
                    char.isspace() or 
                    char in self.PAUSE_PUNCTUATION or
                    ord(char) >= 128  # Keep Unicode characters for other languages
                ):
                    cleaned += char
                # Skip unprintable ASCII control characters
                elif ord(char) < 32:
                    if char == '\n':
                        cleaned += ' '  # Convert newlines to spaces
                    # Skip other control characters
                else:
                    # For other unprintable ASCII, skip
                    pass
            
            # Clean up multiple spaces
            cleaned = re.sub(r'\s+', ' ', cleaned).strip()
            
            return cleaned

        except Exception as e:
            logger.debug(f"Cleanup error: {e}")
            return text


# Test/Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    normalizer = MultilingualTextNormalizer()
    
    # Test cases
    test_cases = [
        ("I have ₹450 in my wallet", "hi-IN"),
        ("The meeting is at 2:30 PM today", "hi-IN"),
        ("வணக்கம், Rs. 1000 தயவு செய்து", "ta-IN"),
        ("నా వద్ద 150 రూపాయలు ఉన్నాయి", "te-IN"),
        ("उसके पास 500 रुपये हैं", "hi-IN"),
    ]
    
    for text, lang in test_cases:
        result = normalizer.normalize(text, lang)
        print(f"Input:  {text}")
        print(f"Lang:   {lang}")
        print(f"Output: {result}")
        print()
