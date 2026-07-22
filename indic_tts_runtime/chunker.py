"""
Streaming Text Chunker: Accumulates incoming tokens and yields text chunks intelligently.
Chunk boundaries are triggered by punctuation or word count thresholds.
"""

import logging
from typing import AsyncGenerator, Optional
import asyncio

logger = logging.getLogger(__name__)


class StreamTextChunker:
    """
    Async stream processor for LLM output tokens.
    Intelligently chunks text based on punctuation and word count.
    """

    # Punctuation that triggers chunk boundary (strong boundary)
    CHUNK_BOUNDARIES = {'.', '?', '!', '|'}
    
    # Soft boundaries (minor pause points but don't necessarily trigger chunk)
    SOFT_BOUNDARIES = {',', ';', ':'}

    def __init__(
        self,
        min_word_threshold: int = 5,
        max_word_threshold: int = 7,
        buffer_timeout_sec: float = 0.5
    ):
        """
        Initialize text chunker.
        
        Args:
            min_word_threshold: Minimum words before yielding on punctuation
            max_word_threshold: Maximum words before yielding (force chunk)
            buffer_timeout_sec: Timeout to yield partial chunk if no input received
        """
        self.min_word_threshold = min_word_threshold
        self.max_word_threshold = max_word_threshold
        self.buffer_timeout_sec = buffer_timeout_sec
        
        self._buffer = ""
        self._word_count = 0
        self._last_input_time = None

    async def chunk_stream(
        self,
        token_stream: AsyncGenerator[str, None]
    ) -> AsyncGenerator[str, None]:
        """
        Process incoming token stream and yield text chunks.
        
        Args:
            token_stream: Async generator yielding tokens from LLM
            
        Yields:
            Text chunks ready for TTS synthesis
        """
        try:
            # Create timeout task for buffer flush
            timeout_task = None
            
            async for token in token_stream:
                self._buffer += token
                self._last_input_time = asyncio.get_event_loop().time()
                self._word_count = len(self._buffer.split())

                # Check for strong chunk boundaries (sentence-ending punctuation)
                if self._has_boundary_punctuation(self._buffer):
                    chunk = self._extract_chunk_at_boundary()
                    if chunk.strip():
                        logger.debug(f"Boundary chunk ({len(chunk)} chars): {chunk[:50]}...")
                        yield chunk
                    self._buffer = ""
                    self._word_count = 0

                # Check for max word threshold (force yield)
                elif self._word_count >= self.max_word_threshold:
                    chunk = self._extract_chunk_at_word_boundary()
                    if chunk.strip():
                        logger.debug(f"Threshold chunk ({len(chunk)} chars): {chunk[:50]}...")
                        yield chunk
                    self._buffer = ""
                    self._word_count = 0

            # Yield remaining buffer at end of stream
            if self._buffer.strip():
                logger.debug(f"Final chunk ({len(self._buffer)} chars): {self._buffer[:50]}...")
                yield self._buffer
                self._buffer = ""
                self._word_count = 0

        except Exception as e:
            logger.error(f"Error in chunk_stream: {e}")
            raise

    # **TASK 1**: Push individual tokens and emit chunks for streaming TTS
    async def push_token(self, token: Optional[str]) -> Optional[str]:
        """
        Append a single token to buffer and emit a chunk if boundary reached.
        Non-async version for integration with LLM streaming.
        
        Args:
            token: Single token from LLM stream
            
        Returns:
            A text chunk if boundary hit, None otherwise
        """
        if not token:
            return None
        
        self._buffer += token
        self._last_input_time = asyncio.get_event_loop().time()
        self._word_count = len(self._buffer.split())
        
        # Check for strong chunk boundaries (sentence-ending punctuation)
        if self._has_boundary_punctuation(self._buffer):
            chunk = self._extract_chunk_at_boundary()
            if chunk.strip():
                logger.debug(f"Clause boundary: {chunk[:60]}...")
                return chunk
            self._buffer = ""
            self._word_count = 0
            return None
        
        # Check for max word threshold (force yield)
        if self._word_count >= self.max_word_threshold:
            chunk = self._extract_chunk_at_word_boundary()
            if chunk.strip():
                logger.debug(f"Clause threshold: {chunk[:60]}...")
                return chunk
            self._buffer = ""
            self._word_count = 0
            return None
        
        return None
    
    
    def flush(self) -> Optional[str]:
        """
        Return any buffered text and clear state.
        Call this when LLM stream ends.
        
        Returns:
            Remaining buffered text or None
        """
        if self._buffer.strip():
            chunk = self._buffer.strip()
            self._buffer = ""
            self._word_count = 0
            return chunk
        return None

    def _has_boundary_punctuation(self, text: str) -> bool:
        """
        Check if text contains strong chunk boundary punctuation.
        
        Args:
            text: Text to check
            
        Returns:
            True if strong boundary punctuation found
        """
        # Look for sentence-ending punctuation followed by space or end
        for boundary in self.CHUNK_BOUNDARIES:
            if boundary in text:
                return True
        return False

    def _extract_chunk_at_boundary(self) -> str:
        """
        Extract chunk up to the first boundary punctuation.
        
        Returns:
            Text chunk including the boundary punctuation
        """
        for boundary in self.CHUNK_BOUNDARIES:
            if boundary in self._buffer:
                idx = self._buffer.rfind(boundary)
                # Include the punctuation in the chunk
                chunk = self._buffer[:idx + 1]
                # Remove chunk from buffer, preserving anything after
                self._buffer = self._buffer[idx + 1:].lstrip()
                return chunk

        return ""

    def _extract_chunk_at_word_boundary(self) -> str:
        """
        Extract chunk at a natural word boundary (space).
        Used when max word threshold is reached.
        
        Returns:
            Text chunk ending at a word boundary
        """
        words = self._buffer.split()
        
        if len(words) <= self.max_word_threshold:
            return self._buffer

        # Take words up to max threshold
        chunk_words = words[:self.max_word_threshold]
        remaining_words = words[self.max_word_threshold:]
        
        chunk = ' '.join(chunk_words)
        self._buffer = ' '.join(remaining_words)
        
        return chunk

    def reset(self) -> None:
        """
        Reset the chunker state.
        Useful for error recovery or starting new session.
        """
        self._buffer = ""
        self._word_count = 0
        self._last_input_time = None
        logger.debug("Chunker reset")


# Example: Simple async token generator for testing
async def sample_token_generator() -> AsyncGenerator[str, None]:
    """
    Sample token generator simulating LLM output.
    Used for testing the chunker.
    """
    tokens = [
        "Hello ", "there! ", "This ", "is ", "a ", "test. ",
        "The ", "meeting ", "is ", "at ", "2:30 ", "PM. ",
        "I ", "have ", "₹450 ", "in ", "my ", "wallet."
    ]
    
    for token in tokens:
        await asyncio.sleep(0.01)  # Simulate token delay
        yield token


# Test example
async def test_chunker():
    """Test the chunker with sample token stream."""
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    chunker = StreamTextChunker(
        min_word_threshold=5,
        max_word_threshold=7,
        buffer_timeout_sec=0.5
    )
    
    print("=== Chunker Test ===")
    chunk_num = 0
    
    async for chunk in chunker.chunk_stream(sample_token_generator()):
        chunk_num += 1
        print(f"Chunk {chunk_num}: [{chunk}]")
    
    print("=== Test Complete ===")


if __name__ == "__main__":
    asyncio.run(test_chunker())
