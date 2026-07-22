#!/usr/bin/env python
"""Quick test of clause streaming fix."""
import asyncio
import sys
sys.path.insert(0, '.')

from indic_tts_runtime.chunker import StreamTextChunker

async def test_push_token():
    """Test clause streaming works."""
    print("Testing push_token method...")
    
    # Test 1: Boundary punctuation
    chunker = StreamTextChunker(min_word_threshold=2, max_word_threshold=4)
    chunk1 = await chunker.push_token("Hello ")
    assert chunk1 is None, "Should not emit yet"
    chunk2 = await chunker.push_token("world.")
    assert chunk2 == "Hello world.", f"Should emit on period, got {chunk2}"
    print("✓ Boundary punctuation test passed")
    
    # Test 2: Flush remaining
    chunker2 = StreamTextChunker()
    await chunker2.push_token("leftover")
    final = chunker2.flush()
    assert final == "leftover", f"Flush should return buffer, got {final}"
    print("✓ Flush test passed")
    
    # Test 3: Helper methods still exist
    chunker3 = StreamTextChunker()
    assert hasattr(chunker3, '_has_boundary_punctuation'), "Missing _has_boundary_punctuation"
    assert hasattr(chunker3, '_extract_chunk_at_boundary'), "Missing _extract_chunk_at_boundary"
    assert hasattr(chunker3, '_extract_chunk_at_word_boundary'), "Missing _extract_chunk_at_word_boundary"
    print("✓ Helper methods exist")
    
    print("\n✅ All tests passed! Clause streaming is fixed.")

asyncio.run(test_push_token())
