"""
Inverted Index Mapper

Creates an inverted index mapping words to document IDs.
This is useful for search engines and text analysis.

Input: Lines of text with format "doc_id: content"
Output: (word, doc_id) pairs
"""

import re


def mapper(line):
    """
    Extract words from a document and emit (word, doc_id) pairs.
    
    Expected input format: "doc_123: This is the document content"
    """
    # Split on first colon to separate doc_id from content
    parts = line.split(':', 1)
    
    if len(parts) != 2:
        # Skip malformed lines
        return
    
    doc_id = parts[0].strip()
    content = parts[1].strip()
    
    # Extract words (alphanumeric sequences)
    words = re.findall(r'\b\w+\b', content.lower())
    
    # Emit unique words from this document
    seen = set()
    for word in words:
        if word not in seen and len(word) > 2:  # Filter short words
            seen.add(word)
            yield (word, doc_id)
