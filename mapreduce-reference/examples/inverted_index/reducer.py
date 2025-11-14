"""
Inverted Index Reducer

Aggregates document IDs for each word.

Input: (word, [doc_id1, doc_id2, ...])
Output: (word, "doc_id1,doc_id2,doc_id3")
"""


def reducer(key, values):
    """
    Combine all document IDs for a word.
    
    Args:
        key: The word
        values: List of document IDs containing this word
    
    Yields:
        (word, comma-separated list of document IDs)
    """
    # Get unique document IDs and sort them
    unique_docs = sorted(set(values))
    
    # Join with commas
    doc_list = ','.join(unique_docs)
    
    yield (key, doc_list)
