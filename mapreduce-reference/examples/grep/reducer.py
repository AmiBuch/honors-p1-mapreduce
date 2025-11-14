"""
Grep Reducer

Count occurrences of each matching line.

Input: (line, [1, 1, 1, ...])
Output: (line, count)
"""


def reducer(key, values):
    """
    Count how many times each matching line appeared.
    
    Args:
        key: The matching line
        values: List of 1s (one per occurrence)
    
    Yields:
        (line, count)
    """
    count = sum(int(v) for v in values)
    yield (key, count)
