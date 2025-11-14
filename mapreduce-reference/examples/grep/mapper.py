"""
Grep Mapper

Search for lines containing a specific pattern.
This demonstrates a filtering MapReduce job.

To use: Set PATTERN environment variable or modify the pattern below.
"""

import re
import os


# Pattern to search for (can be configured via environment variable)
PATTERN = os.getenv('GREP_PATTERN', 'error')


def mapper(line):
    """
    Emit lines that match the search pattern.
    
    Args:
        line: Input line to search
    
    Yields:
        (line_number, line) if pattern is found
    """
    # Search for pattern (case-insensitive)
    if re.search(PATTERN, line, re.IGNORECASE):
        # Use line content as key so identical matches group together
        yield (line.strip(), 1)
