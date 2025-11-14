def mapper(line):
    """
    Map function for word count.
    Emits (word, 1) for each word in the line.
    """
    # Remove punctuation and split into words
    import re
    words = re.findall(r'\b\w+\b', line.lower())
    
    for word in words:
        yield (word, 1)
