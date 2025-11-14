def reducer(key, values):
    """
    Reduce function for word count.
    Sums up all counts for each word.
    """
    total = sum(int(v) for v in values)
    yield (key, total)
