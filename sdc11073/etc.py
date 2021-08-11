
def apply_map(function, *iterable):
    """call function for all elements of iterable(s).
    apply_map uses builtin map internally."""
    return list(map(function, *iterable))
