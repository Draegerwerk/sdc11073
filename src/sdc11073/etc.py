
def apply_map(function, *iterable):
    """call function for all elements of iterable(s).
    apply_map uses builtin map internally."""
    return list(map(function, *iterable))


def _short_action_string(action: str):
    """ return only the last 2 elements of the action """
    elements = action.split('/')
    ret = '/'.join(elements[-2:])
    return ret


def short_filter_string(actions):
    """
    Helper function to make shorter action strings for logging
    :param actions: list of strings
    :return: a comma separated string of shortened names
    """
    return ', '.join([_short_action_string(a) for a in actions])
