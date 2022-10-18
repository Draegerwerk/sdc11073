
class ApiUsageError(Exception):
    """This Exception is thrown when a call is made when it should not be called, e.g. call initialize() twice."""