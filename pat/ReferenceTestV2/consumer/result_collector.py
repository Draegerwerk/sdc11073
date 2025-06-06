import dataclasses


@dataclasses.dataclass
class ResultEntry:
    """Represents one result entry."""

    verdict: bool | None
    step: str
    info: str
    xtra: str

    def __str__(self):
        verdict_str = {None: 'no result', True: 'passed', False: 'failed'}
        return f'{self.step:6s}:{verdict_str[self.verdict]:10s} {self.info}{self.xtra}'


class ResultsCollector:
    """Result collector."""

    def __init__(self):
        self._results: list[ResultEntry] = []

    def log_result(self, is_ok: bool | None, step: str, info: str, extra_info: str | None = None):
        """Log the result."""
        xtra = f' ({extra_info}) ' if extra_info else ''
        self._results.append(ResultEntry(is_ok, step, info, xtra))

    def print_summary(self):
        """Print the summary."""
        print('\n### Summary ###')
        for r in self._results:
            print(r)

    @property
    def failed_count(self) -> int:
        """Get the amount of failures."""
        return len([r for r in self._results if r.verdict is False])

class _ResultCollector:

    def __init__(self):
        self._results: list[ResultEntry] = []
        
    def log_success(self, step: str, message: str) -> None:
        pass
    
    def log_failure(self, step: str, message: str) -> None:
        pass
    
ResultCollector = _ResultCollector()
