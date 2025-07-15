"""Result collector for test results."""

class _ResultCollector:
    def __init__(self):
        self._results: list[str] = []
        self._failed: bool = False

    def log_success(self, step: str, message: str) -> None:
        self._results.append(f'Success: {step} - {message}')

    def log_failure(self, step: str, message: str) -> None:
        self._results.append(f'Failure: {step} - {message}')
        self._failed = True

    @property
    def failed(self) -> bool:
        return self._failed

    def print_summary(self) -> None:
        print('\n### Summary ###')
        for result in self._results:
            print(result)


ResultCollector = _ResultCollector()
