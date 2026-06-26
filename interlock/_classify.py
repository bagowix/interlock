"""Default failure classification policy.

Implements ``FailureClassifier``: the baseline treats any raised exception as a
failure and any returned value as a success. Result-based predicates and
exception allow/ignore lists layer on top in the public API (M5).
"""

__all__ = ('DefaultFailureClassifier',)


class DefaultFailureClassifier:
    """Counts a call as a failure exactly when it raised."""

    def is_failure(self, *, result: object, exception: BaseException | None) -> bool:  # noqa: ARG002
        """Return whether a completed call counts as a failure.

        Args:
            result: The call's return value; ignored by this policy.
            exception: The exception the call raised, or ``None`` if it returned.

        Returns:
            ``True`` if the call raised, ``False`` otherwise.
        """
        return exception is not None
