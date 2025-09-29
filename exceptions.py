"""Domain specific exceptions for the merge_manager app."""

from __future__ import annotations


class MergeManagerError(Exception):
    """Base exception for merge_manager errors."""


class ProfileNotFoundError(MergeManagerError):
    """Raised when the registry cannot find a requested merge profile."""


class InvalidProfileError(MergeManagerError):
    """Raised when a merge profile definition is invalid."""


class StrategyNotFoundError(MergeManagerError):
    """Raised when a field strategy cannot be resolved."""


class MergeValidationError(MergeManagerError):
    """Raised when a merge cannot be executed due to validation errors."""
