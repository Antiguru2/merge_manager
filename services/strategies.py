"""Default field merging strategies."""

from __future__ import annotations

from typing import Any


def prefer_target(target_value: Any, donor_value: Any, context: dict[str, Any]) -> Any:
    """Keep the target value unless it is empty."""

    if _is_empty(target_value):
        return donor_value
    return target_value


def prefer_donor(target_value: Any, donor_value: Any, context: dict[str, Any]) -> Any:
    """Always take the donor value."""

    return donor_value


def prefer_non_null(target_value: Any, donor_value: Any, context: dict[str, Any]) -> Any:
    """Pick the first non-empty value among target/donor."""

    if not _is_empty(target_value):
        return target_value
    return donor_value


def concat(target_value: Any, donor_value: Any, context: dict[str, Any]) -> Any:
    """Concatenate values separated by a space if both present."""

    if _is_empty(target_value):
        return donor_value
    if _is_empty(donor_value):
        return target_value
    separator = context.get('separator', ' ')
    return f"{target_value}{separator}{donor_value}"


def _is_empty(value: Any) -> bool:
    return value in {None, ''}
