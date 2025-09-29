"""Utilities for reading MERGE_MANAGER_SETTINGS from the project settings."""

from __future__ import annotations

from typing import Any, Iterable

from django.conf import settings as django_settings
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string


DEFAULTS: dict[str, Any] = {
    "PROFILES": (),
    "FIELD_STRATEGIES": {
        "prefer_target": "merge_manager.services.strategies.prefer_target",
        "prefer_donor": "merge_manager.services.strategies.prefer_donor",
        "prefer_non_null": "merge_manager.services.strategies.prefer_non_null",
        "concat": "merge_manager.services.strategies.concat",
    },
    "SOFT_DELETE_FIELD": "is_active",
    "SOFT_DELETE_VALUE": False,
    "SOFT_DELETE_FLAG_FIELD": None,
    "DRY_RUN_DEFAULT": False,
    "AUDIT_MODEL": "merge_manager.MergeOperation",
    "AUTO_DISCOVER_MODULES": (),
    "AUDIT_EXTRA_FIELDS": (),
}

IMPORT_STRINGS = {"FIELD_STRATEGIES", "AUDIT_MODEL"}


class MergeManagerSettings:
    """Proxy object that exposes settings with defaults and lazy imports."""

    def __init__(self, user_settings: dict[str, Any] | None = None) -> None:
        if user_settings is None:
            user_settings = getattr(django_settings, "MERGE_MANAGER_SETTINGS", {})

        if not isinstance(user_settings, dict):
            raise ImproperlyConfigured("MERGE_MANAGER_SETTINGS must be a dict.")

        self._user_settings = user_settings
        self._cached: dict[str, Any] = {}

    def __getattr__(self, attr: str) -> Any:
        if attr not in DEFAULTS:
            raise AttributeError(f"Invalid merge manager setting: {attr}")

        if attr in self._cached:
            return self._cached[attr]

        value = self._user_settings.get(attr, DEFAULTS[attr])
        value = self._perform_import(attr, value)
        self._cached[attr] = value
        return value

    def reload(self) -> None:
        """Clear cached values (useful in tests)."""

        self._cached.clear()

    def _perform_import(self, attr: str, value: Any) -> Any:
        if attr not in IMPORT_STRINGS:
            return value

        if attr == "FIELD_STRATEGIES":
            return {
                key: import_string(path) if isinstance(path, str) else path
                for key, path in value.items()
            }

        if isinstance(value, str):
            return import_string(value)

        if isinstance(value, Iterable):
            return [import_string(item) if isinstance(item, str) else item for item in value]

        return value


merge_manager_settings = MergeManagerSettings()


__all__ = ["merge_manager_settings", "MergeManagerSettings"]
