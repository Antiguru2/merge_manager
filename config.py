"""Registration of merge profiles and field strategies."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, Iterator, Mapping, Sequence

from django.apps import apps
from django.db import models
from django.utils.module_loading import import_string

from .exceptions import (
    InvalidProfileError,
    ProfileNotFoundError,
    StrategyNotFoundError,
)
from .settings import merge_manager_settings

FieldStrategyCallable = Callable[[Any, Any, dict[str, Any]], Any]


@dataclass(slots=True)
class FieldMergeRule:
    """Describe how a specific field should be merged."""

    strategy: str | FieldStrategyCallable
    allow_override: bool = True
    description: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def get_callable(self) -> FieldStrategyCallable:
        if callable(self.strategy):
            return self.strategy

        if isinstance(self.strategy, str):
            if self.strategy in merge_manager_settings.FIELD_STRATEGIES:
                return merge_manager_settings.FIELD_STRATEGIES[self.strategy]

            return import_string(self.strategy)

        raise StrategyNotFoundError(
            f"Unable to resolve field merge strategy {self.strategy!r}"
        )


@dataclass(slots=True)
class MergeProfile:
    """Configuration object describing how to merge specific model instances."""

    label: str
    model: str | type[models.Model]
    fields: Mapping[str, FieldMergeRule] = field(default_factory=dict)
    display_fields: Sequence[str] = field(default_factory=tuple)
    soft_delete_field: str | None = None
    soft_delete_value: Any | None = None
    hard_delete: bool = False
    pre_merge_hooks: Sequence[Callable[..., None]] = field(default_factory=tuple)
    post_merge_hooks: Sequence[Callable[..., None]] = field(default_factory=tuple)
    form_class: str | type | None = None

    def __post_init__(self) -> None:
        self.fields = {
            name: value if isinstance(value, FieldMergeRule) else FieldMergeRule(**value)
            for name, value in self.fields.items()
        }
        self.display_fields = tuple(self.display_fields)

    def get_model_class(self) -> type[models.Model]:
        if isinstance(self.model, str):
            return apps.get_model(self.model)
        return self.model

    def get_soft_delete_field(self) -> str | None:
        if self.soft_delete_field is not None:
            return self.soft_delete_field
        return merge_manager_settings.SOFT_DELETE_FIELD

    def get_soft_delete_value(self) -> Any:
        if self.soft_delete_value is not None:
            return self.soft_delete_value
        return merge_manager_settings.SOFT_DELETE_VALUE

    def resolve_form_class(self) -> type | None:
        if self.form_class is None:
            return None
        if isinstance(self.form_class, str):
            return import_string(self.form_class)
        return self.form_class


class MergeProfileRegistry:
    """Container that stores registered merge profiles."""

    def __init__(self) -> None:
        self._profiles: Dict[str, MergeProfile] = {}
        self._model_index: Dict[type[models.Model], MergeProfile] = {}

    def register(self, profile: MergeProfile | Mapping[str, Any]) -> MergeProfile:
        if isinstance(profile, Mapping):
            profile = MergeProfile(**profile)

        if not profile.label:
            raise InvalidProfileError("Merge profile must define a label")

        model_cls = profile.get_model_class()

        self._profiles[profile.label] = profile
        self._model_index[model_cls] = profile
        return profile

    def unregister(self, label: str) -> None:
        profile = self._profiles.pop(label, None)
        if not profile:
            return

        model_cls = profile.get_model_class()
        self._model_index.pop(model_cls, None)

    def get(self, label: str) -> MergeProfile:
        try:
            return self._profiles[label]
        except KeyError as exc:
            raise ProfileNotFoundError(f"Merge profile '{label}' is not registered") from exc

    def get_for_model(self, model: type[models.Model]) -> MergeProfile:
        try:
            return self._model_index[model]
        except KeyError as exc:
            raise ProfileNotFoundError(
                f"No merge profile registered for model {model._meta.label}"
            ) from exc

    def all(self) -> Iterator[MergeProfile]:
        return iter(self._profiles.values())

    def clear(self) -> None:
        self._profiles.clear()
        self._model_index.clear()


registry = MergeProfileRegistry()


def _resolve_profile_source(source: Any) -> Iterable[MergeProfile | Mapping[str, Any]]:
    if isinstance(source, MergeProfile):
        return [source]

    if isinstance(source, Mapping):
        return [source]

    if isinstance(source, (list, tuple, set)):
        return list(source)

    if isinstance(source, str):
        resolved = import_string(source)
        return _resolve_profile_source(resolved)

    if callable(source):
        result = source()
        return _resolve_profile_source(result) if result is not None else []

    if source is None:
        return []

    if isinstance(source, Iterable) and not isinstance(source, (str, bytes, bytearray)):
        return list(source)

    return [source]


def load_profiles_from_settings() -> None:
    """Populate registry based on MERGE_MANAGER_SETTINGS['PROFILES']."""

    registry.clear()

    for item in merge_manager_settings.PROFILES:
        for profile in _resolve_profile_source(item):
            registry.register(profile)

    for module_path in merge_manager_settings.AUTO_DISCOVER_MODULES:
        import_string(module_path)


__all__ = [
    "FieldMergeRule",
    "MergeProfile",
    "MergeProfileRegistry",
    "load_profiles_from_settings",
    "registry",
]
