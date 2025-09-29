"""REST endpoints powering the merge manager interface."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from django.apps import apps
from datetime import date, datetime
from typing import Any, Dict, List, Tuple

from django.core.exceptions import FieldError, ObjectDoesNotExist, ValidationError as DjangoValidationError
from django.db import models
from django.db.models import Q, QuerySet
from django.utils import timezone
from django.utils.text import capfirst, slugify

from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from .config import FieldMergeRule, MergeProfile, registry
from .exceptions import MergeValidationError, ProfileNotFoundError
from .services.merge import MergeService

logger = logging.getLogger(__name__)

_MISSING = object()
_STRING_FIELD_TYPES = (
    models.CharField,
    models.TextField,
    models.EmailField,
    models.SlugField,
)


def _iterate_profiles() -> Tuple[List[Tuple[MergeProfile, str]], Dict[str, MergeProfile]]:
    profiles = list(registry.all())
    if not profiles:
        return [], {}

    profiles.sort(key=lambda profile: str(profile.label).casefold())
    used_slugs: set[str] = set()
    lookup: Dict[str, MergeProfile] = {}
    entries: List[Tuple[MergeProfile, str]] = []

    for profile in profiles:
        base_slug = slugify(str(profile.label))
        if not base_slug:
            base_slug = profile.get_model_class()._meta.model_name.replace('_', '-')
        slug = base_slug or f"profile-{len(entries) + 1}"
        counter = 2
        while slug in used_slugs:
            slug = f"{base_slug}-{counter}"
            counter += 1

        used_slugs.add(slug)
        entries.append((profile, slug))
        lookup[slug] = profile
        lookup[slug.casefold()] = profile
        label = str(profile.label)
        lookup[label] = profile
        lookup[label.casefold()] = profile

    return entries, lookup


def _serialize_profile_basic(profile: MergeProfile, slug: str) -> dict[str, Any]:
    model_cls = profile.get_model_class()
    return {
        "label": profile.label,
        "slug": slug,
        "model_label": model_cls._meta.label,
        "model_verbose_name": str(model_cls._meta.verbose_name),
        "fields_count": len(profile.fields),
        "soft_delete_field": profile.get_soft_delete_field(),
        "soft_delete_value": profile.get_soft_delete_value(),
        "display_fields": list(profile.display_fields),
    }


def _serialize_profile_detail(profile: MergeProfile, slug: str) -> dict[str, Any]:
    payload = _serialize_profile_basic(profile, slug)
    payload["fields"] = [
        {
            "name": name,
            "strategy": _get_strategy_name(rule),
            "allow_override": rule.allow_override,
            "description": rule.description,
        }
        for name, rule in profile.fields.items()
    ]
    return payload


def _get_strategy_name(rule: FieldMergeRule | None) -> str | None:
    if rule is None:
        return None
    strategy = rule.strategy
    if isinstance(strategy, str):
        return strategy
    if callable(strategy):
        return getattr(strategy, "__name__", str(strategy))
    return str(strategy)


def _resolve_profile(identifier: str | None) -> Tuple[MergeProfile, str]:
    entries, lookup = _iterate_profiles()
    if not identifier:
        raise ProfileNotFoundError("Profile identifier is required")

    profile = lookup.get(identifier)
    if profile is None:
        profile = lookup.get(str(identifier).casefold())
    if profile is None:
        raise ProfileNotFoundError(f"Unknown merge profile: {identifier}")

    slug = next((slug for current, slug in entries if current is profile), None)
    if slug is None:
        slug = slugify(str(profile.label)) or profile.get_model_class()._meta.model_name
    return profile, slug


def _parse_pk(model: type[models.Model], value: Any) -> Any:
    if value in (None, ""):
        return _MISSING
    try:
        return model._meta.pk.to_python(value)
    except (TypeError, ValueError, DjangoValidationError):
        return _MISSING


def _clean_limit(value: Any, default: int = 10, maximum: int = 50) -> int:
    try:
        limit = int(value)
    except (TypeError, ValueError):
        limit = default
    return max(1, min(limit, maximum))


def _get_search_fields(profile: MergeProfile, model: type[models.Model]) -> List[str]:
    if profile.display_fields:
        return [field for field in profile.display_fields if field]

    fields: List[str] = []
    for field in model._meta.get_fields():
        if not getattr(field, "concrete", False):
            continue
        if isinstance(field, _STRING_FIELD_TYPES):
            fields.append(field.name)
    return fields



def _perform_entity_search(
    profile: MergeProfile,
    query: str,
    limit: int,
) -> List[dict[str, Any]]:
    Model = profile.get_model_class()
    queryset: QuerySet = Model.objects.all()
    query = (query or "").strip()

    def _apply_default_order(qs: QuerySet) -> QuerySet:
        order_field = profile.display_fields[0] if profile.display_fields else None
        if order_field:
            try:
                return qs.order_by(order_field)
            except FieldError:
                return qs.order_by("pk")
        return qs.order_by("pk")

    if not query:
        queryset = _apply_default_order(queryset)[:limit]
        return [_serialize_entity(obj, profile) for obj in queryset]

    filters = Q()
    pk_value = _parse_pk(Model, query)
    if pk_value is not _MISSING:
        filters |= Q(pk=pk_value)

    for field in _get_search_fields(profile, Model):
        filters |= Q(**{f"{field}__icontains": query})

    if not filters:
        return []

    try:
        queryset = queryset.filter(filters)
    except FieldError:
        if pk_value is _MISSING:
            return []
        queryset = queryset.filter(pk=pk_value)

    queryset = queryset.distinct()
    queryset = _apply_default_order(queryset)[:limit]

    results = [_serialize_entity(obj, profile) for obj in queryset]

    if pk_value is not _MISSING and not any(str(item["id"]) == str(pk_value) for item in results):
        try:
            obj = Model.objects.get(pk=pk_value)
        except Model.DoesNotExist:
            pass
        else:
            results.insert(0, _serialize_entity(obj, profile))

    unique: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in results:
        identifier = str(item["id"])
        if identifier in seen:
            continue
        seen.add(identifier)
        unique.append(item)
        if len(unique) >= limit:
            break

    return unique


def _serialize_entity(instance: models.Model, profile: MergeProfile) -> dict[str, Any]:
    display = _build_display_value(instance, profile)
    return {
        "id": instance.pk,
        "display": display,
        "label": display,
        "subtitle": _build_subtitle(instance),
        "model": instance._meta.label,
        "admin_change_url": _get_admin_change_url(instance),
    }


def _get_admin_change_url(instance: models.Model) -> str | None:
    try:
        url = getattr(instance, 'admin_change_url', None)
    except Exception:  # pragma: no cover - defensive
        return None
    if callable(url):
        try:
            url = url()
        except TypeError:
            # property-style attribute without calling
            pass
        except Exception:  # pragma: no cover - defensive
            return None
    if not url:
        return None
    try:
        return str(url)
    except Exception:  # pragma: no cover - defensive
        return None


def _build_display_value(instance: models.Model, profile: MergeProfile) -> str:
    values: list[str] = []
    for field in profile.display_fields:
        if not field:
            continue
        value = _resolve_attr(instance, field)
        if value in (None, ""):
            continue
        values.append(str(_serialize_value(value)))
    if values:
        return " · ".join(values)
    return _object_label(instance)


def _build_subtitle(instance: models.Model) -> str:
    parts = [f"ID: {instance.pk}"]
    verbose = str(instance._meta.verbose_name)
    if verbose:
        parts.insert(0, capfirst(verbose))
    return " · ".join(parts)


def _object_label(instance: models.Model) -> str:
    try:
        label = str(instance)
    except Exception:  # pragma: no cover - defensive
        label = instance._meta.object_name
    pk = getattr(instance, "pk", None)
    if pk is not None and str(pk) not in label:
        return f"{label} (ID {pk})"
    return label


def _humanize_label(value: str) -> str:
    return capfirst(value.replace("_", " "))


def _resolve_attr(instance: models.Model, path: str) -> Any:
    current: Any = instance
    for chunk in path.split("__"):
        if current is None:
            return None
        current = getattr(current, chunk, None)
        if callable(current):
            try:
                current = current()
            except TypeError:
                return None
    return current


def _serialize_value(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, models.Model):
        return _object_label(value)
    if hasattr(value, "all"):
        try:
            return [str(item) for item in value.all()[:5]]
        except Exception:  # pragma: no cover - defensive
            return str(value)
    if isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray, dict)):
        return [str(item) for item in list(value)[:5]]
    return str(value)


def _build_snapshot(instance: models.Model, profile: MergeProfile) -> list[dict[str, Any]]:
    fields = list(profile.display_fields) or list(profile.fields.keys())
    pk_name = instance._meta.pk.name
    if pk_name not in fields:
        fields.insert(0, pk_name)

    snapshot: list[dict[str, Any]] = []
    seen: set[str] = set()
    for field in fields:
        if not field or field in seen:
            continue
        seen.add(field)
        value = _serialize_value(_resolve_attr(instance, field))
        snapshot.append(
            {
                "field": field,
                "label": _humanize_label(field.split("__")[-1]),
                "value": value,
            }
        )
    return snapshot


def _build_differences(result, profile: MergeProfile) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for field, payload in sorted(result.changed_fields.items()):
        rule = profile.fields.get(field)
        items.append(
            {
                "field": field,
                "label": _humanize_label(field),
                "target": payload.get("from"),
                "donor": payload.get("donor"),
                "result": payload.get("to"),
                "source": payload.get("source"),
                "strategy": _get_strategy_name(rule),
            }
        )
    return items


def _safe_relation_count(instance: models.Model, accessor: str) -> int | None:
    try:
        manager = getattr(instance, accessor)
    except AttributeError:
        return None

    if hasattr(manager, "count"):
        try:
            return manager.count()
        except Exception:  # pragma: no cover - defensive
            return None
    return None


def _resolve_relation_label(name: str, info: dict[str, Any]) -> str:
    related_model = info.get("related_model")
    if related_model:
        try:
            model = apps.get_model(related_model)
        except (LookupError, ValueError):
            pass
        else:
            meta = model._meta
            if info.get("type") in {"one_to_many", "many_to_many", "many_to_many_reverse"}:
                verbose_name = meta.verbose_name_plural or meta.verbose_name
            else:
                verbose_name = meta.verbose_name
            if verbose_name:
                return capfirst(str(verbose_name))
    return _humanize_label(name)


def _build_relations(result, target: models.Model) -> list[dict[str, Any]]:
    relations: list[dict[str, Any]] = []
    for name, info in sorted(result.relations.items()):
        relations.append(
            {
                "name": name,
                "label": _resolve_relation_label(name, info),
                "type": info.get("type"),
                "related_model": info.get("related_model"),
                "action": "transfer",
                "counts": {
                    "donor": info.get("count", 0),
                    "target": _safe_relation_count(target, name),
                },
            }
        )
    return relations


def _build_warnings(result, differences: list[dict[str, Any]], relations: list[dict[str, Any]]) -> list[str]:
    warnings: list[str] = []
    if not differences and not relations:
        warnings.append("Различий между объектами не найдено.")

    soft_info = result.soft_delete if isinstance(result.soft_delete, dict) else {}
    if soft_info.get("applied") is False:
        reason = soft_info.get("reason")
        if reason == "field_missing":
            field = soft_info.get("field")
            warnings.append(f"Soft-delete не применён: поле `{field}` отсутствует у объекта.")
        elif reason == "soft_delete_disabled":
            warnings.append("Soft-delete отключён для текущего профиля.")

    hard_info = result.hard_delete if isinstance(result.hard_delete, dict) else {}
    if hard_info.get("enabled") and result.dry_run:
        warnings.append("Установлен Hard-delete. ⚠️⚠️⚠️Донор будет удален!!!")

    return warnings


def _build_summary(result, differences: list[dict[str, Any]], relations: list[dict[str, Any]]) -> dict[str, Any]:
    soft_info = result.soft_delete if isinstance(result.soft_delete, dict) else {}
    hard_info = result.hard_delete if isinstance(result.hard_delete, dict) else {}
    return {
        "target_label": _object_label(result.target),
        "donor_label": _object_label(result.donor),
        "differences_count": len(differences),
        "relations_count": sum((item.get("counts", {}).get("donor") or 0) for item in relations),
        "soft_delete": {
            "applied": soft_info.get("applied", False),
            "field": soft_info.get("field"),
            "value": soft_info.get("to"),
            "reason": soft_info.get("reason"),
        },
        "hard_delete": {
            "enabled": hard_info.get("enabled", False),
            "applied": hard_info.get("applied", False),
            "dry_run": hard_info.get("dry_run"),
        },
        "dry_run": result.dry_run,
        "status": result.status,
    }


def _build_result_payload(result, profile: MergeProfile) -> dict[str, Any]:
    target = result.target
    donor = result.donor
    differences = _build_differences(result, profile)
    relations = _build_relations(result, target)
    warnings = _build_warnings(result, differences, relations)
    return {
        "target_snapshot": _build_snapshot(target, profile),
        "donor_snapshot": _build_snapshot(donor, profile),
        "differences": differences,
        "relations": relations,
        "warnings": warnings,
        "summary": _build_summary(result, differences, relations),
        "actions": {
            "soft_delete": result.soft_delete,
            "hard_delete": result.hard_delete,
        },
        "generated_at": timezone.now().isoformat(),
    }


def _build_extra_summary(notes: str | None, origin: str) -> dict[str, Any]:
    summary = {"origin": origin}
    if notes:
        summary["notes"] = notes
    return summary


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    if isinstance(value, (int, float)):
        return value != 0
    return False


def _error_response(message: str, *, data: Any | None = None) -> Response:
    payload: dict[str, Any] = {"success": False, "message": str(message)}
    if data is not None:
        payload["data"] = data
    return Response(payload)


def _success_response(data: Any, *, message: str | None = None) -> Response:
    payload: dict[str, Any] = {"success": True, "data": data}
    if message:
        payload["message"] = message
    return Response(payload)


class MergeProfileListAPIView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request) -> Response:  # noqa: D401 - DRF signature
        entries, _ = _iterate_profiles()
        data = []
        for index, (profile, slug) in enumerate(entries):
            item = _serialize_profile_basic(profile, slug)
            item["is_default"] = index == 0
            data.append(item)
        return _success_response(data)


class MergeProfileDetailAPIView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, slug: str) -> Response:  # noqa: D401
        try:
            profile, resolved_slug = _resolve_profile(slug)
        except ProfileNotFoundError as exc:  # pragma: no cover - defensive
            return _error_response(str(exc))
        return _success_response(_serialize_profile_detail(profile, resolved_slug))


class MergeEntitySearchAPIView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request) -> Response:  # noqa: D401
        profile_id = request.query_params.get("profile")
        query = request.query_params.get("query", "")
        limit = _clean_limit(request.query_params.get("limit"), default=100, maximum=500)

        try:
            profile, _ = _resolve_profile(profile_id)
        except ProfileNotFoundError as exc:
            return _error_response(str(exc))

        results = _perform_entity_search(profile, query, limit)
        return _success_response(results)


class MergePreviewAPIView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request) -> Response:  # noqa: D401
        profile_id = request.data.get("profile")
        target_id = request.data.get("target_id")
        donor_id = request.data.get("donor_id")
        notes = request.data.get("notes")
        field_overrides = request.data.get("field_overrides") or {}
        if not isinstance(field_overrides, dict):
            field_overrides = {}

        try:
            profile, _ = _resolve_profile(profile_id)
        except ProfileNotFoundError as exc:
            return _error_response(str(exc))

        Model = profile.get_model_class()
        target_pk = _parse_pk(Model, target_id)
        donor_pk = _parse_pk(Model, donor_id)

        if target_pk is _MISSING or donor_pk is _MISSING:
            return _error_response("Для предпросмотра укажите приёмника и донора")
        if str(target_pk) == str(donor_pk):
            return _error_response("Приёмник и донор не могут совпадать")

        try:
            target = Model.objects.get(pk=target_pk)
            donor = Model.objects.get(pk=donor_pk)
        except ObjectDoesNotExist:
            return _error_response("Не удалось найти указанные объекты для выбранного профиля")

        service = MergeService(profile)
        try:
            result = service.merge(
                target=target,
                donor=donor,
                dry_run=True,
                field_overrides=field_overrides,
                user=request.user if request.user.is_authenticated else None,
                extra_summary=_build_extra_summary(notes, origin="preview"),
                context={"notes": notes} if notes else None,
            )
        except MergeValidationError as exc:
            return _error_response(str(exc))
        except Exception:  # pragma: no cover - defensive logging
            logger.exception("Merge preview failed")
            return _error_response("Не удалось подготовить предпросмотр объединения")

        if getattr(result, "audit_record", None):
            try:
                result.audit_record.delete()
            except Exception:  # pragma: no cover - best effort cleanup
                logger.debug("Failed to delete dry-run audit record", exc_info=True)
            result.audit_record = None

        payload = _build_result_payload(result, profile)
        return _success_response(payload, message="Предпросмотр сформирован")


class MergeExecuteAPIView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request) -> Response:  # noqa: D401
        profile_id = request.data.get("profile")
        target_id = request.data.get("target_id")
        donor_id = request.data.get("donor_id")
        notes = request.data.get("notes")
        field_overrides = request.data.get("field_overrides") or {}
        if not isinstance(field_overrides, dict):
            field_overrides = {}
        dry_run = _to_bool(request.data.get("dry_run"))

        try:
            profile, _ = _resolve_profile(profile_id)
        except ProfileNotFoundError as exc:
            return _error_response(str(exc))

        Model = profile.get_model_class()
        target_pk = _parse_pk(Model, target_id)
        donor_pk = _parse_pk(Model, donor_id)

        if target_pk is _MISSING or donor_pk is _MISSING:
            return _error_response("Для запуска объединения укажите приёмника и донора")
        if str(target_pk) == str(donor_pk):
            return _error_response("Приёмник и донор не могут совпадать")

        try:
            target = Model.objects.get(pk=target_pk)
            donor = Model.objects.get(pk=donor_pk)
        except ObjectDoesNotExist:
            return _error_response("Не удалось найти указанные объекты для выбранного профиля")

        service = MergeService(profile)
        try:
            result = service.merge(
                target=target,
                donor=donor,
                dry_run=dry_run,
                field_overrides=field_overrides,
                user=request.user if request.user.is_authenticated else None,
                extra_summary=_build_extra_summary(notes, origin="merge"),
                context={"notes": notes} if notes else None,
            )
        except MergeValidationError as exc:
            return _error_response(str(exc))
        except Exception:  # pragma: no cover - defensive logging
            logger.exception("Merge execution failed")
            return _error_response("Не удалось выполнить объединение — обратитесь к разработчикам")

        if not result.dry_run:
            try:
                result.target.refresh_from_db()
            except Exception:  # pragma: no cover - defensive
                logger.debug("Failed to refresh target after merge", exc_info=True)
            try:
                result.donor.refresh_from_db()
            except Exception:  # pragma: no cover - defensive
                logger.debug("Failed to refresh donor after merge", exc_info=True)

        payload = _build_result_payload(result, profile)
        message = "Выполнен пробный запуск (без изменений)." if result.dry_run else "Объединение выполнено успешно."
        return _success_response(payload, message=message)


__all__ = [
    "MergeProfileListAPIView",
    "MergeProfileDetailAPIView",
    "MergeEntitySearchAPIView",
    "MergePreviewAPIView",
    "MergeExecuteAPIView",
]
