"""Core merge service implementation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from django.contrib.contenttypes.models import ContentType
from django.db import models as dj_models
from django.db import transaction

from ..config import MergeProfile, registry
from ..exceptions import MergeValidationError
from ..settings import merge_manager_settings

STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"
STATUS_DRY_RUN = "dry_run"

_MISSING = object()


@dataclass(slots=True)
class MergeResult:
    profile: MergeProfile
    target: dj_models.Model
    donor: dj_models.Model
    dry_run: bool
    changed_fields: dict[str, dict[str, Any]] = field(default_factory=dict)
    relations: dict[str, Any] = field(default_factory=dict)
    soft_delete: dict[str, Any] = field(default_factory=dict)
    hard_delete: dict[str, Any] = field(default_factory=dict)
    status: str = STATUS_COMPLETED
    error: str | None = None
    audit_record: Any | None = None


class MergeService:
    """Service that executes merge operations for a particular profile."""

    def __init__(self, profile: MergeProfile | str) -> None:
        if isinstance(profile, MergeProfile):
            self.profile = profile
        else:
            self.profile = registry.get(profile)

    def merge(
        self,
        target: dj_models.Model,
        donor: dj_models.Model,
        *,
        user: Any | None = None,
        dry_run: bool | None = None,
        field_overrides: Mapping[str, Any] | None = None,
        extra_summary: Mapping[str, Any] | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> MergeResult:
        dry_run = merge_manager_settings.DRY_RUN_DEFAULT if dry_run is None else dry_run
        overrides = dict(field_overrides or {})
        base_context = dict(context or {})
        base_context.update({
            'profile': self.profile,
            'dry_run': dry_run,
        })

        self._validate_instances(target, donor)

        result = MergeResult(
            profile=self.profile,
            target=target,
            donor=donor,
            dry_run=dry_run,
        )

        try:
            if dry_run:
                self._execute(target, donor, result, overrides, base_context)
            else:
                with transaction.atomic():
                    self._execute(target, donor, result, overrides, base_context)
        except Exception as exc:  # pragma: no cover - re-raise after audit logging
            result.status = STATUS_FAILED
            result.error = str(exc)
            self._write_audit(result, user=user, extra_summary=extra_summary)
            raise
        else:
            result.status = STATUS_DRY_RUN if dry_run else STATUS_COMPLETED
            self._write_audit(result, user=user, extra_summary=extra_summary)
            return result

    def _execute(
        self,
        target: dj_models.Model,
        donor: dj_models.Model,
        result: MergeResult,
        overrides: Mapping[str, Any],
        context: Mapping[str, Any],
    ) -> None:
        for hook in self.profile.pre_merge_hooks:
            hook(target=target, donor=donor, context=context)

        result.changed_fields = self._apply_field_updates(
            target,
            donor,
            overrides,
            result.dry_run,
            context,
        )
        result.relations = self._transfer_relations(target, donor, result.dry_run)
        hard_delete_enabled = getattr(self.profile, 'hard_delete', False)
        if hard_delete_enabled:
            result.soft_delete = {
                'applied': False,
                'reason': 'hard_delete_enabled',
                'dry_run': result.dry_run,
            }
        else:
            result.soft_delete = self._apply_soft_delete(donor, result.dry_run)

        for hook in self.profile.post_merge_hooks:
            hook(target=target, donor=donor, result=result, context=context)

        if hard_delete_enabled:
            result.hard_delete = {
                'enabled': True,
                'applied': False,
                'dry_run': result.dry_run,
            }
            if not result.dry_run:
                donor_pk = getattr(donor, 'pk', None)
                donor.delete()
                result.hard_delete.update({
                    'applied': True,
                    'pk': str(donor_pk) if donor_pk is not None else None,
                })
        else:
            result.hard_delete = {
                'enabled': False,
                'applied': False,
                'dry_run': result.dry_run,
            }

    def _validate_instances(self, target: dj_models.Model, donor: dj_models.Model) -> None:
        model_class = self.profile.get_model_class()
        if not isinstance(target, model_class):
            raise MergeValidationError(
                f"Target instance must be {model_class.__name__}, got {type(target).__name__}"
            )
        if not isinstance(donor, model_class):
            raise MergeValidationError(
                f"Donor instance must be {model_class.__name__}, got {type(donor).__name__}"
            )

    def _apply_field_updates(
        self,
        target: dj_models.Model,
        donor: dj_models.Model,
        overrides: Mapping[str, Any],
        dry_run: bool,
        context: Mapping[str, Any],
    ) -> dict[str, dict[str, Any]]:
        changes: dict[str, dict[str, Any]] = {}
        update_fields: list[str] = []

        for field_name, rule in self.profile.fields.items():
            target_value = getattr(target, field_name)
            donor_value = getattr(donor, field_name)
            override_value = overrides.get(field_name, _MISSING)

            if override_value is not _MISSING:
                new_value = override_value
                source = 'override'
            else:
                strategy_callable = rule.get_callable()
                strategy_context = dict(context)
                strategy_context['field'] = field_name
                strategy_context['rule'] = rule
                if rule.metadata:
                    strategy_context['metadata'] = dict(rule.metadata)
                    for meta_key, meta_value in rule.metadata.items():
                        strategy_context.setdefault(meta_key, meta_value)
                new_value = strategy_callable(target_value, donor_value, strategy_context)
                source = 'strategy'

            if new_value != target_value:
                changes[field_name] = {
                    'from': _serialize_value(target_value),
                    'to': _serialize_value(new_value),
                    'source': source,
                    'donor': _serialize_value(donor_value),
                }
                if not dry_run:
                    setattr(target, field_name, new_value)
                    update_fields.append(field_name)

        if not dry_run and update_fields:
            target.save(update_fields=update_fields)

        return changes

    def _transfer_relations(
        self,
        target: dj_models.Model,
        donor: dj_models.Model,
        dry_run: bool,
    ) -> dict[str, Any]:
        relations_info: dict[str, Any] = {}
        processed_through: set[Any] = set()

        # Direct many-to-many fields defined on the model.
        for field in donor._meta.many_to_many:
            manager = getattr(donor, field.name)
            target_manager = getattr(target, field.name)
            related_pks = list(manager.values_list('pk', flat=True))
            relations_info[field.name] = {
                'type': 'many_to_many',
                'count': len(related_pks),
                'related_model': field.remote_field.model._meta.label,
            }
            if not dry_run and related_pks:
                target_manager.add(*related_pks)
                manager.remove(*related_pks)
            processed_through.add(field.remote_field.through)

        # Auto-created relations (reverse FK / reverse M2M).
        for relation in donor._meta.get_fields():
            if not relation.auto_created or relation.concrete:
                continue

            if relation.many_to_many:
                through = relation.through
                if through in processed_through:
                    continue
                accessor = relation.get_accessor_name()
                donor_manager = getattr(donor, accessor)
                target_manager = getattr(target, accessor)
                related_pks = list(donor_manager.values_list('pk', flat=True))
                relations_info[accessor] = {
                    'type': 'many_to_many_reverse',
                    'count': len(related_pks),
                    'related_model': relation.related_model._meta.label,
                }
                if not dry_run and related_pks:
                    target_manager.add(*related_pks)
                    donor_manager.remove(*related_pks)
                processed_through.add(through)
                continue

            if relation.one_to_many and hasattr(relation, 'field'):
                accessor = relation.get_accessor_name()
                manager = getattr(donor, accessor)
                queryset = manager.all()
                count = queryset.count()
                relations_info[accessor] = {
                    'type': 'one_to_many',
                    'count': count,
                    'related_model': relation.related_model._meta.label,
                }
                if not dry_run and count:
                    queryset.update(**{relation.field.name: target})

        return relations_info

    def _apply_soft_delete(self, donor: dj_models.Model, dry_run: bool) -> dict[str, Any]:
        field_name = self.profile.get_soft_delete_field()
        if not field_name:
            return {'applied': False, 'reason': 'soft_delete_disabled'}

        if not hasattr(donor, field_name):
            return {
                'applied': False,
                'reason': 'field_missing',
                'field': field_name,
            }

        previous_value = getattr(donor, field_name)
        new_value = self.profile.get_soft_delete_value()
        info = {
            'applied': True,
            'field': field_name,
            'from': _serialize_value(previous_value),
            'to': _serialize_value(new_value),
            'dry_run': dry_run,
        }

        if dry_run:
            return info

        setattr(donor, field_name, new_value)
        donor.save(update_fields=[field_name])
        return info

    def _write_audit(
        self,
        result: MergeResult,
        *,
        user: Any | None,
        extra_summary: Mapping[str, Any] | None = None,
    ) -> None:
        audit_model = merge_manager_settings.AUDIT_MODEL
        if audit_model is None:
            return

        target = result.target
        donor = result.donor

        target_ct = ContentType.objects.get_for_model(target, for_concrete_model=False)
        donor_ct = ContentType.objects.get_for_model(donor, for_concrete_model=False)

        summary = {
            'fields': result.changed_fields,
            'relations': result.relations,
            'soft_delete': result.soft_delete,
            'hard_delete': result.hard_delete,
        }
        if extra_summary:
            summary['extra'] = {key: _serialize_value(value) for key, value in extra_summary.items()}

        audit_kwargs = {
            'profile': result.profile.label,
            'target_content_type': target_ct,
            'target_object_id': str(getattr(target, 'pk', '')),
            'donor_content_type': donor_ct,
            'donor_object_id': str(getattr(donor, 'pk', '')),
            'status': result.status,
            'dry_run': result.dry_run,
            'summary': summary,
            'message': result.error or '',
        }
        if hasattr(audit_model, 'initiator') and user is not None:
            audit_kwargs['initiator'] = user
        elif user is not None and 'initiator' in [field.name for field in audit_model._meta.fields]:
            audit_kwargs['initiator'] = user

        result.audit_record = audit_model.objects.create(**audit_kwargs)


def _serialize_value(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)
