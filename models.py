from __future__ import annotations

from django.conf import settings
from django.contrib.contenttypes.fields import GenericForeignKey
from django.contrib.contenttypes.models import ContentType
from django.db import models


class MergeOperation(models.Model):
    """Store audit information about merge executions."""

    STATUS_COMPLETED = "completed"
    STATUS_FAILED = "failed"
    STATUS_DRY_RUN = "dry_run"

    STATUS_CHOICES = (
        (STATUS_COMPLETED, "Completed"),
        (STATUS_FAILED, "Failed"),
        (STATUS_DRY_RUN, "Dry run"),
    )

    profile = models.CharField(max_length=128)
    target_content_type = models.ForeignKey(
        ContentType,
        on_delete=models.PROTECT,
        related_name="merge_manager_target_operations",
    )
    target_object_id = models.CharField(max_length=64)
    target_object = GenericForeignKey("target_content_type", "target_object_id")

    donor_content_type = models.ForeignKey(
        ContentType,
        on_delete=models.PROTECT,
        related_name="merge_manager_donor_operations",
    )
    donor_object_id = models.CharField(max_length=64)
    donor_object = GenericForeignKey("donor_content_type", "donor_object_id")

    initiator = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="merge_operations",
    )
    status = models.CharField(max_length=32, choices=STATUS_CHOICES, default=STATUS_COMPLETED)
    dry_run = models.BooleanField(default=False)
    summary = models.JSONField(default=dict, blank=True)
    message = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ("-created_at",)
        indexes = [
            models.Index(
                fields=("target_content_type", "target_object_id"),
                name="merge_manager_target_idx",
            ),
            models.Index(
                fields=("donor_content_type", "donor_object_id"),
                name="merge_manager_donor_idx",
            ),
            models.Index(fields=("profile", "created_at"), name="merge_manager_profile_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.profile}: {self.target_object_id} <- {self.donor_object_id}"
