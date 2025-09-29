from django.contrib import admin

from .models import MergeOperation


@admin.register(MergeOperation)
class MergeOperationAdmin(admin.ModelAdmin):
    list_display = (
        'profile',
        'target_object_id',
        'donor_object_id',
        'status',
        'dry_run',
        'created_at',
    )
    list_filter = ('status', 'dry_run', 'profile')
    search_fields = (
        'profile',
        'target_object_id',
        'donor_object_id',
        'message',
    )
    readonly_fields = (
        'created_at',
        'updated_at',
    )
    autocomplete_fields = ('initiator',)
