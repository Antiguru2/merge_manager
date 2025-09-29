from django.apps import AppConfig


class MergeManagerConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'merge_manager'
    verbose_name = 'Merge Manager'

    def ready(self) -> None:
        # Local import to avoid touching registry before Django is ready.
        from .config import load_profiles_from_settings

        load_profiles_from_settings()
