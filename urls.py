from django.urls import path
from django.views.generic import TemplateView

from . import api
from . import views


urlpatterns = [
    path(
        "",
        views.SuperuserRequiredTemplateView.as_view(template_name="merge_manager/merge_interface.html"),
        name="merge-interface",
    ),
    path(
        "api/profiles/",
        api.MergeProfileListAPIView.as_view(),
        name="api-profiles-list",
    ),
    path(
        "api/profiles/<slug:slug>/",
        api.MergeProfileDetailAPIView.as_view(),
        name="api-profiles-detail",
    ),
    path(
        "api/entities/",
        api.MergeEntitySearchAPIView.as_view(),
        name="api-entities",
    ),
    path(
        "api/preview/",
        api.MergePreviewAPIView.as_view(),
        name="api-preview",
    ),
    path(
        "api/merge/",
        api.MergeExecuteAPIView.as_view(),
        name="api-merge",
    ),
]
