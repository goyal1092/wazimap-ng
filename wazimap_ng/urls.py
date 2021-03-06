from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
from django.urls import path, re_path
from django.conf.urls import include
from django.views.decorators.cache import cache_page

from django.views.generic.base import RedirectView

from .datasets import views as dataset_views
from .points import views as points_views
from .boundaries import views as boundaries_views
from .general import views as general_views


cache = cache_page(60*60)

urlpatterns = [

    path("admin/", admin.site.urls),
    re_path(r"admin$", RedirectView.as_view(url="admin/", permanent=True)),
    path("api/v1/datasets/", dataset_views.DatasetList.as_view(), name="dataset"),
    path(
        "api/v1/datasets/<int:dataset_id>/indicators/",
        cache(dataset_views.DatasetIndicatorsList.as_view()),
        name="dataset-indicator-list",
    ),
    path(
        "api/v1/indicators/",
        cache(dataset_views.IndicatorsList.as_view()),
        name="indicator-list",
    ),
    path(
        "api/v1/indicators/<int:indicator_id>/",
        cache(dataset_views.IndicatorDataView.as_view()),
        name="indicator-data-view",
    ),
    path(
        "api/v1/indicators/<int:indicator_id>/geographies/<str:geography_code>/",
        cache(dataset_views.IndicatorDataView.as_view()),
        name="indicator-data-view-geography",
    ),
    path("api/v1/profiles/", dataset_views.ProfileList.as_view(), name="profile-list"),
    path(
        "api/v1/profiles/<int:pk>/",
        cache(dataset_views.ProfileDetail.as_view()),
        name="profile-detail",
    ),
    path(
        "api/v1/profiles/<int:profile_id>/geographies/<str:geography_code>/",
        cache(dataset_views.profile_geography_data),
        name="profile-geography-data",
    ),
    path("api/v1/geography/search/", cache(dataset_views.search_geography)),
    path("api/v1/geography/ancestors/<str:geography_code>/", cache(dataset_views.geography_ancestors), name="geography-ancestors"),
    path(
        "api/v1/points/", 
        cache(points_views.LocationList.as_view()),
        name="points"
    ),

    path("api/v1/points/themes/", cache(points_views.ThemeList.as_view())),
    path("api/v1/points/themes/<int:theme_id>/", cache(points_views.LocationList.as_view())),
    path("api/v1/points/categories/", cache(points_views.CategoryList.as_view())),

    path(
        "api/v1/points/categories/<int:category_id>/",
        cache(points_views.LocationList.as_view()),
        name="point_categories"
    ),

    re_path(r"^$", RedirectView.as_view(url="/api/v1/datasets/", permanent=False)),

    path(
        "api/v1/boundaries/",
        cache(boundaries_views.GeographyList.as_view()),
        name="boundaries"
    ),

    path(
        "api/v1/boundaries/<str:code>/",
        cache(boundaries_views.GeographyItem.as_view()),
        name="boundaries-code"
    ),

    path(
        "api/v1/boundaries/<str:code>/children/",
        cache(boundaries_views.GeographyChildren.as_view()),
        name="boundaries-children"
    ),
    #path("api/v1/boundaries/provinces/", boundaries_views.provinces),

    path(
        "api/v1/all_details/profile/<int:profile_id>/geography/<str:code>/",
        cache(general_views.consolidated_profile),
        name="all-details"
    ),

    path(
        "api/v1/all_details/profile/<int:profile_id>/geography/<str:code>/test/",
        cache(general_views.consolidated_profile_test),
        name="all-details-test"
    ),

] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

if settings.DEBUG:
    import debug_toolbar
    urlpatterns = [
        path('__debug__/', include(debug_toolbar.urls)),

    ] + urlpatterns
