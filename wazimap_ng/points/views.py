from django.views.decorators.http import condition
from django.utils.decorators import method_decorator

from rest_framework.response import Response
from rest_framework_gis.pagination import GeoJsonPagination
from rest_framework import generics

from . import models
from . import serializers
from ..cache import etag_point_updated, last_modified_point_updated

class CategoryList(generics.ListAPIView):
    queryset = models.Category.objects.all()
    serializer_class = serializers.CategorySerializer

class ThemeList(generics.ListAPIView):
    queryset = models.Theme.objects.all()
    serializer_class = serializers.ThemeSerializer

class LocationList(generics.ListAPIView):
    pagination_class = GeoJsonPagination
    serializer_class = serializers.LocationSerializer
    queryset = models.Location.objects.all().select_related("category")

    def list(self, request, theme_id=None, category_id=None):
        queryset = self.get_queryset()
        if theme_id is not None:
            queryset = queryset.filter(category__theme__pk=theme_id)

        if category_id is not None:
            queryset = queryset.filter(category__pk=category_id)

        serializer = self.get_serializer_class()(queryset, many=True)
        data = serializer.data
        return Response(data)

    @method_decorator(condition(etag_func=etag_point_updated, last_modified_func=last_modified_point_updated))
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)
