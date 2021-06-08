from wazimap_ng.datasets.models import IndicatorData 

from .helpers import MetricCalculator

def get_indicator_data(highlight, geographies, fetch_first=False):
    indicator_data = IndicatorData.objects.filter(indicator__profilehighlight=highlight, geography__in=geographies)

    if fetch_first and indicator_data:
        return indicator_data.first()

    return indicator_data

def absolute_value(highlight, geography):
    data = get_indicator_data(highlight, [geography], True)
    if not data:
        return None
    return MetricCalculator.absolute_value(data.data, highlight, geography)


def subindicator(highlight, geography):
    data = get_indicator_data(highlight, [geography], True)
    if not data:
        return None
    return MetricCalculator.subindicator(data.data, highlight, geography)


def sibling(highlight, geography):
    siblings = list(geography.get_siblings())
    data = get_indicator_data(highlight, [geography] + siblings)
    return MetricCalculator.sibling(data, highlight, geography)

algorithms = {
    "absolute_value": absolute_value,
    "sibling": sibling,
    "subindicators": subindicator
}

def HighlightsSerializer(profile, geography):
    highlights = []

    profile_highlights = profile.profilehighlight_set.all().order_by("order")

    for highlight in profile_highlights:
        denominator = highlight.denominator
        method = algorithms.get(denominator, absolute_value)
        val = method(highlight, geography)

        if val is not None:
            highlights.append({"label": highlight.label, "value": val, "method": denominator})
    return highlights
