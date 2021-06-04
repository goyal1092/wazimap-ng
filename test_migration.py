import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "wazimap_ng.config")
os.environ.setdefault("DJANGO_CONFIGURATION", "Local")

import configurations
configurations.setup()

import pandas as pd

from wazimap_ng.datasets.models import DatasetData, Dataset
from django.db.models import Count


data = [
    [
        "dataset_id", "group_name", "no of groups", "used group id", "to be deleted group ids",
        "old subindicators", "new subindicators", "datset data groups",
        "all datset data groups present in new subindicators", "ordered",
    ]
]

for dataset in Dataset.objects.all():

   

    groups = dataset.group_set.all()
    for group in groups.values("name").annotate(gc=Count("name")):
        d = [dataset.id]
        d.append(group["name"])
        d.append(group["gc"])

        filtered_group = groups.filter(name=group["name"]).order_by("-updated")
        last_updated_group = filtered_group.first()
        d.append(last_updated_group.id)

        if group["gc"] > 1:
            d.append(
                list(filtered_group.exclude(id=last_updated_group.id).values_list("id", flat=True))
            )
        else:
            d.append("[]")

        d.append(last_updated_group.subindicators)

        # Fetch list of subindicator from dataset data
        subindicators = list(
            DatasetData.objects.filter(dataset=dataset)
            .order_by()
            .values_list(F"data__{group['name']}", flat=True)
            .distinct()
        )

        datasetdata_groups = subindicators

        # Sort subindicators acording to last updated groups subindicator

        sub_groups = last_updated_group.subindicators
        sorted_list = sorted(
            subindicators, key=lambda x: sub_groups.index(x) if x in sub_groups else len(subindicators)
        )

        d.append(sorted_list)
        d.append(subindicators)

        all_present = True

        for s in subindicators:
            if s not in sorted_list:
                all_present = False
                break

        d.append(all_present)

        old_subindicators = []

        for o in last_updated_group.subindicators:
            if o in subindicators:
                old_subindicators.append(o)

        d.append(sorted_list == old_subindicators)

        data.append(d)

my_df = pd.DataFrame(data)
my_df.to_csv('test_migration.csv', index=True, header=True)
