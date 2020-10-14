import os
import pandas as pd
import logging

from django.db import transaction
from django.conf import settings

from ..dataloader import loaddata
from .. import models

from wazimap_ng.general.services.permissions import assign_perms_to_group

logger = logging.getLogger(__name__)


@transaction.atomic
def process_uploaded_file(dataset_file, dataset, **kwargs):
    logger.debug(f"process_uploaded_file: {dataset_file}")
    """
    Run this Task after saving new document via admin panel.

    Read files using pandas according to file extension.
    After reading data convert to list rather than using numpy array.

    Get header index for geography & count and create Result objects.
    """
    def process_file_data(df, dataset, row_number):
        df = df.applymap(lambda s: s.strip().capitalize() if type(s) == str else s)
        datasource = (dict(d[1]) for d in df.iterrows())
        return loaddata(dataset, datasource, row_number)

    filename = dataset_file.document.name
    chunksize = getattr(settings, "CHUNK_SIZE_LIMIT", 1000000)
    logger.debug(f"Processing: {filename}")

    new_columns = None
    error_logs = []
    warning_logs = []
    row_number = 1

    if ".csv" in filename:
        logger.debug(f"Processing as csv")
        df = pd.read_csv(dataset_file.document.open(), nrows=1, dtype=str, sep=",")
        old_columns = df.columns.str.lower().str.strip()
        df.drop(df.columns[df.columns.str.contains('unnamed',case = False)],axis = 1, inplace = True)
        new_columns = df.columns.str.lower().str.strip()

        for df in pd.read_csv(
            dataset_file.document.open(), chunksize=chunksize, dtype=str,
            sep=",", header=None, skiprows=1
        ):
            df.dropna(how='all', axis='index', inplace=True)
            df.columns = old_columns
            df = df.loc[:, new_columns]

            df.fillna('', inplace=True)
            errors, warnings = process_file_data(df, dataset, row_number)
            error_logs = error_logs + errors
            warning_logs = warning_logs + warnings
            row_number = row_number + chunksize
    else:
        logger.debug("Process as other filetype")
        skiprows = 1
        i_chunk = 0
        df = pd.read_excel(dataset_file.document.open(), nrows=1, dtype=str)
        old_columns = df.columns.str.lower().str.strip()
        df.drop(df.columns[df.columns.str.contains('unnamed',case = False)],axis = 1, inplace = True)
        new_columns = df.columns.str.lower().str.strip()
        while True:
            df = pd.read_excel(
                dataset_file.document.open(), nrows=chunksize, skiprows=skiprows, header=None,
                dtype=str
            )
            skiprows += chunksize
            # When there is no data, we know we can break out of the loop.
            if not df.shape[0]:
                break
            else:
                df.dropna(how='all', axis='index', inplace=True)
                df.columns = old_columns
                df = df.loc[:, new_columns]
                df.fillna('', inplace=True)
                errors, warnings = process_file_data(df, dataset, row_number)
                error_logs = error_logs + errors
                warning_logs = warning_logs + warnings
                row_number = row_number + chunksize
            i_chunk += 1

    groups = [group for group in new_columns.to_list() if group not in ["geography", "count"]]

    dataset.groups = list(set(groups + dataset.groups))
    dataset.save()

    if error_logs:
        logger.error(error_logs)
        logdir = settings.MEDIA_ROOT + "/logs/dataset/errors/"
        if not os.path.exists(logdir):
            os.makedirs(logdir)
        logfile = logdir + "%s_%d_error_log.csv" % (dataset.name.replace(" ", "_"), dataset_file.id)
        df = pd.DataFrame(error_logs)
        df.to_csv(logfile, header=["Line Number", "Field Name", "Error Details"], index=False)
        error_logs = logfile

    if warning_logs:
        logdir = settings.MEDIA_ROOT + "/logs/dataset/warnings/"
        if not os.path.exists(logdir):
            os.makedirs(logdir)
        logfile = logdir + "%s_%d_warnings.csv" % (dataset.name.replace(" ", "_"), dataset.id)
        df = pd.DataFrame(warning_logs)
        df.to_csv(logfile, header=new_columns, index=False)
        warning_logs = logfile

    return {
        "model": "datasetfile",
        "name": dataset.name,
        "id": dataset_file.id,
        "dataset_id": dataset.id,
        "error_log": error_logs or None,
        "warning_log": warning_logs or None
    }
