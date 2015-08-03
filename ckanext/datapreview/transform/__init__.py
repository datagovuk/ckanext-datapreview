import sys
from ckanext.datapreview.transform.base import *
from ckanext.datapreview.transform.tabular_transform import TabularTransformer

ALLOWED_MIMETYPES = ["text/csv", "text/comma-separated-values", "application/excel", "application/vnd.ms-excel"]

register_transformer({
        "name": "csv",
        "class": TabularTransformer,
        "extensions": ["csv", "tsv", "xls", "ods"],
        "mime_types": ALLOWED_MIMETYPES
    })
