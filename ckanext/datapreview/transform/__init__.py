import sys
from ckanext.datapreview.transform.base import *
from ckanext.datapreview.transform.tabular_transform import TabularTransformer

register_transformer({
        "name": "csv",
        "class": TabularTransformer,
        "extensions": ["csv", "tsv", "xls"],
        "mime_types": ["text/csv", "text/comma-separated-values", "application/excel", "application/vnd.ms-excel"]
    })
