import sys
from ckanext.datapreview.transform.base import *

from ckanext.datapreview.transform.csv_transform import CSVTransformer
from ckanext.datapreview.transform.xls_transform import XLSTransformer
from ckanext.datapreview.transform.plain_transform import PlainTransformer

register_transformer({
        "name": "xls",
        "class": XLSTransformer,
        "extensions": ["xls"],
        "mime_types": ["application/excel", "application/vnd.ms-excel"]
    })

register_transformer({
        "name": "csv",
        "class": CSVTransformer,
        "extensions": ["csv", "tsv"],
        "mime_types": ["text/csv", "text/comma-separated-values"]
    })

register_transformer({
        "name": "plain",
        "class": PlainTransformer,
        "extensions": [".*"],
        "mime_types": ["*/*"]
    })
