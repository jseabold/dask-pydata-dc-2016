import json
import os
import re

import dask.bag as db

js = (db.read_text(os.path.join('data', 'inspections', 'inspections*.json.gz'))
      .map(json.loads))


def is_complaint(record):
    visit = record['Inspection Type']

    if visit is None:
        return

    return re.search('compl', visit, flags=re.IGNORECASE)


# the first few partitions are empty, so we need to raise the number
# we look in
js.filter(is_complaint).take(5, npartitions=6)
