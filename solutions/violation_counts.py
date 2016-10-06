from collections import Counter
import json
import os

import dask.bag as db


js = (db.read_text(os.path.join('data', 'inspections', 'inspections*.json.gz'))
      .map(json.loads))


def count_violations(counter, inspection):
    if counter is None:
        counter = Counter()
    violations = inspection['Violations']
    violations_cat = [x['Violation Category'] for x in violations]
    counter.update(violations_cat)
    return counter


def combine_counts(counter_x, counter_y):
    if counter_x is None:
        counter_x = Counter()
    counter_x.update(counter_y)
    return counter_x

violation_types = js.foldby('Name', count_violations, None, combine_counts,
                            None)

# what are the biggest offenders failing to do
sorted(violation_types, key=lambda x: sum(x[1].values()), reverse=True)[:5]
