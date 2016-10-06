import json
import operator
import os

import dask.bag as db


js = (db.read_text(os.path.join('data', 'inspections', 'inspections*.json.gz'))
      .map(json.loads))


def total_violations(total, records):
    return total + records['Critical Violations']

critical = js.foldby(key='Name',
                     binop=total_violations,
                     initial=0,
                     combine=operator.add,
                     combine_initial=0)

sorted(critical, key=operator.itemgetter(1), reverse=True)[:5]


critical = js.foldby(key=lambda x: (x['Name'], x['Address']),
                     binop=total_violations,
                     initial=0,
                     combine=operator.add,
                     combine_initial=0)

sorted(critical, key=operator.itemgetter(1), reverse=True)[:5]
