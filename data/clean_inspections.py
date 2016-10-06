"""
The CSV files were obtained from

http://opendatadc.org/dataset/restaurant-inspection-data
"""

import pandas as pd

restaurants = pd.read_csv("./restaurants.csv")
violations = pd.read_csv("./violations.csv", na_values=['None'])
violation_keys = pd.read_csv("violations_key.csv")

violations = violations.merge(violation_keys, on='Violation Number')
# get these row oriented
# so the Violations columns is a list of json objects
violations = (violations.groupby('Inspection ID').
              apply(lambda df: df.filter(regex="^(?!Inspection)", axis=1)
                    .to_dict(orient='records')).
              reset_index().
              rename(columns={0: 'Violations'}))

inspections = pd.read_csv("inspections.csv", parse_dates=['Date'])

dta = (inspections.merge(violations, on="Inspection ID").
       merge(restaurants, on=['Permit ID']))


def to_json(df):
    year = df.iloc[0].Date.year
    month = df.iloc[0].Date.month
    filename = 'inspections/inspections-{}-{:02d}.json'.format(year, month)

    with open(filename, 'w') as fout:
        for idx, row in df.iterrows():
            fout.write(row.to_json(date_format='iso') + '\n')

dta.groupby((dta.Date.dt.year, dta.Date.dt.month)).apply(to_json)
