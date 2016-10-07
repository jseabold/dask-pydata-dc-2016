import dask.dataframe as dd
from dask.diagnostics import ProgressBar


def average_cons(hhold):
    per_person = hhold['cq'].div(hhold['memhh'])
    daily = per_person.sum() / 7
    return daily


def per_group(partition):
    group = partition.groupby(['minfd'])
    # call reset_index so we don't get a MultiIndex
    result = group.apply(average_cons).reset_index()
    # reset the index to the partition index
    result[partition.index.name] = partition.index[0]
    return result.set_index(partition.index.name)


df = dd.read_csv("data/nfs/NFS*.csv").set_index('styr', sorted=True)

res = df.map_partitions(per_group,
                        meta=[('minfd', float), ('pc_consumption', float)])

with ProgressBar():
    average = res.compute()

average
