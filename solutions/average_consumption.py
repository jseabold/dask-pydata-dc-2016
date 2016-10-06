import dask.dataframe as dd
from dask.diagnostics import ProgressBar


def average_cons(hhold):
    per_person = hhold['cq'].div(hhold['memhh'])
    daily = per_person.sum() / 7
    return daily


def grouper(partition):
    group = partition.groupby(['minfd'])
    # call reset_index so we don't get a MultiIndex
    result = group.apply(average_cons).reset_index()
    # reset the index to the partition index
    result[partition.index.name] = partition.index[0]
    return result.set_index(partition.index.name)


df = dd.read_csv("data/nfs/NFS*.csv")
partitions = list(range(1974, 2001)) + [2000]
df = df.set_partition('styr', divisions=partitions)

res = df.map_partitions(grouper,
                        meta=[('minfd', 'f8'), ('pc_consumption', 'f8')])

with ProgressBar():
    average = res.compute()

average.head()
