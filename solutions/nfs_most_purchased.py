import pandas as pd
import dask.dataframe as dd

df = dd.read_csv("data/nfs/NFS*.csv").set_index('styr', sorted=True)
food_mapping = pd.read_csv("data/nfs/food_mapping.csv", index_col='minfd')

df_1974 = df.loc[1974]

top_84 = df_1974.minfd.value_counts().nlargest(10).compute()

top = df.minfd.value_counts().nlargest(10).compute()

food_mapping.loc[401]
food_mapping.loc[25201]
food_mapping.loc[27001]
