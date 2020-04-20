# %%
# import
import dask
import dask.array as da
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
from matplotlib import pyplot as plt

# %%
# single-machine scheduler
raw_df = dd.read_csv(
    "../data/nyc_taxi.csv",
    blocksize=256e6,  # 256MB chunks
    )

dist = raw_df['trip_distance'].to_dask_array()
hist, bins = da.histogram(dist, bins=100, range=[0, 20])

# hist.visualize()

# %%
# run!
hist_arr = hist.compute()

# %%
# plot
width = 0.7 * (bins[1] - bins[0])
center = (bins[:-1] + bins[1:]) / 2
plt.bar(center, hist_arr, align='center', width=width)
plt.show()

# %%
# local cluster
cluster = LocalCluster(
    n_workers=2, 
    threads_per_worker=2,
    memory_limit=512e6)
client = Client(cluster)


# %%
