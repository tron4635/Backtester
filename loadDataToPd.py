import pandas as pd

reread = pd.read_hdf('./Results/HDF5/FxTickData.h5')

reread.head(n=10)