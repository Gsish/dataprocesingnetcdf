import xarray as xr

file = r'D:\sih\2902086\2902086_Rtraj.nc'


ds = xr.open_dataset(file)


print(ds.attrs["history"])