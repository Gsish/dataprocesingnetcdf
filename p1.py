import xarray as xr
import pandas as pd
import numpy as np


file = r'D:\sih\2024\01\20240101_prof.nc'


ds = xr.open_dataset(file)

meta = {
    "platform_number": ds["PLATFORM_NUMBER"].values.astype(str),
    "cycle_number": ds["CYCLE_NUMBER"].values,
    "direction": ds["DIRECTION"].values.astype(str),
    "data_mode": ds["DATA_MODE"].values.astype(str),
    "vertical_sampling_scheme": ds["VERTICAL_SAMPLING_SCHEME"].values.astype(str),
    "juld": pd.to_datetime(ds["JULD"].values),
    "latitude": ds["LATITUDE"].values,
    "longitude": ds["LONGITUDE"].values
}


pres = ds["PRES_ADJUSTED"].values
temp = ds["TEMP_ADJUSTED"].values
psal = ds["PSAL_ADJUSTED"].values

n_prof, n_levels = pres.shape


rows = []
for i in range(n_prof):        
    for j in range(n_levels):   
        # if np.isnan(pres[i, j]) or np.isnan(temp[i, j]) or np.isnan(psal[i, j]):
        #     continue
        rows.append({
            "platform_number": meta["platform_number"][i],
            "cycle_number": int(meta["cycle_number"][i]),
            "direction": meta["direction"][i],
            "data_mode": meta["data_mode"][i],
            "vertical_sampling_scheme": meta["vertical_sampling_scheme"][i],
            "juld": meta["juld"][i],
            "latitude": float(meta["latitude"][i]),
            "longitude": float(meta["longitude"][i]),
            "pres": float(pres[i, j]),
            "temp": float(temp[i, j]),
            "psal": float(psal[i, j])
        })


df = pd.DataFrame(rows)
df.to_csv("data1.csv")

print(df.head())
print(df.info())

