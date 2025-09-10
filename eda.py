import pandas as pd

df = pd.read_csv('data1.csv')

print(df.isnull().sum())
# print(df.shape)          # rows, columns
# print(df.columns)        # column names
# print(df.dtypes)         # datatypes
# print(df.head())         # first 5 rows
# print(df.tail())         # last 5 rows
# print(df.sample(5))      # random sample
# print(df.info())         # concise summary
# print(df.describe())     # stats for numeric cols
# print(df.describe(include="object"))  # stats for categorical cols
