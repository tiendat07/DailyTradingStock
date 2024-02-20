import pandas as pd
import os

df = pd.read_csv(f'{os.getenv("AIRFLOW_HOME")}/Data/date.csv')
print(df)

df.to_csv(f'{os.getenv("AIRFLOW_HOME")}/Data/date11.csv', index=False)