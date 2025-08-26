import requests
import pandas as pd
import numpy as np

HTTPDB_HOST = "http://localhost:3737"

n_rows = 100

df = pd.DataFrame(
    np.random.randn(n_rows, 2),
    columns=["val1", "val2"],
    index=pd.date_range("20130101", periods=n_rows, freq="T"),
)

resp = requests.post(f"{HTTPDB_HOST}/frame", data=df.to_csv())
print(resp.text)
