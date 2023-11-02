CSVD_HOST = 'http://localhost:3737'

import pandas as pd
import numpy as np
import requests
import random
import string
import threading

tables = []

n_rows = 10

def write_table(name):
    my_data = pd.DataFrame(
        np.random.randn(n_rows, 1),
        columns=["value"],
        index=pd.date_range("20130101", periods=n_rows, freq="T"),
    )
    print('writing data to %s...' % name)
    requests.post(CSVD_HOST+'/tables/'+name, my_data.to_csv(header=True))
    tables.append(name)


def read_table(name):
    print('reading data from %s...' + name)
    df = pd.read_csv(CSVD_HOST+'/tables/'+name)

threads = []

for i in range(0, 10):
    name = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(16))
    t = threading.Thread(target=write_table, args=(name,))
    threads.append(t)
    t.start()

    # if len(tables) > 0:
    #     t2 = threading.Thread(target=read_table, args=(random.choice(tables),))
    #     threads.append(t2)
    #     t2.start()
    #     t3 = threading.Thread(target=read_table, args=(random.choice(tables),))
    #     threads.append(t3)
    #     t3.start()

for t in threads:
    t.join()
