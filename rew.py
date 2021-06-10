import time

import pyarrow as pa
import pyarrow.parquet as pq

if __name__ == "__main__":
    table = pq.read_table('128MB.parquet')
    print(table.column(0))
    s = time.time()
    pq.write_table(table, '128MB.snappy.parquet', compression="snappy")
    e = time.time()
    print(e - s)
