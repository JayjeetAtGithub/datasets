import time

import pyarrow as pa
import pyarrow.parquet as pq

if __name__ == "__main__":
    table = pq.read_table('128MB.parquet')
    print(table.column(0))
    s = time.time()
    options = pa.ipc.IpcWriteOptions()
    options.compression = 'zstd'
    f = pa.BufferOutputStream()
    writer = pa.ipc.RecordBatchFileWriter(f, table.schema, options=options)
    writer.write(table)
    writer.close()
    e = time.time()
    print(e - s)
