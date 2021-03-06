import sys
import logging
import pandas as pd

# Logging setup
logger = logging.getLogger()


from blkbis import blkidx
from blkbis import tstdata
from blkbis import qc

ids = tstdata.create_random_universe()
sectors = tstdata.pick_random_sectors()

df = pd.DataFrame({'id': ids, 'sector': sectors})

print(df)

qcChk = qc.ValueQCCkeck('test', 'This is a dummy test')
print(qcChk)



logger.info('Starting test...')

if __name__ == "__main__":

    test_index = tstdata.HistoricalSampleData('Test Index')
    test_index.show_info()

    idx = test_index.build_index()

    idx = blkidx.BlkIdx('Top level index', description='This is a dummy index')
    print(idx)

    idx = blkidx.BlkEQIdx('Equity index', description='This is a dummy equity index')
    print(idx)

    qc.full_qc(idx)
