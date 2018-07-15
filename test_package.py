import sys
import logging


from blkbis import blkidx
from blkbis import tstdata


# Logging setup
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s:%(name)s:%(funcName)s:%(levelname)s: %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


logger.info('Starting test...')

if __name__ == "__main__":

    idx = blkidx.BlkIdx('Top level index', description='This is a dummy index')
    idx.print()

    idx = blkidx.BlkEQIdx('Equity index', description='This is a dummy equity index')
    idx.print()

    test_index = tstdata.HistoricalSampleData('Test Index')
    test_index.show_info()
    test_index.build_index()


