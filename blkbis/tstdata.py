import os
import sys
import logging
import datetime
from datetime import datetime
import time
import pandas as pd
import numpy as np
import random
import string
from tabulate import tabulate


import blkbis.dates

from blkbis import blkidx
from blkbis import qc

_DEFAULT_NAME = 'Synthetic sample index'
_SECTORS = ['FIN', 'UTIL', 'TELE']
_DEFAULT_START_DATE = '20180101'
_DEFAULT_END_DATE = '20180105'
_DEFAULT_FREQUENCY = 'MONTHLY'
_DEFAULT_REFIX__FREQUENCY = 'ANNUAL'

_DEFAULT_NUMBER_FIRMS = 10
_NUMBER_FIRMS_SIGMA = 10
_DEFAULT_FIRM_SIZE_MM = 1000
_MIN_FIRM_SIZE_MM = 50
_FIRM_SIZE_SIGMA = 500


# Logging setup
_logger = logging.getLogger()


def create_random_id(**kwargs):
    return (''.join([random.choice(string.ascii_letters) for n in range(7)])).upper()


def create_random_universe(n=1000, **kwargs):
    l = []
    for n in range(n):
        l.append(create_random_id())
    return l


def pick_random_sector():
    return _SECTORS[random.randint(0, len(_SECTORS)-1)]


def pick_random_sectors(n=1000, **kwargs):
    l = []
    for n in range(n):
        l.append(pick_random_sector())
    return l


'''
Shows how to go from an effective_date to a start_date and end_date
dataset
'''


def strTimeProp(start, end, format, prop):
    """
    Get a time at a proportion of a range of two formatted times.

    start and end should be strings specifying times formated in the
    given format (strftime-style), giving an interval [start, end].
    prop specifies how a proportion of the interval to be taken after
    start.  The returned time will be in the specified format.
    """

    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(format, time.localtime(ptime))
    # return ptime


def randomDate(start, end):
    return strTimeProp(start, end, '%m/%d/%Y', random.random())


# Creates a map between the integers and the cusip
def somefunction(keyFunction, values):
    return dict((v, keyFunction(v)) for v in values)



class HistoricalTstData:

    'Sample class that creates historical index for performance and QC testing'

    def __init__(self,
                 name = _DEFAULT_NAME,
                 start_date = _DEFAULT_START_DATE,
                 end_date = _DEFAULT_END_DATE,
                 data_frequency = _DEFAULT_FREQUENCY,
                 refix_frequency = _DEFAULT_REFIX__FREQUENCY,
                 first_refix=None):

        self.name = name
        self.start_date = datetime.strptime(start_date, '%Y%m%d').date()
        self.end_date = datetime.strptime(end_date, '%Y%m%d').date()
        self.data_frequency = data_frequency
        self.refix_frequency = refix_frequency
        self.first_refix = first_refix

        _logger.info(str(self))


    def __str__(self):
        return 'Index Name       = ' + self.name + 'Start Date       = ' + str(self.start_date) + 'End Date         = ' + str(self.end_date) + 'Data Frequency   = ' + self.data_frequency + 'Refix Frequency  = ' + self.refix_frequency



    def build_index(self):
        _logger.info('Building index...')
        if not hasattr(self, 'name'):
            self.__init__()

        dates = blkbis.dates.create_date_series(self.start_date,
                                                self.end_date,
                                                self.data_frequency,
                                                self.refix_frequency,
                                                self.first_refix)

        master_list_ids = create_random_universe(n=15)
        master_list_sectors = pick_random_sectors(n=15)

        hist = []

        # Loops over each industry and dates

        for date in dates:
            _logger.debug('Building single period data for date %s', date)

            df2 = pd.DataFrame(columns=['id', 'sector'])
            df2['id'] = pd.Series(master_list_ids)
            df2['sector'] = pd.Series(master_list_sectors)

            import random

            def some(x, n):
                return x.ix[random.sample(x.index.tolist(), n)]

            df2['date'] = date
            hist.append(some(df2, 10))

        df = pd.concat(hist)
        df.set_index(['date', 'id'], inplace=True)

        idx = blkidx.BlkIdx(self.name, dataframe=df)

        return idx



    def single_period_data(self):
        l = []
        for sector in _SECTORS:
            print(sector)
            df = pd.DataFrame(np.random.normal(_DEFAULT_FIRM_SIZE_MM, _FIRM_SIZE_SIGMA, _DEFAULT_NUMBER_FIRMS),
                              columns=['size_mm'])

            clip_size = lambda x: max(x, _MIN_FIRM_SIZE_MM)

            df['size_mm'] = df['size_mm'].transform(clip_size)
            df['sector'] = sector
            l.append(df)


        return pd.concat(l)



if __name__ == "__main__":

    _logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(funcName)s:%(levelname)s: %(message)s')
    ch.setFormatter(formatter)
    _logger.addHandler(ch)

    # Creates sample data
    test_data = HistoricalTstData('Test Index')
    idx = test_data.build_index()

    print(idx.idxdata['sector'].unique())
    print(idx)


