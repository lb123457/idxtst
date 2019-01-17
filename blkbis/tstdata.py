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


logger = logging.getLogger(__name__)


def create_random_id(**kwargs):
    return (''.join([random.choice(string.ascii_letters) for n in range(7)])).upper()


def create_random_universe(n=1000, **kwargs):
    l = []
    for n in range(n):
        l.append(create_random_id())
    return l


def pick_random_sector():
    logger.info('Hey')

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






class HistoricalSampleData:

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

        logging.info(str(self))


    def __str__(self):
        return 'Index Name       = ' + self.name + 'Start Date       = ' + str(self.start_date) + 'End Date         = ' + str(self.end_date) + 'Data Frequency   = ' + self.data_frequency + 'Refix Frequency  = ' + self.refix_frequency



    def build_index(self):
        logging.info('Building index...')
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
            logging.debug('Building single period data for date %s', date)
            df = self.single_period_data()

            df2 = pd.DataFrame(columns=['id', 'sector'])
            df2['id'] = pd.Series(master_list_ids)
            df2['sector'] = pd.Series(master_list_sectors)

            import random

            def some(x, n):
                return x.ix[random.sample(x.index.tolist(), n)]

            df2['date'] = date
            hist.append(some(df2, 10))

        result = pd.concat(hist)

        idx = blkidx.BlkIdx(self.name, dataframe=result)

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

    df = pd.DataFrame(np.random.randint(0, 5, size=(20, 1)), columns=['cusip_index'])
    df['value'] = np.random.randn(20)

    df['effective_date'] = df['cusip_index'].apply(lambda x: randomDate("1/1/2008", "1/1/2029"))

    cusip_map = somefunction(lambda a: ''.join(random.choices(string.ascii_uppercase + string.digits, k=9)), range(5))

    df['cusip'] = df['cusip_index'].map(cusip_map)

    df.sort_values(['cusip', 'effective_date'], inplace=True)

    df[['next_effective_date', 'next_cusip']] = df.shift(-1)[['effective_date', 'cusip']]

    df['next_effective_date'] = np.where(df['cusip'] != df['next_cusip'], pd.to_datetime('21001231', format='%Y%m%d'),
                                         df['next_effective_date'])

    df['effective_date'] = pd.to_datetime(df['effective_date'])
    df['next_effective_date'] = pd.to_datetime(df['next_effective_date'])

    df.sort_values(['cusip', 'effective_date'], inplace=True)

    df.drop(columns=['next_cusip', 'cusip_index'], axis=1, inplace=True)
    df = df.reset_index(drop=True)
    df = df.reindex_axis(sorted(df.columns), axis=1)

    df = df.reindex(columns=(['value'] + list([a for a in df.columns if a != 'value'])))

    df

    # Creates sample data
    test_data = HistoricalSampleData('Test Index')
    idx = test_data.build_index()

    print(idx.idxdata['sector'].unique())
    print(idx)


    # Now creates a report of what goes in and out at each period

    df =  idx.idxdata

    qc_engine = qc.HistoricalPanelQCCheck(id='TEST', description='This is a test')
    qc_engine.run_check(df)

