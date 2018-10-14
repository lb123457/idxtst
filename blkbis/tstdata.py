import logging
import datetime
from datetime import datetime
import pandas as pd
import numpy as np
import random
import string


import blkbis.dates

from blkbis import blkidx

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


    def show_info(self):
        logging.debug('')
        print('Index Name       = ' + self.name)
        print('Start Date       = ' + str(self.start_date))
        print('End Date         = ' + str(self.end_date))
        print('Data Frequency   = ' + self.data_frequency)
        print('Refix Frequency  = ' + self.refix_frequency)


    def build_index(self):
        logging.info('Building index...')
        if not hasattr(self, 'name'):
            self.__init__()
        self.show_info()
        dates = blkbis.dates.create_date_series(self.start_date,
                                                self.end_date,
                                                self.data_frequency,
                                                self.refix_frequency,
                                                self.first_refix)

        hist = []

        # Loops over each industry and dates

        for date in dates:
            logging.debug('Building single period data for date %s', date)
            df = self.single_period_data()
            df['date'] = date
            hist.append(df)

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

    # Creates sample data
    test_data = HistoricalSampleData('Test Index')
    idx = test_data.build_index()

    print(idx.idxdata['sector'].unique())
    print(idx)

    df = idx.idxdata
    #df.reset_index(inplace=True)
    print(df)
    df['index2'] = df.index
    #df.set_index(['date', 'index'], inplace=True)
    print(df)
    df = df[df['index'] == 0]
    df2 = df.pivot(index=['date', 'index2'])
    print(df2)


