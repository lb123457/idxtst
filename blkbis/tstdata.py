import logging
import datetime
from datetime import datetime
import pandas as pd
import numpy as np
import random
import string
from tabulate import tabulate


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

        master_list_ids = create_random_universe(n=15)
        master_list_sectors = create_random_universe(n=15)

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

    # Creates sample data
    test_data = HistoricalSampleData('Test Index')
    idx = test_data.build_index()

    print(idx.idxdata['sector'].unique())
    print(idx)


    # Now creates a report of what goes in and out at each period

    df =  idx.idxdata

    df_dates = pd.DataFrame(df.date.unique(), columns=['date'])
    df_ids = pd.DataFrame(df.id.unique(), columns=['id'])

    print(df_dates)
    print(df_ids)

    df_dates['dummy'] = 1
    df_ids['dummy'] = 1

    df_ids_by_dates = df_dates.merge(df_ids, on='dummy')

    print(df_ids_by_dates.shape)

    df_coverage = df_ids_by_dates.merge(df, on=['date', 'id'], how='outer')

    print(df_coverage)


    # Sorts the coverage

    df_coverage.sort_values(['id', 'date'], inplace=True)

    df_coverage['next_date'] = df_coverage['date'].shift(-1)
    df_coverage['previous_date'] = df_coverage['date'].shift(1)
    df_coverage['next_id'] = df_coverage['id'].shift(-1)
    df_coverage['previous_id'] = df_coverage['id'].shift(1)
    df_coverage['next_sector'] = df_coverage['sector'].shift(-1)
    df_coverage['previous_sector'] = df_coverage['sector'].shift(1)


    df_coverage['drop_next_period'] = np.where((~df_coverage['sector'].isnull()) & (df_coverage['next_sector'].isnull()) & (df_coverage['next_id'] == df_coverage['id']), True, np.nan)
    df_coverage['enter_this_period'] = np.where((~df_coverage['sector'].isnull()) & (df_coverage['previous_sector'].isnull()) & (df_coverage['previous_id'] == df_coverage['id']), True, np.nan)


    print(df_coverage)



    print(tabulate(df_coverage, headers='keys', tablefmt='psql'))

    print(tabulate(df_coverage.sort_values(['date', 'id']), headers='keys', tablefmt='psql'))

    # Create the summary count of how many securities enter and exit each period
    print(df_coverage.groupby(['date'])['enter_this_period'].count())
    print(df_coverage.groupby(['date'])['drop_next_period'].count())


    # Another way to do the analysis
    groups = []
    names = []

    for name, group in df_coverage.groupby(['date']):
        print(name)
        print(tabulate(group, headers='keys', tablefmt='psql'))
        names.append(name)
        groups.append(group)

    for df in groups[1:-1]:
        print(tabulate(df, headers='keys', tablefmt='psql'))

    i = 1
    while i < len(groups) - 1:
        print('Previous group')
        print(tabulate(groups[i-1], headers='keys', tablefmt='psql'))
        print('Current group')
        print(tabulate(groups[i], headers='keys', tablefmt='psql'))
        print('Next group')
        print(tabulate(groups[i+1], headers='keys', tablefmt='psql'))
        i += 1


