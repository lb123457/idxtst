import logging
import datetime
from datetime import datetime
import pandas as pd
import numpy as np
import blkbis.dates

_DEFAULT_NAME = 'Synthetic sample index'
_SECTORS = ['FIN', 'UTIL', 'TELE']
_DEFAULT_START_DATE = '20180101'
_DEFAULT_END_DATE = '20180802'
_DEFAULT_FREQUENCY = 'MONTHLY'
_DEFAULT_REFIX__FREQUENCY = 'ANNUAL'

_DEFAULT_NUMBER_FIRMS = 1000
_NUMBER_FIRMS_SIGMA = 10
_DEFAULT_FIRM_SIZE_MM = 1000
_MIN_FIRM_SIZE_MM = 50
_FIRM_SIZE_SIGMA = 500


class HistoricalSampleData:

    'Sample class that creates historical index for performance testing'

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
        for sector in _SECTORS:
            logging.info('Building universe for sector %s', sector)
            for date in dates:
                df = pd.DataFrame(np.random.normal(_DEFAULT_FIRM_SIZE_MM, _FIRM_SIZE_SIGMA, _DEFAULT_NUMBER_FIRMS), columns=['size_mm'])

                clip_size = lambda x: max(x, _MIN_FIRM_SIZE_MM)
                df['size_mm'] = df['size_mm'].transform(clip_size)

                df['date'] = date
                df['sector'] = sector
                print(df)
                hist.append(df)

        result = pd.concat(hist)

        return result



    def dummy_function():
        print('d')