"""
QC Package
"""


import os
import sys
import logging
import pandas as pd
import numpy as np
from blkbis import blkidx
from blkbis import tstdata
from tabulate import tabulate

# Logging setup
_logger = logging.getLogger()


'''

A QC is defined by:

- The static description of the model

    * qc_model_type
    * qc_model_type_description
    * qc_model_subtype
    * qc_model_subtype_description
    * qc_application
    
- The static configuration of the model

    * The configuration is model dependent
    * In some cases, the configuration may not exist

- Some information about the data

    * This is nice to have but it may not be complete
    * Examples are the start and end date, the sample size
    
- The results

    * Pass / no pass
    * Level of abnormality
    * ...

'''

# Defines the metadata items that are valid for an item.

__METADATA_ITEMS__ = ['__id__', '__type__', '__type_description__']



def __init__():
    _logger.debug('')


def full_qc(idx):
    _logger.debug('Running full QC')




class QCCheck:

    __id__ = 'BASE'
    __description__ = 'This is a base generic test'

    def __init__(self, **kwargs):
        """
        This is the base class for all QC Checks classes

        :param kwargs:
        """
        pass


    def run_check(self, df, **kwargs):
        """
        Runs the actual check. This is implemented at the leaf class level.

        :param df:
        :param kwargs:
        :return:
        """
        pass


    # Overloads the print statement
    def __str__(self):
        s =  'Id               = ' + self.__id__ + '\n'
        s += 'Description = ' + self.__description__ + '\n'

        return s


    def upload_check_definition(self):
        """
        Saves the check definition in the persistence layer

        :return:
        """
        pass


    def download_check_definition(self):
        """
        Retrieves the check definition from the persistence layer

        :return:
        """
        pass


    def save_check_results(self):
        """
        Saves the results of the QC check.
        This is just a placeholder for now.

        :return:
        """
        pass



class ValueCheck(QCCheck):
    """
    Verifies that all values in a column satisfy a condition defined as a function
    """
    __id__ = 'VALUE_CHECK'
    __description__ = 'This is a base generic test'

    def __init__(self,
                 function=None,
                 **kwargs):
        """
        This is the base class for all QC Checks classes

        :param kwargs:
        """

        if function is None:
            raise ValueError('Please specify a function')
        else:
            self.function = function



    def run_check(self,
                  df,
                  columns=None,
                  **kwargs):
        """
        Takes a dataframe and

        :param df:
        :param kwargs:
        :return:
        """

        for c in columns:
            if False in df[c].apply(self.function):
                df[c + '_value_check'] = df[c].apply(self.function)
                print(tabulate(df[~df[c + '_value_check']], headers='keys', tablefmt='psql'))
                raise ValueError('Dataframe contains values that do not satisfy the condition')


class TimeSeriesQCCheck(QCCheck):
    """
    This is the base class for all Time Series QC Checks classes
    """

    __id__ = 'TIME_SERIES'
    __description__ = 'This check uses a times series to identify anomalous values'

    def __init__(self,
                 id=None,
                 description=None,
                 **kwargs):

        QCCheck.__init__(self, **kwargs)




class CrossSectionalQCCkeck(QCCheck):

    __id__ = 'CROSS_SECTIONAL'
    __description__ = 'This check uses a cross-sectional analysis to identify anomalous values'

    def __init__(self,
                 id=None,
                 description=None,
                 **kwargs):

        QCCheck.__init__(self, id, description, **kwargs)








class XRayQCCheck(QCCheck):

    __id__ = 'XRAY'
    __description__ = 'This check runs a comprehensive set of checks'

    def __init__(self,
                 id=None,
                 description=None,
                 **kwargs):

        QCCheck.__init__(self, id, description, **kwargs)
        


class HistoricalPanelQCCheck(QCCheck):

    __id__ = 'HISTORICALPANEL'
    __description__ = 'This check runs a comprehensive set of checks on an historical panel'

    def __init__(self,
                 id=None,
                 description=None,
                 **kwargs):

        QCCheck.__init__(self, **kwargs)


    def _build_date_universe(self):
        self.__dates__ = pd.DataFrame(self.__df__.date.unique(), columns=['date'])


    def _build_asset_universe(self):
        self.__assets__ = pd.DataFrame(self.__df__.id.unique(), columns=['id'])


    def _build_dates_by_assets(self):
        self.__dates__['dummy'] = 1
        self.__assets__['dummy'] = 1

        self.__dates_by_assets__ = self.__dates__.merge(self.__assets__, on='dummy').drop(columns=['dummy'])


    def run_check(self, df):
        '''
        Given a dataframe df with a date and id columns that can be used as an index, creates
        a report showing the assets that are entering and exiting the universe at each period.

        :param df:
        :return:
        '''

        self.__df__ = df
        self._build_date_universe()
        self._build_asset_universe()
        self._build_dates_by_assets()

        df_coverage = self.__dates_by_assets__.merge(df, on=['date', 'id'], how='outer')

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




if __name__ == "__main__":

    _logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(funcName)s:%(levelname)s: %(message)s')
    ch.setFormatter(formatter)
    _logger.addHandler(ch)


    k = 5
    N = 10

    # http://docs.scipy.org/doc/numpy/reference/generated/numpy.random.randint.html
    # http://stackoverflow.com/a/2257449/2901002

    df1 = pd.DataFrame({'T': range(1, N + 1, 1),
                       'V': np.random.randint(k, k + 100, size=N),
                       'I': 'A'})

    df2 = pd.DataFrame({'T': range(1, N + 1, 1),
                       'V': np.random.randint(k, k + 100, size=N),
                       'I': 'B'})

    df = df1.append(df2)
    print(df)

    ewm = df.ewm(halflife=1)
    #ewm = df.ewm(com=5)
    print(ewm)
    print(ewm.mean())


    # Calculate relative and absolute differences
    df['dV_abs'] = df['V'] - df['V'].shift(1)
    df['dV_rel'] = (df['V'] - df['V'].shift(1)) / df['V']



    # z-score
    df['dV_abs_z'] = (df.dV_abs - df.dV_abs.mean()) / df.dV_abs.std(ddof=0)
    df['dV_rel_z'] = (df.dV_rel - df.dV_rel.mean()) / df.dV_rel.std(ddof=0)
    print(df.head())
    print(df.head().shift(1))
    print(df.count())
    print(df.std())


    for name, group in df.groupby('I'):
        print(name)
        print(group)
        print(type(group))


    qc1 = QCCheck(id='Gen1', description='This is a generic dummy check')
    print(qc1)

    qc2 = TimeSeriesQCCheck(id='Gen2', description='This is a generic time series check')
    print(qc2)


    # Creates sample data
    test_data = tstdata.HistoricalSampleData('Test Index')
    idx = test_data.build_index()

    print(idx.idxdata['sector'].unique())
    print(idx)


    # Now creates a report of what goes in and out at each period
    df =  idx.idxdata
    print(df)

    qc = HistoricalPanelQCCheck(id='test', description='test')
    qc.run_check(df)


    qc = ValueCheck(lambda x: x != 'UTIL')
    df.reset_index(inplace=True)
    qc.run_check(df, columns=['sector'])














