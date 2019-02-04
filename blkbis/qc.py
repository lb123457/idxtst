"""
QC Package
"""


import os
import sys
import logging
import pandas as pd
import numpy as np
import pickle
from blkbis import blkidx
from blkbis import tstdata
from blkbis import dfutils
from blkbis import dates
from blkbis import filters
from tabulate import tabulate

# Logging setup
_logger = logging.getLogger()


class QCCheckError(Exception):
    def __init__(self, check):
        # Call the base class constructor with the parameters it needs
        super().__init__(check.__exception_msg__)

        # Now for your custom code...
        self.errors = check.exceptions

        self.dispatch_error()

        check.add_diagnostics()

    def dispatch_error(self):
        print('Will implement dispatching later')


class QCCheck(object):
    """
    This is the parent class for all QC checks.

    Checks are performed within the children classes.
    The parent class implements various
    """

    # When set to True, forces all failed checks to throw an exception
    force_strict = None

    def __init__(self):
        pass


    def run_check(self, *args, **kwargs):
        """
        Takes a dataframe and

        :param df:
        :param kwargs:
        :return:
        """

        # Registers the check if necessary
        self.register()

        self.get_exceptions(*args, **kwargs)

        # Prints the exceptions and either raise an exception of sends a warning
        if not self.successful:
            #self.save_check()
            self.show_exceptions()

            if self.force_strict or (self.strict and self.force_strict is None):
                raise QCCheckError(self)

            else:
                _logger.warning('Dataframe contains values that do not satisfy the condition')


    def register(self):
        print('Will implement registration later')


    def show_exceptions(self):
        print(' '.join(self.__description__.split()))

        if self.exceptions is not None:
            if isinstance(self.exceptions, pd.core.frame.DataFrame):
                print(tabulate(self.exceptions, headers='keys', tablefmt='psql'))
            else:
                _logger.warning(self.exceptions)

        if hasattr(self, 'exceptions_explanation') and self.exceptions_explanation is not None:
            print(self.exceptions_explanation)


    def add_diagnostics(self):
        _logger.debug('This is the parent add_diagnostics')


    def save_check(self, filename):
        self.pickle_file = filename
        _logger.debug('Saving to %s', filename)
        with open(self.pickle_file, 'wb') as outfile:
            pickle.dump(self, outfile)


    @staticmethod
    def load_check(filename):
        _logger.debug('Loading check from %s', filename)
        with open(filename, 'rb') as infile:
            file = open(filename, 'rb')
            return pickle.load(file)




class QCValueCheck(QCCheck):

    __description__ = 'Verifies a pandas dataframe columns satisfy a condition defined in a function.'
    __exception_msg__ = 'Some values in the dataframe column(s) do not satisfy the condition'

    def __init__(self, function=None, filter=None, strict=True, **kwargs):

        if function is not None and filter is not None:
            raise ValueError('The function and filter arguments cannot be both passed')
        elif function is None and filter is None:
            raise ValueError('Please specify either a function or a filter')
        elif function is not None:
            self.function = function
        elif isinstance(filter, filters.DFFilter):
            self.filter = filter
            self.function = filter.function
        else:
            raise ValueError('Something is wrong with the arguments')

        self.strict = strict

    def get_exceptions(self, df, columns=None, **kwargs):
        self.successful = df[columns].apply(self.function).all(axis=1).all()
        if not self.successful:
            self.exceptions = df[~df[columns].apply(self.function).all(axis=1)]

    def add_diagnostics(self):
        print('Will add diagnostics later')



class QCColumnsSumCheck(QCCheck):

    __description__ = 'Verifies that a set of columns add up to a specified value'
    __exception_msg__ = 'Some rows in the dataframe do not sum up to the specified value.'

    def __init__(self, strict=True, **kwargs):
        self.strict = strict

    def get_exceptions(self, df, columns=None, value=None, **kwargs):
        func = lambda x: x == value
        self.successful = df[columns].sum(axis=1).apply(func).all()
        if not self.successful:
            self.exceptions = df[~df[columns].sum(axis=1).apply(func)]

    def add_diagnostics(self):
        print('Will add diagnostics later')


class QCCorrCheck(QCCheck):

    def __init__(self):
        pass



class TimeSeriesQCCheck(QCCheck):
    pass



class CrossSectionalQCCkeck(QCCheck):
    pass



class XRayQCCheck(QCCheck):
    pass

        


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





class StructuralIndexCheck(QCCheck):

    __description__ = 'Verifies that the dataframe is indexed as expected'
    __exception_msg__ = 'The dataframe is not indexed as expected'

    def __init__(self, strict=True, **kwargs):
        self.strict = strict

    def get_exceptions(self, df, columns=None, **kwargs):
        self.successful = df.index.names[0] == 'date'
        if not self.successful:
            if df.index.names[0] is None:
                self.exceptions = 'There is no index defined for this dataframe'
            else:
                self.exceptions = 'The level 0 index for this dataframe is ' + df.index.names[0] + ' and we expect "date"'

    def add_diagnostics(self):
        print('Will add diagnostics later')



class StructuralColumnsCheck(QCCheck):

    __description__ = 'Verifies that the columns are all in lowercase'
    __exception_msg__ = 'One or more column names are not in lowercase'

    def __init__(self, strict=True, **kwargs):
        self.strict = strict

    def get_exceptions(self, df, columns=None, **kwargs):
        if columns is None:
            columns = df.columns

        df_cols = pd.DataFrame(df[columns].columns, columns=['original_column'])

        df_cols['lower_column_name'] = df_cols['original_column'].str.lower()
        df_cols['column_is_lowercase'] = np.where(df.columns != df.columns.str.lower(), False, True)

        self.successful = df_cols['column_is_lowercase'].all()

        if not self.successful:
            self.exceptions = df_cols[~df_cols['column_is_lowercase']]

    def add_diagnostics(self):
        print('Will add diagnostics later')






if __name__ == "__main__":

    _logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(funcName)s:%(levelname)s: %(message)s')
    ch.setFormatter(formatter)
    _logger.addHandler(ch)


    # Creates sample data
    test_data = tstdata.HistoricalTstData('Test Index')
    idx = test_data.build_index()

    print(idx.idxdata['sector'].unique())
    print(idx)

    df = idx.idxdata.copy(deep=True)

    qc = QCValueCheck(function=lambda x: x != 'UTIL', strict=False)
    qc.run_check(df, columns=['sector'])

    df['value'] = 1

    qc = QCColumnsSumCheck(strict=False)
    qc.run_check(df, columns=['value'], value=0)


    # Creates a filter and uses it for QC
    filter = filters.DFFilter(lambda x: x != 'UTIL', 'Dummy function 2')

    qc = QCValueCheck(filter=filter, strict=False)
    qc.run_check(df, columns=['sector'])

    # Tests the structural checks
    df2 = df.rename(columns={'sector': 'Sector'})
    qc = StructuralColumnsCheck(strict=False)
    qc.run_check(df2)

    qc = StructuralIndexCheck(strict=False)
    df.reset_index(inplace=True)
    qc.run_check(df)

    qc.save_check('qc.pkl')
    qc_retrieved = QCCheck.load_check('qc.pkl')
