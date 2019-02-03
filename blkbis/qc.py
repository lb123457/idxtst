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



# Defines the metadata items that are valid for an item.

__METADATA_ITEMS__ = ['__id__', '__type__', '__type_description__']



def __init__():
    _logger.debug('')


def full_qc(idx):
    _logger.debug('Running full QC')




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
            self.save_results()
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
            print(tabulate(self.exceptions, headers='keys', tablefmt='psql'))

        if hasattr(self, 'exceptions_explanation') and self.exceptions_explanation is not None:
            print(self.exceptions_explanation)


    def add_diagnostics(self):
        print('This is the parent add_diagnostics')


    def save_results(self):
        print('Will implement results save later')



class QCValueCheck(QCCheck):

    __description__ = """
                      This is a value based QC check. It consists in verifying that columns in a pandas 
                      dataframe satisfy a condition defined in a function.
                      """

    __exception_msg__ = 'Some values in the dataframe column(s) do not satisfy the condition'

    def __init__(self, function=None, strict=True, **kwargs):
        if function is None:
            raise ValueError('Please specify a function')
        else:
            self.function = function

        self.strict = strict


    def get_exceptions(self, df, cols=None, **kwargs):
        self.successful = df[cols].apply(self.function).all(axis=1).all()
        if not self.successful:
            self.exceptions = df[~df[cols].apply(self.function).all(axis=1)]

    def add_diagnostics(self):
        print('Will add diagnostics later')


class QCColumnsSumCheck(QCCheck):

    __description__ = 'Verifies that a set of columns add up to a specified value'

    __exception_msg__ = 'Some rows in the dataframe do not sum up to the specified value.'

    def __init__(self, strict=True, **kwargs):
        self.strict = strict

    def get_exceptions(self, df, cols=None, value=None, **kwargs):
        func = lambda x: x == value
        self.successful = df[cols].sum(axis=1).apply(func).all()
        if not self.successful:
            self.exceptions = df[~df[cols].sum(axis=1).apply(func)]

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




if __name__ == "__main__":

    _logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(funcName)s:%(levelname)s: %(message)s')
    ch.setFormatter(formatter)
    _logger.addHandler(ch)


    # Creates sample data
    test_data = tstdata.HistoricalSampleData('Test Index')
    idx = test_data.build_index()

    print(idx.idxdata['sector'].unique())
    print(idx)

    df = idx.idxdata.copy(deep=True)

    qc = QCValueCheck(lambda x: x != 'UTIL', strict=False)
    qc.run_check(df, cols=['sector'])

    df['value'] = 1

    qc = QCColumnsSumCheck(strict=True)
    qc.run_check(df, cols=['value'], value=0)











