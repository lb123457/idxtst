import logging
import sys
import inspect
import re
from dill.source import getsource
import pandas as pd
from datetime import datetime

from blkbis import tstdata

logger = logging.getLogger(__name__)


def updateDateTypes(df):
    '''
    Takes a pandas dateframe and update columns that look like datetimes.

    :param df: input dataframe
    :return: None
    '''

    for c in df.columns:
        guessDateTypes(df, c)


def guessDateTypes(df, c):
    '''
    Takes a pandas dataframe and column c and updates its type to datetime if possible.

    :param df: input dataframe
    :return: None
    '''

    formats = {'%Y-%m-%d': '^[1-2]\d{3}-\d{2}-\d{2}',
               '%m/%d/%Y': '^\d{2}\/\d{2}\/[1-2]\d{3}',
               '%Y%m%d': '^[1-2]\d{3}\d{2}\d{2}'
    }

    # For this format to work we need to make sure that the format matches
    # as integers will convert just fine

    for format, regexp in formats.items():
        logger.debug('Trying to convert column "%s" to datetime using format %s', c, format)
        if False not in df[c].astype(str).str.contains(regexp).tolist():
            try:
                df[c] = pd.to_datetime(df[c], format=format)
            except:
                logger.debug('Type conversion failed')
            else:
                logger.debug('>>> Converted column %s to datetime using format %s', c, format)
                return
        else:
            logger.debug('Format %s does not match values' % (format))





def filterOnColumn(df, column, condition, result_column, condition_column, inplace=True, **kwargs):
    '''

    :param df: input pandas dataframe
    :param column: column that is the target of the filtering
    :param condition:
    :param inplace: inplace condition
    :return: the new dataframe if inplace is True, otherwise None
    '''

    if not inplace:
        df = df.copy(deep=True)

    # If a lambda function is used for filtering, for some reason, getsource
    # returns not just the source code of the lambda function but the function call
    # that has the lambda function. It therefore is necessary to extract the lambda
    # function from the call so that there is not the additional stuff.

    filter_source = getsource(condition)
    logger.debug('Original filter_source')
    logger.debug(filter_source)
    searchObj = re.search(r'(lambda [^,]*)([,]*)(.*)[)]{1}$', filter_source, re.M | re.I)

    if searchObj:
        filter_source = searchObj.group(1)

    logger.debug('Retained filter_source')
    logger.debug(filter_source)

    df[result_column] = df[column].apply(condition)
    df[condition_column] = getsource(condition)

    if not inplace:
        return df



def multi_dataframe_merge(dataframe_list):
    '''
    Takes a list of dataframes and merge them on their indices.

    :param dataframe_list:
    :return:
    '''

    df = dataframe_list[0]

    for d in dataframe_list[1:]:
        logger.info('Merging dataframe')
        df = df.merge(d, left_index=True, right_index=True)

    return df



def compare_groups(df1, df2):
    '''
    Compares two dataframes
    :param df1:
    :param df2:
    :return: None
    '''



if __name__ == "__main__":

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(funcName)s:%(levelname)s: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # Creates sample data
    test_data = tstdata.HistoricalSampleData('Test Index')
    idx = test_data.build_index()

    print(idx.idxdata['sector'].unique())
    print(idx)

    df = idx.idxdata.copy(deep=True)


    # A couple examples using an anonymous function
    filterOnColumn(df, 'sector', lambda x: True if x == 'TELE' else False, 'sector_filter_result', 'sector_filtering_method', forensics='ALL')
    filterOnColumn(df, 'date', lambda x: True if x.strftime('%Y-%m-%d') in ('2018-01-01') else False, 'date_filter_result', 'date_filtering_method')

    exec("""def fun():
      print('bbb')
    """)
    # One example using an explicit function
    def filter_on_size(x, size_limit=500):
        if x >= size_limit:
            return True
        else:
            return False

    filterOnColumn(df, 'size_mm', filter_on_size, 'size_filter_result', 'size_filtering_method')

    print(df.head())
    fun()

    print(df.index)

    df = idx.idxdata.copy(deep=True)
    df.set_index(['sector', 'date'], inplace=True)

    df_results = multi_dataframe_merge([df, df, df])
    print(df_results.head())