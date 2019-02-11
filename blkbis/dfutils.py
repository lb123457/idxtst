import logging
import sys
import inspect
import re
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

    filter_source = inspect.getsource(condition)
    logger.debug('Original filter_source')
    logger.debug(filter_source)
    searchObj = re.search(r'(lambda [^,]*)([,]*)(.*)[)]{1}$', filter_source, re.M | re.I)

    if searchObj:
        filter_source = searchObj.group(1)

    logger.debug('Retained filter_source')
    logger.debug(filter_source)

    df[result_column] = df[column].apply(condition)
    df[condition_column] = inspect.getsource(condition)

    if not inplace:
        return df


class DFFilter:

    __type__ = 'GENERIC'
    __type_description__ = 'This is a base generic test'

    def __init__(self,
                 function=None,
                 description=None,
                 **kwargs):
        '''
        This is a filter that is designed to be applied to dataframe.

        :param function:
        :param description:
        :param kwargs:
        '''

        if function is None:
            raise ValueError('"function" must be specified')
        else:
            self.function = function

        if description is None:
            raise ValueError('"description" must be specified')
        else:
            self.description = description



    # Applies the filter to the specified dataframe and columns
    def apply_filter(self, df, columns=None):
        '''
        Applies the filter to the dataframe df.
        The parameter columns needs to be specified.

        :param df:
        :param columns:
        :return: the dataframe with the filter applied
        '''

        df[columns] = df[columns].apply(self.function)





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




def compare_dataframe(df_left, df_right, *kwargs):
    '''
    Compares two dataframes
    :param df1:
    :param df2:
    :return: Dictionary
    '''

    resuts = {}

    # Checks columns
    if df_left.columns.tolist() == df_right.columns.tolist():
        # Column names are the same and in the same order
        resuts['column_names_match'] = True
        resuts['column_order_match'] = True
    elif set(df_left.columns.tolist()) - set(df_right.columns.tolist()) == set(df_right.columns.tolist()) - set(df_left.columns.tolist()):
        resuts['column_names_match'] = True
        resuts['column_order_match'] = False
    else:
        resuts['column_names_match'] = False
        resuts['column_order_match'] = False
        if len(set(df_left.columns.tolist()) - set(df_right.columns.tolist())):
            logger.warning('The following columns are in the left dataframe but not in the right--')
            logger.warning(set(df_left.columns.tolist()) - set(df_right.columns.tolist()))
        if len(set(df_right.columns.tolist()) - set(df_left.columns.tolist())):
            logger.warning('The following columns are in the right dataframe but not in the left')
            logger.warning(set(df_right.columns.tolist()) - set(df_left.columns.tolist()))

    # Checks column types
    if df_left.dtypes.tolist() != df_right.dtypes.tolist():
        resuts['column_types_match'] = False
    else:
        resuts['column_types_match'] = True


    # Checks the values
    if resuts['column_names_match'] & resuts['column_order_match'] & resuts['column_types_match']:
        if False not in (df_left == df_right):
            resuts['exact_match'] = True
    else:
        resuts['exact_match'] = False


    # Other things to potentially check for...
    # Checks the order of columns


    # Checks the indices

    return resuts



def dataframe_delta(df_left, df_right, **kwargs):
    '''
    Computes the delta between two dataframes defined as follows.

    - Dataframes need to have the exact same columns
    - Dataframes have to have the exact same index
    - The delta is the combination of the rows that are in df_left but not df_right,
      the rows that are in df_right but not df_left, and the rows that are in both but with different values

    :param df_left:
    :param df_right:
    :param kwargs:
    :return: a dataframe that contains the difference
    '''

    # Checks if the dataframes are structurally the same
    try:
        if False in (df_left.columns == df_right.columns):
            raise Exception('The left and right dataframes do not have the same columns')
    except:
        raise Exception('Could not compare columns')

    try:
        if False in (df_left.dtypes == df_right.dtypes):
            raise Exception('The left and right dataframes do not have the same column types')
    except:
        raise Exception('Could not compare column types')

    try:
        if df_left.index.name != df_right.index.name:
            raise Exception('The left and right dataframes do not have the same indices')
    except:
        raise Exception('Could not compare indices')

    # Merges the two dataframes
    df = df_left.merge(df_right, left_index=True, right_index=True, how='outer', indicator=True)

    # Isolates the rows that are not a match and identifies the columns that are at fault
    cnames = []

    for c in df.columns:
        if c.endswith('_x'):
            cname = c + ' matches ' + c[:-2] + '_y'
            cnames.append(cname)
            df[cname] = (df[c] == df[c[:-2] + '_y'])

    # Now consolidates the results of all comparison
    df['columns_matches_number'] = df[cnames].sum(axis=1)
    df['columns_matches_all'] = df[cnames].all(axis=1)

    df = df.reindex(sorted(df.columns), axis=1)

    return df



def color_rows(s):
    '''
    Utility to change the stype of the rendered dataframe.
    See https://pandas.pydata.org/pandas-docs/stable/style.html for some good background information.

    :param s:
    :return: the style
    '''
    df = s.copy(deep=True)

    if '_merge' in df.columns:
        df['_merge'] = df['_merge'].astype(str)

    # Starts with a little column manipulation
    cols = df.columns

    # Looks for all the original columns
    c_map = {}
    original_columns = []
    columns_x = []
    columns_y = []
    columns_c = []

    for c in df.columns:
        if c.endswith('_x'):
            original_columns.append(c[0:-2])
            columns_x.append(c)
            columns_y.append(c[0:-2] + '_y')
            columns_c.append(c + ' matches ' + c[:-2] + '_y')
            c_map[c + ' matches ' + c[:-2] + '_y'] = [c, c + ' matches ' + c[:-2] + '_y', c[:-2] + '_y']

    for index, row in df.iterrows():
        if not row['columns_matches_all']:
            df.loc[index, :] = 'color: red'

            # If not all column match we highlight in red the triplets of
            # c_x, c_x matches c_y and c_y that are not a match.
            for c in columns_c:
                if not row[c]:
                    df.loc[index, c_map[c]] = 'background-color: #EEBBBB; color: blue; border-left: 1px'
        else:
            df.loc[index, :] = 'background-color: #EEEEEE'

    return df


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

def dummy():
    pass


class DFComparator(object):

    def __init__(self, df1, df2, **kwargs):
        self.df1 = df1
        self.df2 = df2

    def compare_dataframes(df1, df2, **kwargs):
        """
        Things to compare and results format:
        -
        """
        pass

    def have_same_columns(self, **kwargs):
        if self.have_same_column_names():
            if self.have_same_column_types():
                return True
        else:
            return False

    def have_same_column_names(self, **kwargs):
        return self.df2.columns.tolist() == self.df1.columns.tolist()

    def have_same_column_types(self, **kwargs):
        return not False in self.df1.dtypes == self.df2.dtypes

    def have_same_rowcount(self, **kwargs):
        return self.df1.shape[0] == self.df2.shape[0]



class DFWriter(object):

    def __init__(self, **kwargs):
        pass


class DFParquetWriter(DFWriter):

    def __init__(self):
        pass

    def write_file(self, df, file, **kwargs):
        pass


class DFCSVWriter(DFWriter):

    def __init__(self):
        pass

    def write_file(self, df, file, **kwargs):
        pass



if __name__ == "__main__":

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(funcName)s:%(levelname)s: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # Creates sample data
    test_data = tstdata.HistoricalTstData('Test Index')
    idx = test_data.build_index()

    print(idx.idxdata['sector'].unique())
    print(idx)

    df = idx.idxdata.copy(deep=True)


    # A couple examples using an anonymous function
    filterOnColumn(df, 'sector', lambda x: True if x == 'TELE' else False, 'sector_filter_result', 'sector_filtering_method', forensics='ALL')

    exec("""def fun():
      print('bbb')
    """)


    # Compares dataframes
    df2 = df.copy()
    df2['c'] = 'new columns'
    print(compare_dataframe(df, df2))

