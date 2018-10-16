'''

Tableau utilities

'''

from __future__ import print_function
import os
import sys
import logging
import pandas as pd
import numpy as np
import logging


import argparse
import sys
import textwrap

from tableausdk import *
from tableausdk.HyperExtract import *

# Logging setup
logger = logging.getLogger()



def __init__():
    logger.debug('')


'''
Parse Arguments
'''

def parseArguments():
    parser = argparse.ArgumentParser( description='A simple demonstration of the Tableau SDK.', formatter_class=argparse.RawTextHelpFormatter )
    # (NOTE: '-h' and '--help' are defined by default in ArgumentParser
    parser.add_argument( '-b', '--build', action='store_true', # default=False,
                         help=textwrap.dedent('''\
                            If an extract named FILENAME exists in the current directory,
                            extend it with sample data.
                            If no Tableau extract named FILENAME exists in the current directory,
                            create one and populate it with sample data.
                            (default=%(default)s)
                            ''' ) )
    parser.add_argument( '-s', '--spatial', action='store_true', # default=False,
                         help=textwrap.dedent('''\
                            Include spatial data when creating a new extract."
                            If an extract is being extended, this argument is ignored."
                            (default=%(default)s)
                            ''' ) )
    parser.add_argument( '-f', '--filename', action='store', metavar='FILENAME', default='order-py.hyper',
                         help=textwrap.dedent('''\
                            FILENAME of the extract to be created or extended.
                            (default='%(default)s')
                            ''' ) )
    return vars( parser.parse_args() )


'''
Creates a schema from a pandas dataframe.
'''


from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype
from pandas.api.types import is_integer_dtype
from pandas.api.types import is_float_dtype
from pandas.api.types import is_datetime64_dtype

import math

def valueIsMissing(v):
    if math.isnan(v):
        return True
    else:
        return False


def df2schema(df, filename):

    try:

        if os.path.exists(filename):
            os.remove(filename)

        extract = Extract( filename )

        #if ( extract.hasTable( 'Extract' ) ):
        #   extract.__del__()

        if ( not extract.hasTable( 'Extract' ) ):
            schema = TableDefinition()
            schema.setDefaultCollation( Collation.EN_GB )

            for c in df.columns:
                if is_string_dtype(df[c]):
                    schema.addColumn(c, Type.CHAR_STRING)
                    df[c].fillna('', inplace=True)
                elif is_datetime64_dtype(df[c]):
                    schema.addColumn(c, Type.DATETIME)
                elif is_integer_dtype(df[c]):
                    schema.addColumn(c, Type.INTEGER)
                    df[c].fillna(0, inplace=True)
                elif is_float_dtype(df[c]):
                    schema.addColumn(c, Type.DOUBLE)
                    df[c].fillna(0, inplace=True)
                elif is_bool_dtype(df[c]):
                    schema.addColumn(c, Type.BOOLEAN)


            table = extract.addTable( 'Extract', schema )
            if ( table == None ):
                print('A fatal error occurred while creating the table:\nExiting now\n.')
                exit( -1 )

    except TableauException as e:
        print('A fatal error occurred while creating the new extract:\n', e, '\nExiting now.')
        exit(-1)


    try:
        # Get Schema
        table = extract.openTable( 'Extract' )
        schema = table.getTableDefinition()

        nrows = 0

        for index, row in df.iterrows():

            print(index)
            print(row)
            print()

            tabRow = Row( schema )

            colIdx = 0

            for c in df.columns:
                print(c)
                print(row[c])
                print(type(row[c]))
                if is_string_dtype(df[c]):
                    tabRow.setCharString(colIdx, row[c])
                elif is_datetime64_dtype(df[c]):
                    tabRow.setDateTime(colIdx, row[c].year, row[c].month, row[c].day, row[c].hour,  row[c].minute, row[c].second, 0)
                elif is_integer_dtype(df[c]):
                    tabRow.setInteger(colIdx, row[c])
                elif is_float_dtype(df[c]):
                    tabRow.setDouble(colIdx, row[c])
                elif is_bool_dtype(df[c]):
                    tabRow.setBoolean(colIdx, row[c])

                colIdx += 1

            table.insert( tabRow )
            if nrows == 20:
                return
            nrows += 1

        print('%d rows inserted' % nrows)

    except TableauException as e:
        print('A fatal error occurred while populating the extract:\n', e, '\nExiting now.')
        exit( -1 )

    else:
        extract.close()

    return extract



def publishExtract(extract):
    pass



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
        print('Trying to convert column "%s" to datetime using format %s' % (c, format))
        if False not in df[c].astype(str).str.contains(regexp).tolist():
        #if False not in df[c].astype(str).str.contains('^[1-2]\d{3}-\d{2}-\d{2}').tolist():
            try:
                df[c] = pd.to_datetime(df[c], format=format)
            except:
                print('pd.to_datetime() conversion failed')
            else:
                print('Converted column %s to datetime using format %s' % (c, format))
                return
        else:
            print('Format %s because format does not match values' % (format))


#------------------------------------------------------------------------------
#   Main
#------------------------------------------------------------------------------

def main():
    # Parse Arguments
    options = parseArguments()

    # Extract API Demo
    if ( options[ 'build' ] ):
        # Initialize the Tableau Extract API
        ExtractAPI.initialize()

        # Create or Expand the Extract
        extract = createOrOpenExtract( options[ 'filename' ], options[ 'spatial' ] )
        populateExtract( extract, options[ 'spatial' ] )

        # Flush the Extract to Disk
        extract.close()

        # Close the Tableau Extract API
        ExtractAPI.cleanup()

    return 0

if __name__ == "__main__":

    df = pd.read_csv('/Users/ludovicbreger/Data/Tableau/test_df.csv')
    df['test_date'] = '01/01/2001'
    df['test_date2'] = '20010101'
    df['test_date3'] = '2001-01-01 01:01:01.500'

    print(df.dtypes)

    updateDateTypes(df)

    df = df.drop('next_cusip', 1)
    df = df.head(50)
    #guessSeriesType(df, 'effective_date')
    #guessSeriesType(df, 'A')
    #guessSeriesType(df, 'test_date')
    #guessSeriesType(df, 'test_date2')
    #guessSeriesType(df, 'test_date3')


    print(df.dtypes)
    print(df.shape)
    print(df)


    #df.fillna(0, inplace=True)

    extract = df2schema(df, '/Users/ludovicbreger/Data/Tableau/test_df2.hyper')


