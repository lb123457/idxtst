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
        extract = Extract( filename )

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
                    if math.isnan(row[c]):
                        tabRow.setNull(colIdx, row[c])
                    else:
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

    except TableauException as e:
        print('A fatal error occurred while populating the extract:\n', e, '\nExiting now.')
        exit( -1 )

    return extract


'''
Creates extract schema
'''

def createOrOpenExtract(filename,
                        useSpatial):
    try:
        # Create Extract Object
        # (NOTE: The Extract constructor opens an existing extract with the
        #  given filename if one exists or creates a new extract with the given
        #  filename if one does not)
        print(filename)
        extract = Extract( filename )

        # Define Table Schema (If we are creating a new extract)
        # (NOTE: In Tableau Data Engine, all tables must be named 'Extract')
        if ( not extract.hasTable( 'Extract' ) ):
            schema = TableDefinition()
            schema.setDefaultCollation( Collation.EN_GB )
            schema.addColumn( 'Purchased',              Type.DATETIME )
            schema.addColumn( 'Product',                Type.CHAR_STRING )
            schema.addColumn( 'uProduct',               Type.UNICODE_STRING )
            schema.addColumn( 'Price',                  Type.DOUBLE )
            schema.addColumn( 'Quantity',               Type.INTEGER )
            schema.addColumn( 'Taxed',                  Type.BOOLEAN )
            schema.addColumn( 'Expiration Date',        Type.DATE )
            schema.addColumnWithCollation( 'Produkt',   Type.CHAR_STRING, Collation.DE )
            if ( useSpatial ):
                schema.addColumn( 'Destination',        Type.SPATIAL )
            table = extract.addTable( 'Extract', schema )
            if ( table == None ):
                print('A fatal error occurred while creating the table:\nExiting now\n.')
                exit( -1 )

    except TableauException as e:
        print('A fatal error occurred while creating the new extract:\n', e, '\nExiting now.')
        exit(-1)

    return extract


'''
Populates extract
'''

def populateExtract(
    extract,
    useSpatial
):
    try:
        # Get Schema
        table = extract.openTable( 'Extract' )
        schema = table.getTableDefinition()

        # Insert Data
        row = Row( schema )
        row.setDateTime( 0, 2012, 7, 3, 11, 40, 12, 4550 )  # Purchased
        row.setCharString( 1, 'Beans' )                     # Product
        row.setString( 2, u'uniBeans'    )                  # Unicode Product
        row.setDouble( 3, 1.08 )                            # Price
        row.setDate( 6, 2029, 1, 1 )                        # Expiration Date
        row.setCharString( 7, 'Bohnen' )                    # Produkt
        # in python2: use `xrange` instead of `range` here to reduce memory consumption
        for i in range( 10 ):
            row.setInteger( 4, i * 10 )                     # Quantity
            row.setBoolean( 5, i % 2 == 1 )                 # Taxed
            if useSpatial:
                row.setSpatial( 8, "POINT (30 10)" )        # Destination
            table.insert( row )

    except TableauException as e:
        print('A fatal error occurred while populating the extract:\n', e, '\nExiting now.')
        exit( -1 )


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

    #guessSeriesType(df, 'effective_date')
    #guessSeriesType(df, 'A')
    #guessSeriesType(df, 'test_date')
    #guessSeriesType(df, 'test_date2')
    #guessSeriesType(df, 'test_date3')


    print(df.dtypes)
    print(df)

    #df.fillna(0, inplace=True)

    extract = df2schema(df, '/Users/ludovicbreger/Data/Tableau/test_df1.hyper')
    fillExtract(extract, df)

    exit()
    createTestData()
    retval = main()
    sys.exit( retval )


