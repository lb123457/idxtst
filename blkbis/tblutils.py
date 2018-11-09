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
import getpass
import logging

import argparse
import sys
import textwrap

from tableausdk import *
from tableausdk.HyperExtract import *

import tableauserverclient as TSC

from blkbis import dfutils

# Logging setup
logger = logging.getLogger()



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



def publishExtract(args):
    # SIGN IN
    tableau_auth = TSC.TableauAuth(args.username, args.password)
    server = TSC.Server(args.server)
    server.use_highest_version()
    with server.auth.sign_in(tableau_auth):
        # Query projects for use when demonstrating publishing and updating
        all_projects, pagination_item = server.projects.get()
        default_project = next((project for project in all_projects if project.is_default()), None)

        # Publish datasource if publish flag is set (-publish, -p)
        if args.publish:
            if default_project is not None:
                new_datasource = TSC.DatasourceItem(default_project.id)
                new_datasource = server.datasources.publish(
                    new_datasource, args.publish, TSC.Server.PublishMode.Overwrite)
                print("Datasource published. ID: {}".format(new_datasource.id))
            else:
                print("Publish failed. Could not find the default project.")

        # Gets all datasource items
        all_datasources, pagination_item = server.datasources.get()
        print("\nThere are {} datasources on site: ".format(pagination_item.total_available))
        print([datasource.name for datasource in all_datasources])






#------------------------------------------------------------------------------
#   Main
#------------------------------------------------------------------------------

def main():

    parser = argparse.ArgumentParser( description='A simple demonstration of the Tableau SDK.', formatter_class=argparse.RawTextHelpFormatter )
    # (NOTE: '-h' and '--help' are defined by default in ArgumentParser


    parser.add_argument('--build', '-b', help='path to hyper file to build', default='/Users/ludovicbreger/PycharmProjects/idxtst/test.hyper')
    parser.add_argument('--publish', '-p', metavar='FILEPATH', help='path to datasource to publish', default=True)
    parser.add_argument('--download', '-d', metavar='FILEPATH', help='path to save downloaded datasource')
    parser.add_argument('--server', '-s', help='server address', default='https://public.tableau.com')
    parser.add_argument('--username', '-u', help='username to sign into server', default='breglud')
    parser.add_argument('--password', '-x', help='password to sign into server', default='LudoTableau123$')

    parser.add_argument('--logging-level', '-l', choices=['debug', 'info', 'error'], default='error',
                        help='desired logging level (set to error by default)')

    args = parser.parse_args()

    if args.build:

        df = pd.read_csv('/Users/ludovicbreger/Data/Tableau/test_df.csv')
        df['test_date'] = '01/01/2001'
        df['test_date2'] = '20010101'
        df['test_date3'] = '2001-01-01 01:01:01.500'

        print(df)

        dfutils.updateDateTypes(df)

        df = df.drop('next_cusip', 1)
        df = df.head(50)
        # guessSeriesType(df, 'effective_date')
        # guessSeriesType(df, 'A')
        # guessSeriesType(df, 'test_date')
        # guessSeriesType(df, 'test_date2')
        # guessSeriesType(df, 'test_date3')

        print(df.dtypes)
        print(df.shape)
        print(df)

        # df.fillna(0, inplace=True)

        extract = df2schema(df, args.build)



    if args.publish | 1 == 1:
        publishExtract(args)



    return 0

if __name__ == "__main__":
    main()





