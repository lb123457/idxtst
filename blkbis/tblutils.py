'''

Tableau utilities

'''

from __future__ import print_function
import os
import sys
import logging
import pandas
import logging


import argparse
import sys
import textwrap

from tableausdk import *
from tableausdk.HyperExtract import *

# Logging setup
logger = logging.getLogger()

'''

A QC is defined by:

- The static description of the model

    * qc_model_type
    * qc_model_type_description
    * qc_model_subtype
    * qc_mdel_subtype_description
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

__QCCHECK_METADATA_ITEMS__ = ['description', 'id', 'type', 'type_description']


def __init__():
    logger.debug('')


def full_qc(idx):
    logger.debug('Running full QC')


#------------------------------------------------------------------------------
#
#   This file is the copyrighted property of Tableau Software and is protected
#   by registered patents and other applicable U.S. and international laws and
#   regulations.
#
#   Unlicensed use of the contents of this file is prohibited. Please refer to
#   the NOTICES.txt file for further details.
#
#------------------------------------------------------------------------------



#------------------------------------------------------------------------------
#   Parse Arguments
#------------------------------------------------------------------------------
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

#------------------------------------------------------------------------------
#   Create Extract
#------------------------------------------------------------------------------
#   (NOTE: This function assumes that the Tableau SDK Extract API is initialized)
def createOrOpenExtract(
    filename,
    useSpatial
):
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
        exit( -1 )

    return extract

#------------------------------------------------------------------------------
#   Populate Extract
#------------------------------------------------------------------------------
#   (NOTE: This function assumes that the Tableau SDK Extract API is initialized)
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
    retval = main()
    sys.exit( retval )


