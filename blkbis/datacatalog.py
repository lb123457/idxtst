'''

Data Catalog Module

Data Catalog Description

The Data Catalog offers a common persistence layer for data that resides elsewhere
in multiple locations, predominantly in databases.

Building the catalog involves the following steps.

The first time around, the seed dataset is created by running the specified query.

Datasets that are created in Change Data Capture mode keep track of all the changes that are
observed in the database. This is appropriate then the database table itself does not keep track of
changes. Once the initial datasets are created, subsequent runs the exact same queries and compare the
results with what was saved in the previous run. Some audit columns are added to the original dataset.

For datasets that are not created in Change Data Capture mode, a new file that created from the
database query every time the datalog is updated. This is most appropriate when either there is no need
to worry about changes, or when the database table itself contains the history of changes.
Note that there are various options to keep a backup of the previous files, and analyze differences.

Curated versions of the data are derived from the raw data. The processing that is applied depends on the
dataset and most often includes columns renaming and additions.


Configuration
-------------

Configuration is stored in a yaml file.
Database connection information is stored in a separate yaml file


Data Organization
-----------------

All datasets are saved in parquet format.






'''

import os
import sys
import logging
import yaml

from tstdata import pick_random_sector

'''
    * Pass / no pass
    * Level of abnormality
    * ...

'''

#logger = logging.getLogger(__name__)

# If the query is specified, we cannot easily extract the table name
# from the query as there could be joins, and it is safer to explicitly
# give the table name


def config_to_path(config, root):
    '''
    Uses the configuration information to create the path
    From configuration to full dataset path:

    <root_path>/<access>/<type>

    <root_path>
    <access> is either public or private
    <type> is raw, curated, derived

    Data that is raw is often extracted from existing servers, and it is convenient to organize it as follows:
    <environment>/<server>/<database>/<dataset>

    '''

    if 'access' in config.keys():
        access = config['public_access']
    else:
        access = 'public'

    if 'type' in config.keys():
        type = config['type']
    else:
        type = 'raw'

    if 'environment' not in config.keys():
        raise Error('The configuration needs to contain the environment')
    else:
        environment = config['environment']

    if 'server' not in config.keys():
        raise Error('The configuration needs to contain the server')
    else:
        server = config['server']

    if 'table' in config.keys():
        database = config['table'].split('.')[0]
        user = config['table'].split('.')[1]
        table = config['table'].split('.')[2]
        dataset = table
    elif 'dataset' in config.keys():
        dataset = config['dataset']
        if 'database' not in config.keys():
            raise ValueError('The configuration contains the dataset but it does not contain the database')
        else:
            database = config['database']
    else:
        raise ValueError('The configuration needs to contain either the table or the dataset')

    # Now we have everything to specify the path

    return os.path.join(root, access, type, environment, server, database, dataset + '.parquet')


# Creates some simple test configurations

config = [{'table': 'secdb.dbo.sec_master',
           'environment': 'ACE',
           'server': 'DSREAD'},

          {'sql': 'select top 10 * from secdb.dbo.sec_master',
           'dataset': 'sec_master_gov',
           'database': 'secdb',
           'environment': 'ACE',
           'server': 'DSREAD'}]

print(config_to_path(config[0], '/GAAR/bis/data/dwh'))
print(config_to_path(config[1], '/GAAR/bis/data/dwh'))

class DCBuilder():

    def __init__(self,
                 configuration_file=None,
                 configuration_type='yaml',
                 **kwargs):
        '''
        Data Catalog Builder

        :param configuration: yaml file that contains the configuration
        :param kwargs:
        '''

        # Logging setup
        #logger = logging.getLogger(__name__)


        if configuration_file is None:
            raise ValueError('configuration_file must be specified')
        else:
            self.configuration_file = configuration_file
            logger.debug('Configuration file = %s', self.configuration_file)

        if configuration_type is None:
            raise ValueError('configuration_type must be specified')
        else:
            self.configuration_type = configuration_type
            logger.debug('Configuration type = %s', self.configuration_type)

        if 'rotation' in kwargs:
            self.rotation = kwargs['rotation']
            logger.debug('rotation = %s', self.rotation)


        with open(self.configuration_file, 'r') as cf:
            try:
                self.config_data = yaml.load(cf)
                logger.debug(str(self.config_data))
            except yaml.YAMLError as exc:
                print(exc)



    def build_data(self):

        # Gets the directory where the catalog will be stored


        pass




class DCItem():

    def __init__(self):
        pass



class DCReader():
    '''
    This is the class
    '''

    def __init__(self,
                 location=None,
                 **kwargs):
        pass


    def list_catalog(self):
        '''
        Prints all items in the catalog

        :return: None
        '''
        pass

    def load_catalog(self):
        '''
        Loads all items in the catalog

        :return: A dictionary where the keys are the filenames without the parquet extension
        '''
        pass


    def load_catalog_multithreaded(self):
        pass


    def load_item(self, item=None):
        '''
        Loads one items in the catalog

        :item: The file name of the item in the catalog without the parquet extension
        :return: A dictionary where the keys are the file names without the parquet extension
        '''
        pass

    def __str__(self):
        pass



def dummy_function(arg1=1, arg2=2):
    print(arg1)
    print(arg2)

if __name__ == '__main__':

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(funcName)s:%(levelname)s: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    dc_builder = DCBuilder(configuration_file='/Users/ludovicbreger/Data/DataCatalog/test_config.yml')
    dc_builder.build_data()
    dc_reader = DCReader(location='/Users/ludovicbreger')







