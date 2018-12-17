'''

Data Warehouse Module

Data Warehouse Description

The Data Warehouse offers a common persistence layer for data that resides elsewhere
in multiple locations, predominantly in databases.

Building the datasets involves the following steps.

The first time around, the seed dataset is created by running the specified query.

Datasets that are created in Change Data Capture mode keep track of all the changes that are
observed in the database. This is appropriate then the database table itself does not keep track of
changes. Once the initial datasets are created, subsequent runs the exact same queries and compare the
results with what was saved in the previous run. Some audit columns are added to the original dataset.

For datasets that are not created in Change Data Capture mode, a new file is created from the
database query every time the datalog is updated. This is most appropriate when either there is no need
to worry about changes, or when the database table itself contains the history of changes.
Note that there are various options to keep a backup of the previous files, and analyze differences.

Curated versions of the data are derived from the raw data. The processing that is applied depends on the
dataset and most often includes columns renaming and additions.


Configuration
-------------

Configuration is stored in a yaml file.
Database connection information is stored in a separate yaml file
Credentials are stored in yet another file that is not stored in source control


Data Organization
-----------------

All datasets are saved in parquet format.



'''

import os
import sys
import logging
import yaml
import pprint



_logger = logging.getLogger(__name__)

# If the query is specified, we cannot easily extract the table name
# from the query as there could be joins, and it is safer to explicitly
# give the table name
            
            
def dict_value_str_replace(d, old, new):
    """Find and replace value sub-strings in a dictionary

    Args:
        d: Dictionary which is modified in place
        old: Old string
        new: New string

    Example:
        d = {'key': '$ROOT/path'}
        dict_value_str_replace({'key': '$ROOT/path'}, '$ROOT', '/absolute')

        Results in: {'key': '/absolute/path'}
    """
    if isinstance(d, dict):
        for k, v in d.items():
            if isinstance(v, dict):
                dict_value_str_replace(v, old, new)
            elif isinstance(v, list):
                dict_value_str_replace(v, old, new)
            elif isinstance(v, str):
                d[k] = v.replace(old, new)
    elif isinstance(d, list):
        for i, v in enumerate(d):
            if isinstance(v, dict):
                dict_value_str_replace(v, old, new)
            elif isinstance(v, str):
                d[i] = v.replace(old, new)
                
                
def _config_to_path(dwhItemConfig, dwhRoot):
    '''
    Uses the configuration information to create the location where the output
    data should be saved.
    From configuration to full dataset path:

    <root_path>/<access>/<type>

    <root_path>
    <access> is either public or private
    <type> is raw, curated, derived

    Data that is raw is often extracted from existing servers, and it is convenient to organize it as follows:
    <server>/<database>/<dataset>

    '''

    if 'access' in dwhItemConfig.keys():
        access = dwhItemConfig['public_access']
    else:
        access = 'public'

    if 'type' in dwhItemConfig.keys():
        type = dwhItemConfig['type']
    else:
        type = 'raw'

    if 'server' not in dwhItemConfig.keys():
        raise ValueError('The configuration needs to contain the server')
    else:
        server = dwhItemConfig['server']

    if 'table' in dwhItemConfig.keys():
        database = dwhItemConfig['table'].split('.')[0]
        user = dwhItemConfig['table'].split('.')[1]
        table = dwhItemConfig['table'].split('.')[2]
        dataset = table
    elif 'dataset' in dwhItemConfig.keys():
        dataset = dwhItemConfig['dataset']
        if 'database' not in dwhItemConfig.keys():
            raise ValueError('The configuration contains the dataset but it does not contain the database')
        else:
            database = dwhItemConfig['database']
    else:
        raise ValueError('The configuration needs to contain either the table or the dataset')

    # Now we have everything to specify the path
    output_file = os.path.join(dwhRoot, access, type, server, database, dataset + '.parquet')
    _logger.debug('Output location for this dataset is %s', output_file)
    return output_file



class DWHBuilder():
    
    @staticmethod
    def _resolve_symbols(pipeline, symbols):
        for symbol, value in symbols.items():
            dict_value_str_replace(pipeline, symbol, value)
            

    def __init__(self,
                 configuration_file=None,
                 configuration_type='yaml',
                 symbols=None,
                 **kwargs):
        '''
        Data Warehouse Components Builder

        :param configuration: yaml file that contains the configuration
        :param kwargs:
        '''

        # Logging setup
        #logger = logging.getLogger(__name__)


        if configuration_file is None:
            raise ValueError('configuration_file must be specified')
        else:
            self.configuration_file = configuration_file
            _logger.debug('Configuration file = %s', self.configuration_file)

        if configuration_type is None:
            raise ValueError('configuration_type must be specified')
        else:
            self.configuration_type = configuration_type
            _logger.debug('Configuration type = %s', self.configuration_type)

        if symbols is not None:
            self._resolve_symbols(pipeline, symbols)
            
        if 'rotation' in kwargs:
            self.rotation = kwargs['rotation']
            _logger.debug('rotation = %s', self.rotation)


        with open(self.configuration_file, 'r') as cf:
            try:
                self.config_data = yaml.load(cf)
                _logger.debug(str(self.config_data))
            except yaml.YAMLError as exc:
                print(exc)



    def __str__(self):
        return pprint.pformat(self.config_data)


    def build_data(self):

        # Runs through each item
        for k, v in self.config_data['datasets'].items():
            _logger.debug('Building dataset %s', v)
            dcItem = DWHItem(v)
            dcItem.build_item()
            print(dcItem)




def run_test_ext(v=None):
    print(v)


def postproc_sec_master(**kwargs):
    _logger.info('')
    print(kwargs)

def postproc_credit_rating_hist(**kwargs):
    _logger.info('')
    print(kwargs)



class DWHItem():

    def __init__(self, dataset):
        self.configuration = dataset


    def __str__(self):
        return pprint.pformat(self.configuration)


    def build_item(self):
        self.get_data()
        self.post_process()
        pass


    def get_data(self):

        for server in self.configuration['servers']:

            config = self.configuration
            config['server'] = server

            # Gets the output location
            print(config_to_path(config, '/GAAR/bis/data/dwh'))
        pass


    def post_process(self):
        post_process_function = self.configuration['post_processing']['function']
        post_process_args = self.configuration['post_processing']['arguments']
        _logger.debug('Post-processing function = %s', post_process_function)
        _logger.debug('Post-processing args = %s', post_process_args)
        eval(post_process_function)(**post_process_args)




class DWHReader():
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

    dwh = DWHBuilder(configuration_file=os.path.join(os.path.dirname(os.path.realpath(__file__)), 'dwh.yml'))
    dwh.build_data()







