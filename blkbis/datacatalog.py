'''

Data Catalog Module

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


    print(pick_random_sector())







