'''

Data Catalog Module

'''

import os
import sys
import logging
import yaml


# Logging setup
logger = logging.getLogger()

'''
    * Pass / no pass
    * Level of abnormality
    * ...

'''



def __init__():
    logger.debug('')




class DCBuilder():

    __type__ = 'GENERIC'
    __type_description__ = 'This is a base generic test'

    def __init__(self,
                 configuration_file=None,
                 configuration_type=None,
                 **kwargs):
        '''
        Data

        :param configuration: yaml file that contains the configuration
        :param kwargs:
        '''

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


        try:
            with open(self.configuration_file) as f:
                if self.configuration_type == 'yaml':
                    self.config_data = yaml.load(f)
                else:
                    raise('Configuration type %s not recognized', self.configuration_type)
        except:
            raise ('Could not read configuration from %f', self.configuration_file)
        else:
            logger.debug('Configuration loaded from %s', self.configuration_file)
            logger.debug(str(self.config_data))






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



if __name__ == '__main__':

    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(funcName)s:%(levelname)s: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    dc_builder = DCBuilder(configuration_file='/Users/ludovicbreger/Data/DataCatalog/test_config.yml',
                           configuration_type='yaml',
                           rotation=True)
    dc_builder.build_data()

    dc_reader = DCReader(location='/Users/ludovicbreger')







