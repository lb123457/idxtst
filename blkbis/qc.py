'''

QC utilities

'''

import os
import sys
import logging
import pandas
import logging

logger = logging.getLogger()


def __init__():
    logger.debug('')


def full_qc(idx):
    logger.debug('Running full QC')



class QCCheck:

    __type__ = 'GENERIC'
    __type_description__ = 'This is a base generic test'

    def __init__(self,
                 id=None,
                 description=None,
                 **kwargs):
        '''
        This is the base class for all QC Checks classes

        Checks to make sure all arguments are initialized.

        :param name:
        :param kwargs:
        '''

        if id is None:
            raise ValueError('id must be specified')
        else:
            self.id = id

        if description is None:
            raise ValueError('description must be specified')
        else:
            self.description = description


    # Overloads the print statement
    def __str__(self):
        s =  'Id               = ' + self.id + '\n'
        s += 'Description      = ' + self.description + '\n'
        s += 'Type             = ' + self.__type__ + '\n'
        s += 'Type description = ' + self.__type_description__ + '\n'

        return s




class TimeSeriesQCCheck(QCCheck):
    '''
    This is the base class for all Time Series QC Checks classes
    '''

    __type__ = 'TIME_SERIES'
    __type_description__ = 'This check uses a times series to identify anomalous values'

    def __init__(self,
                 id=None,
                 description=None,
                 **kwargs):

        QCCheck.__init__(self, id, description, **kwargs)




class CrossSectionalQCCkeck(QCCheck):

    __type__ = 'CROSS_SECTIONAL'
    __type_description__ = 'This check uses a cross-sectional analysis to identify anomalous values'

    def __init__(self,
                 id=None,
                 description=None,
                 **kwargs):

        QCCheck.__init__(self, id, description, **kwargs)





class ValueQCCkeck(QCCheck):


    __type__ = 'VALUE_BASED'
    __type_description__ = 'This check uses a rule and value to identify anomalous values'

    def __init__(self,
                 id=None,
                 description=None,
                 **kwargs):

        QCCheck.__init__(self, id, description, **kwargs)





if __name__ == "__main__":

    qc1 = QCCheck(id='Gen1', description='This is a generic dummy check')
    print(qc1)

    qc2 = TimeSeriesQCCheck(id='Gen2', description='This is a generic time series check')
    print(qc2)