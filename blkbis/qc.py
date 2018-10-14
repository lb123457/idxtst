'''

QC utilities

'''

import os
import sys
import logging
import pandas
import logging
from blkbis import blkidx
from blkbis import tstdata

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


    # Saves the check definition in the persistence layer
    def upload_check_definition(self):
        pass


    # Retrieves the check results in the persistence layer
    def download_check_definition(self):
        pass





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





class XRayQCCheck(QCCheck):

    __type__ = 'XRAY'
    __type_description__ = 'This check runs a comprehensive set of checks'


if __name__ == "__main__":

    import pandas as pd
    import numpy as np
    import random
    import string

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


    for name, group in df.groupby('I'):
        print(name)
        print(group)
        print(type(group))




    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(funcName)s:%(levelname)s: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    qc1 = QCCheck(id='Gen1', description='This is a generic dummy check')
    print(qc1)

    qc2 = TimeSeriesQCCheck(id='Gen2', description='This is a generic time series check')
    print(qc2)




