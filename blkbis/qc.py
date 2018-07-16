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


    def __init__(self,
                 id=None,
                 description=None,
                 **kwargs):
        '''

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


    def info(self):
        logger.debug('')
        print("Index name = " + self.name)
        if hasattr(self, 'description'):
            print("Index description = " + self.description)


    # Overloads the print statement
    def __str__(self):
        s = ''
        if hasattr(self, 'id'):
            s += 'Id = ' + self.id + '\n'
        if hasattr(self, 'description'):
            s += 'Description = ' + self.description + '\n'

        return s




class TimeSeriesQCCheck(QCCheck):

    def __init__(self):
        QCCheck.__init__(self, id='TS_CHECK', description='Timeseries check')



class CrossSectionalQCCkeck(QCCheck):

    def __init__(self):
        QCCheck.__init__(self, id='CS_CHECK', description='Cross-sectional check')



class ValueQCCkeck(QCCheck):

    def __init__(self):
        QCCheck.__init__(self, id='VALUE_CHECK', description='Value check')








if __name__ == "__main__":

    idx = BlkIdx('Top level index', description='This is a dummy index')
    idx.print()

    idx = BlkEQIdx('Equity index', description='This is a dummy equity index')
    idx.print()

    print()