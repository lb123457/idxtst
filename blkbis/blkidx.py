'''

This is a module to create and manipulate indices.

'''


import os
import sys
import logging
import pandas


logger = logging.getLogger()


def __init__(self):
    print('Main init at the top')


class BlkIdx:


    def __init__(self, name, **kwargs):
        self.name = name

        if 'description' in kwargs:
            self.description = kwargs['description']


        # If a pandas dataframe is given, then uses it to create the index
        if 'dataframe' in kwargs:
            self.idxdata = kwargs['dataframe']


    def info(self):
        logger.debug('')
        print("Index name = " + self.name)
        if hasattr(self, 'description'):
            print("Index description = " + self.description)


    # Overloads the print statement
    def __str__(self):
        s = 'Index name = ' + self.name + '\n'
        if hasattr(self, 'description'):
            s += 'Index name = ' + self.description + '\n'

        return s




class BlkEQIdx(BlkIdx):

    def __init__(self, name, **kwargs):
        BlkIdx.__init__(self, name, **kwargs)


    def print(self):
        BlkIdx.print(self)
        print('%s is an Equity Index' % self.name)



class BlkFIIdx(BlkIdx):

    def __init__(self, name, **kwargs):
        BlkIdx.__init__(self, name, **kwargs)






if __name__ == "__main__":

    idx = BlkIdx('Top level index', description='This is a dummy index')
    idx.print()

    idx = BlkEQIdx('Equity index', description='This is a dummy equity index')
    idx.print()

    print()