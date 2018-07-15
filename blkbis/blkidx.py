'''

This is a module to create and manipulate indices.

'''


import os
import sys
import logging



def __init__(self):
    print('Main init at the top')


class BlkIdx:


    def __init__(self, name, **kwargs):
        self.name = name

        if 'description' in kwargs:
            self.description = kwargs['description']


    def print(self):
        print("Index name = " + self.name)
        if hasattr(self, 'description'):
            print("Index description = " + self.description)



class BlkEquityIdx(BlkIdx):


    def __init__(self, name, **kwargs):
        BlkIdx.__init__(self, name, **kwargs)



if __name__ == "__main__":

    idx = BlkIdx('Top level index', description='This is a dummy index')
    idx.print()

    idx = BlkEquityIdx('Equity index', description='This is a dummy equity index')
    idx.print()