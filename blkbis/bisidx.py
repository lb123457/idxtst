'''

This is a module to create and manipulate indices.

'''


import os
import sys
import logging
import pandas as pd


logger = logging.getLogger()


def __init__(self):
    print('Main init at the top')


#######################################################################


class BISUniverse(object):

    @staticmethod
    def from_file():
        pass
    
    def __init__(self):
        pass
    

    """
    Container for a point-in-time universe
    """





class BISUniverseTimeSeries(BISUniverse):
    """
    Container for a set of point-in-time universes.
    Class variables:
    - universe id
    - universe name
    - universe description
    - asset class
    - asset type
    - market
    - start_date
    - end_date
    - frequency
    - rebalance rule identifier
    - recomposition rule identifier
    """

    def __init__(self):
        pass


class BISFIUniverse(BISUniverse):

    def __init__(self):
        pass

    """
    Container for a Fixed Income point-in-time universe
    """

    def get_secmaster(self, date, **kwargs):
        pass


    def get_ratings(self, date, **kwargs):
        pass


    def combine_data(self):
        pass
    
    
class BISEQUniverse(BISUniverse):

    def __init__(self):
        pass

    """
    Container for an Equity point-in-time universe
    """


#######################################################################


class BISIndexTimeSeries(BISUniverseTimeSeries):
    """
    Container for a set of universes each associated with a date.
    Class variables:
    - start_date
    - end_date
    - frequency
    - ...
    """

    def __init__(self):
        pass


    def build_timeseries(self):
        """
        Loops through dates
        :return:
        """
        pass



class BISIndex(BISUniverse):

    """
    Parent class for indices. Class variables:
    - id
    - name
    - description
    - date
    """

    @staticmethod
    def create_from_pickled(file, **kwargs):
        pass


    @staticmethod
    def create_from_parquet(file, **kwargs):
        pass


    @staticmethod
    def create_from_dataframe(df, **kwargs):
        pass


    @staticmethod
    def create_from_aladdin(index_code, **kwargs):
        return BISIndex()
        pass



    def __init__(self, **kwargs):

        if 'id' in kwargs:
            self._id = kwargs['id']

        if 'name' in kwargs:
            self._name = kwargs['name']

        if 'description' in kwargs:
            self._description = kwargs['description']

        # If a pandas dataframe is given, then uses it to create the index
        if 'dataframe' in kwargs:
            self.idxdata = kwargs['dataframe']



    @property
    def id(self):
        return self._id


    @id.setter
    def id(self, value):
        if type(value) == str:
            self._id = value
        else:
            raise ValueError("'id' must be a string")


    @property
    def name(self):
        return self._name


    @name.setter
    def name(self, value):
        if type(value) == str:
            self._name = value
        else:
            raise ValueError("'name' must be a string")


    @property
    def description(self):
        return self._description


    @name.setter
    def description(self, value):
        if type(value) == str:
            self._description = value
        else:
            raise ValueError("'description' must be a string")



class BISEQIndex(BISIndex, BISEQUniverse):

    def __init__(self, **kwargs):
        BISIndex.__init__(self, **kwargs)




class BISFIIndex(BISIndex, BISFIUniverse):

    @staticmethod
    def from_dataframe(df):
        idx = BISFIIndex(df)
        return idx

    def __init__(self, df, **kwargs):
        BISIndex.__init__(self, **kwargs)
        self.df = df







#######################################################################


if __name__ == "__main__":

    idx = BISIndex(name='Top level index', description='This is a dummy index')

    idx = BISEQIndex(name='Equity index', description='This is a dummy equity index')

    idx = BISIndex.create_from_aladdin(index_code='')

    idx = BISFIIndex.from_dataframe(pd.DataFrame())

    print(idx)