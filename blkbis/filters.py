'''
Package used to do either filter out dataframe values based
or verify that values are within certain bounds. In both cases,
this boils down to analyzing the value in a set of columns

'''


class DFFilter():

    def __init__(self, function=None, description=None):

        if function is None:
            raise ValueError('"function" must be set')
        else:
            self.function = function

        if description is None:
            raise ValueError('"description" must be set')



    def filter_columns(self, df, columns=None, inplace=False):
        '''
        Uses the function that defines the filter and applies it on the list of
        columns to remove all values that do not match.

        :param df:
        :param columns:
        :return:
        '''

        for c in columns:
            df[c] = df[c].apply(self.function)



    def check_columns(self, df, columns=None):
        '''
        Uses the function that defines the filter and applies it on the list of
        columns to check if all values verify the condition.

        :param df:
        :param columns:
        :return: True or False
        '''