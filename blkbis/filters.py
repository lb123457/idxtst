import pandas as pd
import inspect

'''
The DFFilter class. It is used primarily for two purposes:

(1) Filter out dataframes so that columns satisfy some specific constraints.
In this case, there are options to choose to only assess whether value satisf

(2) Verify that values in a dataframe satisfy specific constraints.

A filter is essentially a function that is applied to the dataframe, enriched  with
a description of what it does to help keep track of all the processing that went into
building the dataset.

'''


class DFFilter():

    def __init__(self, function=None, description=None):

        if function is None:
            raise ValueError('"function" must be set')
        else:
            self.function = function

        if description is None:
            raise ValueError('"description" must be set')
        else:
            self.description = description


    def __str__(self):
        '''
        This defines what is showed by the print function.
        :return:
        '''

        return self.description + str(inspect.getsource(self.function))



    def filter_columns(self, df, columns=None, inplace=False, add_function_description=False, filter_results=False,
                       function_ext=None, results_ext=None):
        '''
        Uses the function that defines the filter and applies it on the list of
        columns to remove all values that do not match.

        :param df:
        :param columns:
        :return:
        '''

        if not columns:
            columns = df.columns

        if not results_ext:
            results_ext = '_filter_result'

        if not function_ext:
            function_ext = '_filter_function'

        if inplace:
            df0 = df
        else:
            df0 = df.copy(deep=True)

        for c in columns:
            df0[c + results_ext] = df[c].apply(self.function)

            if filter_results:
                df0 = df0[df0[c + results_ext]]

            if add_function_description:
                df0[c + function_ext] = str(inspect.getsource(self.function))

        if not inplace:
            return df0




    def check_columns(self, df, columns=None, abort_if_test_fails=True):
        '''
        Uses the function that defines the filter and applies it on the list of
        columns to check if all values verify the condition.

        :param df:
        :param columns:
        :return: True or False
        '''

        if not columns:
            columns = df.columns

        for c in columns:
            if False in df[c].apply(self.function):
                raise ValueError('Dataframe contains values that do not satisfy the condition')




if __name__ == '__main__':

    df = pd.DataFrame([1, 2, 3], columns=['v'])
    df_copy = df.copy(deep=True)

    f2 = DFFilter(lambda x: x >= 2, 'Dummy function 2')
    f3 = DFFilter(lambda x: x >= 3, 'Dummy function 3')

    print(df)

    f2.filter_columns(df, inplace=True, add_function_description=True)

    print(df)

    df = f3.filter_columns(df, columns=['v'], filter_results=True)

    print(df)

    f3.check_columns(df_copy, columns=['v'])



