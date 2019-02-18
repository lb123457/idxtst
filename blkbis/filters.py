import pandas as pd
import inspect
import logging
import sys
import yaml
import types


'''
The DFFilter class. It is used primarily for two purposes:

(1) Filter out dataframes so that columns satisfy some specific constraints.
In this case, there are options to choose to only assess whether value satisf

(2) Verify that values in a dataframe satisfy specific constraints.

A filter is essentially a function that is applied to the dataframe, enriched  with
a description of what it does to help keep track of all the processing that went into
building the dataset.

LB

'''

_logger = logging.getLogger(__name__)

class DFFilter():

    def __init__(self, name=None, function=None, description=None):

        if name is None:
            raise ValueError('"name" must be set')
        else:
            self.name = name

        if function is None:
            raise ValueError('"function" must be set')
        elif isinstance(function, types.LambdaType):
            self.function = function
        elif type(function) == str:
            if 'lambda' in function:
                self.function_name = self.name + '_lambda'
                self.function_def = self.name + ' = ' + function
                exec(self.function_name + ' = ' + function)
                _logger.debug('This filter uses a lambda function')
                _logger.debug(self.function_name)
                _logger.debug(self.function_def)
            else:
                function_def = function
                exec(function_def)
                _logger.debug(function_def)

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




class FiltersBuilder():

    def __init__(self,
                 configuration_file=None,
                 **kwargs):

        if configuration_file is None:
            raise ValueError('configuration_file must be specified')
        else:
            self.configuration_file = configuration_file
            _logger.debug('Configuration file = %s', self.configuration_file)


        with open(self.configuration_file, 'r') as cf:
            try:
                self.config_data = yaml.load(cf)
                _logger.debug(str(self.config_data))
            except yaml.YAMLError as exc:
                print(exc)


    def build_filters(self):

        # Runs through each item
        for k, v in self.config_data['filters'].items():
            _logger.debug('Building filter %s:\n%s', k, v)
            filter = DFFilter(k, v['function'], v['description'])





if __name__ == '__main__':

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(funcName)s:%(levelname)s: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    df = pd.DataFrame([1, 2, 3], columns=['v'])
    df_copy = df.copy(deep=True)

    f2 = DFFilter(name='filter1', function=lambda x: x >= 2, description='Dummy function 2')
    f3 = DFFilter('filter2', lambda x: x >= 3, 'Dummy function 3')

    print(df)

    f2.filter_columns(df, inplace=True, add_function_description=True)

    print(df)

    df = f3.filter_columns(df, columns=['v'], filter_results=True)

    print(df)

    try:
        f3.check_columns(df_copy, columns=['v'])
    except:
        pass


    fb = FiltersBuilder(configuration_file='/Users/ludovicbreger/PycharmProjects/idxtst/blkbis/filters.yml')
    fb.build_filters()

    s = 'f = ' + fb.config_data['filters']['filter2']['function']


    # These variables are replaced at run time
    yml_str = '''
    metadata:
        - description: This is a test file for filter configuration
        - business: BDS
    
    
    filters:
    
        filter1:
            function: 'lambda x: x < 0'
            description: Ludo1
    
        filter2:
            function: >
                      def retain_positive(x):
                         return 'Ludo'
            description: Ludo2
    '''
    import yaml
    config_data = yaml.load(yml_str)
    f1_str = config_data['filters']['filter1']['function']
    exec('f1 = ' + f1_str)

    f2_str = config_data['filters']['filter2']['function']
    exec(f2_str)

    print(f1_str)
    print(f2_str)
    print(f1(1))
    print(retain_positive(1))



