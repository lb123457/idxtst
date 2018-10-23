import logging
import pandas as pd

logger = logging.getLogger()


def updateDateTypes(df):
    '''
    Takes a pandas dateframe and update columns that look like datetimes.

    :param df: input dataframe
    :return: None
    '''

    for c in df.columns:
        guessDateTypes(df, c)


def guessDateTypes(df, c):
    '''
    Takes a pandas dataframe and column c and updates its type to datetime if possible.

    :param df: input dataframe
    :return: None
    '''

    formats = {'%Y-%m-%d': '^[1-2]\d{3}-\d{2}-\d{2}',
               '%m/%d/%Y': '^\d{2}\/\d{2}\/[1-2]\d{3}',
               '%Y%m%d': '^[1-2]\d{3}\d{2}\d{2}'
    }

    # For this format to work we need to make sure that the format matches
    # as integers will convert just fine

    for format, regexp in formats.items():

        print('Trying to convert column "%s" to datetime using format %s' % (c, format))
        if False not in df[c].astype(str).str.contains(regexp).tolist():
        #if False not in df[c].astype(str).str.contains('^[1-2]\d{3}-\d{2}-\d{2}').tolist():
            try:
                #df[c] = df[c].astype('datetime64[ns]')
                df[c] = pd.to_datetime(df[c], format=format)

            except:
                print('Type conversion failed')
            else:
                print('\n>>> Converted column %s to datetime using format %s\n' % (c, format))
                return
        else:
            print('Format %s does not match values' % (format))