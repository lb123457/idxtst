
import os
import logging
import pandas as pd
import datetime
from shutil import copyfile

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from blkbis import qc
from blkbis import tstdata


# Set up logging to file - see previous section for more details
logging.basicConfig(level=logging.DEBUG)

formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

# Define a handler which writes INFO messages or higher to the sys.stderr
console_hdlr = logging.StreamHandler()
console_hdlr.setFormatter(formatter)

_logger = logging.getLogger()
_logger.handlers = []
_logger.addHandler(console_hdlr)


def rotate_file(file, **kwargs):
    """
    Takes a file and make a copy of it according to some rules defined in
    **kwargs. The purpose of this is to keep a copy of a file that will be
    overwritten. Ths is particularly useful for logs and data files.
    """

    file_basename = os.path.basename(file)

    if os.path.dirname(file):
        file_dirname = os.path.dirname(file)
    else:
        file_dirname = '.'

    if 'clean_old_files' in kwargs and kwargs['clean_old_files'] != 'none':
        _logger.debug('Cleaning old files')

        files = [os.path.join(file_dirname, f) for f in os.listdir(file_dirname) if
                 os.path.isfile(os.path.join(file_dirname, f)) and f.startswith(file_basename) and '.back.' in f]

        if kwargs['clean_old_files'] == 'all':
            # Remove all log files except for the root one, which will be backed up
            for lf in files:
                if lf != file:
                    _logger.debug('Removing %s', lf)
                    os.remove(lf)
        elif kwargs['clean_old_files'] == 'keep_latest_daily':
            # Keeps the latest daily files
            pass

    if 'check_latest_backup' in kwargs and kwargs['check_latest_backup']:
        check_latest_backup(file, **kwargs)

    now_str = datetime.datetime.now().strftime('%Y-%m-%d:%H:%M:%S')
    src_file = os.path.join(file_dirname, file)
    dst_file = os.path.join(file_dirname, file + '.back.' + now_str)

    if os.path.isfile(src_file):
        _logger.debug('Backing up %s to %s', src_file, dst_file)
        copyfile(src_file, dst_file)

    return src_file


def check_latest_backup(file, **kwargs):
    last_backup = get_latest_backup(file, **kwargs)

    if last_backup is None:
        return

    _logger.debug('Checking latest backup %s of file %s', last_backup, file)
    df_file = load_file(file)
    df_backup = load_file(last_backup)

    file_comparator = FileComparator(file, last_backup)

    _logger.debug('File and backup have the same columns = %r', file_comparator.have_same_columns())
    _logger.debug('File and backup have the same rowcount = %r', file_comparator.have_same_rowcount())

    print(df_file.dtypes)
    print(df_backup.dtypes)


def get_latest_backup(file, **kwargs):
    """
    Takes a file, looks for backups and reports differences:
    - Large changes in size
    - Changes in file structure
    - ...
    """

    file_basename = os.path.basename(file)

    if os.path.dirname(file):
        file_dirname = os.path.dirname(file)
    else:
        file_dirname = '.'

    files = [os.path.join(file_dirname, f) for f in os.listdir(file_dirname) if
             os.path.isfile(os.path.join(file_dirname, f)) and f.startswith(file_basename) and '.back.' in f]

    if len(files) == 0:
        return None

    # Picks up the back up that has the latest date
    df = pd.DataFrame(files, columns=['filename'])

    formats = {'%Y-%m-%d:%H:%M:%S': '\d{4}-\d{2}-\d{2}[:]\d{2}[:]\d{2}[:]\d{2}$',
               '%Y-%m-%d': '\d{4}-\d{2}-\d{2}[:]\d{2}[:]\d{2}[:]\d{2}$'}

    for format, regexp in formats.items():
        _logger.debug('Trying to extract date using format %s and regexp %s', format, regexp)
        if False not in df['filename'].astype(str).str.contains(regexp).tolist():
            try:
                df['backup_datetime_string'] = df['filename'].str.extract('(' + regexp + ')').str.strip()
                df['backup_datetime'] = pd.to_datetime(df['backup_datetime_string'], format=format)
                _logger.debug('Successful')
            except:
                _logger.warning('Could not convert')

    df.sort_values(by=['backup_datetime'], inplace=True)
    last_backup = df.iloc[-1]['filename']
    _logger.debug('Last available backup is %s', last_backup)

    return last_backup


def load_file(file, **kwargs):
    """
    Generic class to load a file into a dataframe.
    """
    if file.endswith('.csv'):
        return pd.read_csv(file, **kwargs)
    elif file.endswith('.parquet'):
        return pd.read_parquet(file, **kwargs)
    elif '.parquet.back.' in file:
        return pd.read_parquet(file, **kwargs)
    else:
        _logger.error('Extension not supported')


class FileComparator(object):

    def __init__(self, file1, file2, **kwargs):

        if not os.path.isfile(file1):
            raise FileNotFoundError('File %s does not exist', file1)
        if not os.path.isfile(file2):
            raise FileNotFoundError('File %s does not exist', file2)

        _logger.debug('%s & %s', file1, file2)

        self.file1 = file1
        self.file2 = file2

        self.df_file1 = load_file(file1)
        self.df_file2 = load_file(file2)

        self.df_comparator = DFComparator(self.df_file1, self.df_file2)

    def have_same_columns(self):
        return self.df_comparator.have_same_columns()

    def have_same_rowcount(self):
        return self.df_comparator.have_same_rowcount()



if __name__ == '__main__':

    # Example of how to use the above

    # Set up logging to file - see previous section for more details
    logging.basicConfig(level=logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

    # Define a handler which writes INFO messages or higher to the sys.stderr
    console_hdlr = logging.StreamHandler()
    console_hdlr.setFormatter(formatter)

    # Define a handler which writes to a file
    logfile = rotate_file('test.log', clean_old_files='all')
    file_hdlr = logging.FileHandler(logfile, mode='w')
    file_hdlr.setFormatter(formatter)

    _logger = logging.getLogger()
    _logger.handlers = []
    _logger.addHandler(console_hdlr)
    _logger.addHandler(file_hdlr)

    _logger.info('test')
    _logger.debug('test')

    # Example of how to rotate a data file
    df1 = pd.DataFrame({'one': [-1, np.nan, 2.5],
                        'two': ['foo', 'bar', 'baz'],
                        'three': [True, False, True]},
                       index=list('abc'))

    table = pa.Table.from_pandas(df1)

    pq.write_table(table, rotate_file('example.parquet', clean_old_files='none', check_latest_backup=True))

    df2 = pd.DataFrame({'one': [-1, np.nan, 2.5],
                        'two': ['foo', 'bar', 'baz'],
                        'four': [True, False, True]},
                       index=list('abd'))

    table = pa.Table.from_pandas(df2)

    pq.write_table(table, rotate_file('example.parquet', clean_old_files='none', check_latest_backup=True))