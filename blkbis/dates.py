import datetime
import logging

def create_date_series(start_date,
                       end_date,
                       data_frequency = 'MONTHLY',
                       refix_frequency = 'ANNUAL',
                       first_refix=None,
                       **kwargs):

    print(start_date)


    ds = DatesSeries(start_date,
                     end_date,
                     data_frequency,
                     refix_frequency,
                     first_refix,
                     **kwargs)

    return ds.build_time_series()


class DatesSeries:

    'Sample class that creates historical index for performance testing'

    def __init__(self,
                 start_date,
                 end_date,
                 data_frequency = 'MONTHLY',
                 refix_frequency = 'ANNUAL',
                 first_refix=None,
                 **kwarg):

        logging.debug('')

        self.start_date = start_date
        self.end_date = end_date
        self.data_frequency = data_frequency
        self.refix_frequency = refix_frequency
        self.first_refix = first_refix

        # Number of days between two consecutive dates
        if 'ndays' in kwarg.keys():
            self.ndays = kwarg['ndays']
        else:
            self.ndays = 1


    def show_info(self):
        logging.debug('')
        print('Start Date       = ' + str(self.start_date))
        print('End Date         = ' + str(self.end_date))
        print('Data Frequency   = ' + self.data_frequency)
        print('Refix Frequency  = ' + self.refix_frequency)


    def build_time_series(self):
        logging.info('Building time series...')

        running_date = self.start_date
        date_list = [running_date]

        while running_date < self.end_date:
            running_date = running_date + datetime.timedelta(days=self.ndays)
            logging.debug(running_date)
            date_list.append(running_date)

        logging.debug(str(date_list))

        return  date_list

