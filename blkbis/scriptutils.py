import sys
import re
import functools
import logging

"""
The idea behind this is to create a model by running a succession of functions
defined in the body of the script. In the course of building a model, particularly 
during the research phase, or when needing to rerun specific parts of the script
that may have failed and/or because data has changed, it may be useful to select
what to rerun by passing a command line argument. 
"""


# Logging setup
_logger = logging.getLogger()

steps_to_be_executed = []

def is_step(f):
    @functools.wraps(f)
    def func(*args, **kwargs):
        if f.__name__ in steps_to_be_executed:
            return f(*args, **kwargs)

    return func


@is_step
def f1(s):
    print('function1', s)


@is_step
def f2(s):
    print('function2', s)


@is_step
def f3(s):
    print('function2', s)


def set_steps():
    global steps_to_be_executed

    with open(__file__, 'r') as f:
        script = f.read()

    step_list = re.findall(r'@is_step[ \n]*def (.+)[(]', script)
    steps = 'f1-f2'

    #steps_to_be_executed = []
    if ',' in steps:
        steps_to_be_executed = steps_to_be_executed + steps.split(',')
    elif '-' in steps:
        include = False
        start_step = steps.split('-')[0]
        end_step = steps.split('-')[1]

        for s in step_list:
            if s == start_step or include:
                steps_to_be_executed.append(s)
                include = True
            if s == end_step:
                include = False
    else:
        steps_to_be_executed = steps

    _logger.info('The steps that will be executed are %s', steps_to_be_executed)


if __name__ == "__main__":

    _logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(funcName)s:%(levelname)s: %(message)s')
    ch.setFormatter(formatter)
    _logger.addHandler(ch)

    set_steps()

    f1('')
    f2('')
    f3('')