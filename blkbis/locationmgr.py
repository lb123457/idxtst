import os

def autoproperty(attribute_name, can_get=True, can_set=True, allow_null=False, default_value=0):
    def getter(self):
        return getattr(self, attribute_name, default_value)

    def setter(self, value):
        if not allow_null and value is None:
            raise ValueError('Cannot set {} to None'.format(name))
        setattr(self, attribute_name, value)

    return property(getter if can_get else None, setter if can_set else None)




class Directories(object):
    """
    Implements the standards on how to organize directories and files
    """

    objects = []

    def __init__(self, root=None, group=None):
        """
        root is the top-level directory under which all files
        for the process are stored

        group is model, index or data
        """

        self._root = root
        self._group = group

        if self._group == 'model':
            self._raw = os.path.join(root, 'raw')
            self._curated = os.path.join(root, 'curated')
        elif self._group == 'index':
            self._raw = os.path.join(root, 'raw')
        elif self._group == 'data':
            self._raw = os.path.join(root, 'raw')
        else:
            raise ValueError("'group' parameter value '%s' should be in ['model', 'index', 'raw']" % self._group)

        if len(self.objects) == 0:
            self.objects.append(self)

    curated = autoproperty('_curated', can_get=True, can_set=True, allow_null=False, default_value=0)

    @property
    def root(self):
        return self._root

    @property
    def group(self):
        return self._group

    @property
    def raw(self):
        return self._raw

    @classmethod
    def manager(self, name=None):
        return self.objects[0]


def test_function1():
    bisdir1 = Directories(root='/dwh', group='model')


def test_function2():
    dm = Directories.manager()
    print(dm.root)
    print(dm.raw)
    print(dm.curated)


def test_function3():
    dm = Directories.manager()
    print(dm)


test_function1()
test_function2()
test_function3()