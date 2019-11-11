""" An API that creates chainable methods that modify a schema object. Based on a simplfied version found in pydash.
These are used inside the dask worker functions. Any errors are re-raised
Ex.
response = {
    'a': 1,
    'b':
}
chain = Chain().do_thing().do_another_thing()
result = chain(response)
"""
import traceback
import collections
import functools
import inspect
import copy

from pprint import pprint
from .exceptions import ChainArgValueError, ChainWrapperError, JoinKeyError, InvalidMethodError


class ChainableMappingProxy(collections.abc.MutableMapping):
    """ This is a proxy to a dict_ object. Its purpose is simply to create 
    a type that our chainable API will expect
    """
    def __init__(self, dict_={}):
        self._dict = dict_

    @property 
    def dict_(self):
        return self._dict 


    def __getitem__(self, key):
        return self._dict[key]


    def __len__(self):
        return len(self._dict)

    
    def __iter__(self):
        return iter(self._dict)


    def __setitem__(self, key, value):
        self._dict[key] = value


    def __delitem__(self, key):
        del self._dict[key]


    def __repr__(self):
        return self._dict.__repr__()


class Chain(object):
    """ A chain excepts a schema object (dict or list of dicts) and performs an operation on that object.
    A chain must either work on a single object or a list of objects but not both.
    """

    module = None

    def __init__(self, schema=None):
        self._schema = schema

        
    @classmethod
    def get_method(cls, name):
        """Return valid 'module' method."""
        method = getattr(cls.module, name, None)
        if not callable(method):
            raise InvalidMethodError('Invalid gen method: {0}'.format(name))

        return method

    def __getattr__(self, attr):
        """Proxy attribute access to the module and return a method wrapped in a _Chainable
        """
        return _Chainable(self._schema, self.__class__.get_method(attr), self.__class__)

    def __call__(self, value):
        """Return result of passing 'value' through chained methods."""
        if isinstance(self._schema, _Chainable):
            value = self._schema.unwrap(value)
        return value

class _Chainable(object):
    """ Wraps a Chain method call within a context
    """

    def __init__(self, schema, method, chain_class):
        self._schema = schema
        self.method = method
        self.chain_class = chain_class
        self.args = ()
        self.kwargs = {}

    def _copy_self(self):
        """Generate a copy of this instance."""
        new = self.__class__.__new__(self.__class__)
        new.__dict__ = self.__dict__.copy()
        return new

    def unwrap(self, schema=None):
        """Execute 'method' with '_schema', 'args', and 'kwargs'. If '_schema' is
        an instance of 'ChainWrapper', then unwrap it before calling 'method'.
        """
        chainable = self._copy_self()  # do not freeze the schema... allows late passing
        if isinstance(chainable._schema, _Chainable):
            chainable._schema = chainable._schema.unwrap(schema)

        elif not isinstance(schema, _Chainable) and schema is not None:
            chainable._schema = schema

        if chainable._schema is not None:
            schema = chainable._schema

        res = chainable.method(schema, *chainable.args, **chainable.kwargs)
        return ChainableMappingProxy(res)


    def __call__(self, *args, **kwargs):
        """Invoke the 'method' with 'value' as the first argument and return a
        new 'Chain' object with the return value.
        """
        self.args = args
        self.kwargs = kwargs

        return self.chain_class(schema=self)



def _reconcile_non_default_args(argspec):
    """ take an argspec and pop any that do not contain a default
    """
    args = argspec.args
    if argspec.defaults is not None:
        for i in range(len(argspec.defaults)):
            args.pop()
    return {a:[] for a in args}


def _create_args(proxy, argspec):
    """ proxy -> ChainableMappingProxy (dict_)
    """
    args = _reconcile_non_default_args(argspec)
    for arg, lst in args.items():
        if arg not in proxy:
            raise ChainArgValueError('The arg {} was not present in the current schema'.format(arg))
        else:
            if isinstance(proxy[arg], list):
                args[arg] += proxy[arg]
            else:
                args[arg] = proxy[arg]
    return args


# def _update_proxy(proxy, output_data, join_key, result_key):
#     """ given the origin schema_lst and new output_data, write the updated keys into the new dict on the specified result key. The join
#     key is used to bind the data together. This should be the only method where the state of the schema data is manipulated.
#     """

#     for schema in proxy:
#         try:
#             key = schema[join_key]  # get the bdbid or other distinct value
#             schema[result_key] = [rec for rec in output_data if rec[join_key] == key]  # filter all recs on this key and extend the input-schema
#         except KeyError as err:
#             raise JoinKeyError('The join key "{}" was not found'.format(join_key)) from err

#     return proxy


def chained(result_key):
    """ A decorator that takes lists of 1:n schema dumped by the application. It essentially handles chainable errors and manipulates the
    state of the schema list that was provided by the application. The needed input data is extracted from all schemas in _create_args.
    After the mapped method is run, new data is appended to the schema_lst via _update_schema_list
    If an error occurred on the chain, the schema_lst is appended to err.context. This lets us expose a snapshot of the data run up until that
    point, so if we are running a long chain, the data that was successfully run is preserved. These errors can be persisted in the database for
    tracking purposes
    """

    def _decorator(method):

        @functools.wraps(method)
        def _inner(*args, **kwargs):

            if not isinstance(args[0], ChainableMappingProxy):  # if we're not in a chain simply treat like a normal function
                return method(*args, **kwargs)

            try:
                proxy = args[0]
                argspec = inspect.getfullargspec(method)
                args_dict = _create_args(proxy, argspec) #, categorical)  # build a dict of args k,v pairs
                all_args ={**args_dict, **kwargs} # combine with current kwargs
                output_data = method(**all_args)  #output a list of records
               # updated_proxy = _update_proxy(proxy, output_data)  # merge into original schema_list
                proxy[result_key] = output_data

            except Exception as err:
                e = ChainWrapperError('There was an error in the chain. Access "context" var for the accumulated schema')
                e.context = {
                    'current_schema': proxy.dump(),
                    'point_of_failure': method.__name__,
                    'error': str(err),
                    'traceback': traceback.format_exc()
                }
                raise e from err
            return proxy
        return _inner
    return _decorator


# def get_chained(chained_func):
#     """ simple helper to fetch the wrapped chain function for testing or standalone purposes without having
#     to touch the internals.
#     """
#     return chained_func.__wrapped__