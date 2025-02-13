import collections
from marshmallow import class_registry as ma_class_registry
from .exceptions import RegistryEntryError
import inspect




_RegistryEntry = collections.namedtuple('_RegistryEntry', ['func', 'name', 'args', 'schema'])


def inspect_function(func):
    """ and return a dict keyed with its properties and method signatures in order to perform dynamic mapping.
    """
    argspec = inspect.getfullargspec(func)
    # print(argspec)
    return {
        'name': func.__name__,
        'args': argspec.args,
    }


def create_entrypoint(func, schema_class=''):
    """ Create a new entry class.
    """
    funcmeta = inspect_function(func)
    if isinstance(schema_class, str):
        schema_class = ma_class_registry.get_class(schema_class) # <-- fetch schema class here
    
    return _RegistryEntry(
        func=func,
        args=funcmeta['args'],
        name=funcmeta['name'],
        schema_class=schema_class(),
    )


class EntrypointRegistry(object):
    """ Holds a registry of methods(entrypoints) for the application. 
    """

    _registry = {}

    def get_entrypoint(self, func):
        """ Fetch an entrypoint from the registry.
        """
        try:
            return self._registry[func]   # <--- raises a KeyError if not found but possibly should be wrapped.
        except KeyError as err:
            raise RegistryEntryError('The entrypoint {} was not found in the registry.'.format(func))


    def _update_registry(self, entry):
        """ Update the registry using both func and funcname as keys that point to the same entry.
        """
        self._registry[entry.name] = entry


    def register(self, schema_name):
        """ Creates an entrypoint for an application in the registry. These are the coroutine handlers 
        that the application uses.
        """
        def _constructor(coro):
            entry = create_entrypoint(coro, schema_name)
            self._update_registry(entry) # maybe update registry if it is a new entry
            return coro
        return _constructor

