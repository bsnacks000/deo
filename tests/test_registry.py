import pytest 
import unittest  


from curiorpc.registry import inspect_function, _RegistryEntry, \
    create_entrypoint, EntrypointRegistry
from curiorpc.exceptions import RegistryEntryError

tc = unittest.TestCase()

import marshmallow as ma  

class RegistryTestSchema(ma.Schema):
    a = ma.fields.Integer(default=42)


def test_inspect_function():
    
    async def testfunc(a,b, c=42):
        return a, b, c  
    
    f = inspect_function(testfunc) 
    assert f['name'] == 'testfunc'
    tc.assertListEqual(['a', 'b', 'c'], f['args'])


def test_create_entrypoint():
    
    async def testfunc(a,b, c=42):
        return a, b, c  

    ep = create_entrypoint(testfunc, schema_name='RegistryTestSchema')
    assert isinstance(ep, _RegistryEntry)
    assert isinstance(ep.schema_class(), RegistryTestSchema)


def test_entrypoint_registry():

    registry = EntrypointRegistry()

    @registry.register('RegistryTestSchema')
    async def testfunc(a, b, c=42):
        return a, b, c  
    
    test = registry.get_entrypoint('testfunc')
    assert isinstance(test, _RegistryEntry)
    assert test.func is testfunc
    
    with pytest.raises(RegistryEntryError):
        registry.get_entrypoint('blah')
