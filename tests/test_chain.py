import pytest  
import unittest 

ut = unittest.TestCase()

from aiorpc import chain  
from . import fake_chains

class FakeChain(chain.Chain):
    module = fake_chains


def test_chainable_mapping_proxy():
    d = {'a': 1, 'b': 2}
    proxy = chain.ChainableMappingProxy(d) 

    assert proxy['a'] == 1 
    assert proxy['b'] == 2
    assert len(d) == 2 

    items = [v for k,v in d.items()]
    d['hep'] = 'tup'

    del d['b']


