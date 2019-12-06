import pytest  
import unittest 

tc = unittest.TestCase()

from aiorpc import chain  
from . import fake_chains


from pprint import pprint 


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


def test_chain_runs_normal():

    d = {'a':1, 'b': 2}
    proxy = chain.ChainableMappingProxy(d)

    calc = FakeChain().make_c_from_ab(some_val=1).make_d_from_c(some_val=2).make_e()
    res = calc(proxy)

    expected = {'a': 1, 'b': 2, 'c': 4, 'd': 6, 'e': 13}
    
    res = res.dump()
    tc.assertDictEqual(expected, res)


def test_raises_chainwrapper_error():
    proxy = chain.ChainableMappingProxy()
    calc = FakeChain().bad()

    with pytest.raises(chain.ChainWrapperError):
        calc(proxy)
    try:
        calc(proxy)
    except chain.ChainWrapperError as err:
        assert 'current_schema' in err.context 
        assert err.context['error'] == 'boo'