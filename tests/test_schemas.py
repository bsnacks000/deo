import pytest 

from curiorpc.schemas import ContextData, JsonRPCSchema


def test_contextdata():
    c = ContextData()
    assert c.jsonrpc == '2.0'
    assert hasattr(c, 'params')
    assert hasattr(c, 'id')
    assert hasattr(c, 'method')

    d = c.to_dict()
    assert d is not vars(c)