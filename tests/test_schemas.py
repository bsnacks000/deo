import pytest
import unittest 
import marshmallow as ma  

tc = unittest.TestCase()

from curiorpc.schemas import ContextData, JsonRPCSchema



class AddParamsSchema(ma.Schema):
    a = ma.fields.Integer()
    b = ma.fields.Integer()


class AddResultSchema(ma.Schema):
    c = ma.fields.Integer()


class AddSchema(JsonRPCSchema):
    params = ma.fields.Nested(AddParamsSchema)
    result = ma.fields.Nested(AddResultSchema)


def test_contextdata():
    c = ContextData()
    assert c.jsonrpc == '2.0'
    assert hasattr(c, 'params')
    assert hasattr(c, 'id')
    assert hasattr(c, 'method')

    d = c.to_dict()
    assert d is not vars(c)


def test_jsonrpc_schema():

    a = AddSchema()
    
    test = {'id': 1, 'method': 'method', 'jsonrpc': '2.0', 'params': {'a': 1, 'b': 2}}
    obj = a.load(test)
    assert isinstance(obj, ContextData)
    tc.assertDictEqual(test['params'], obj.params) 
    assert obj.method == test['method']

    obj.result = {'c': 3}
    dumped = a.dump(obj)
    tc.assertDictEqual({'id': 1, 'jsonrpc': '2.0', 'result': {'c': 3}}, dumped)

    # cover testing string id  
    test = {'id': "blah", 'method': 'method', 'jsonrpc': '2.0', 'params': {'a': 1, 'b': 2}}
    obj = a.load(test)
    assert obj.id == 'blah'
    a.dump(obj)

    test = {'id': None, 'method': 'method', 'jsonrpc': '2.0', 'params': {'a': 1, 'b': 2}}
    obj = a.load(test)
    assert obj.id is None 
    a.dump(obj)

    with pytest.raises(ma.ValidationError):  # bad rpc 
        test = {'id': None, 'method': 'method', 'jsonrpc': '42.0', 'params': {'a': 1, 'b': 2}}
        obj = a.load(test)

    with pytest.raises(ma.ValidationError):
        a.dump(['not the right thing...'])