import pytest 
import unittest 
tc = unittest.TestCase()

from aiorpc.parsers import JSONByteParser 
from aiorpc.exceptions import ParseError



def test_json_byte_parser():

    parser = JSONByteParser()
    b = b'{"a":1,"b":[1,2,3],"c":{"hi":"bye"}}'
    pyobj = {'a': 1, 'b': [1,2,3], 'c': {'hi': 'bye'}}

    test = parser.decode(b)
    tc.assertDictEqual(test, pyobj)

    test = parser.encode(pyobj)
    assert b == test 

    with pytest.raises(ParseError):
        parser.decode(b'{"hi"Lsdf;;,')
    
    class Dummy:
        pass

    with pytest.raises(ParseError):
        parser.encode(Dummy())