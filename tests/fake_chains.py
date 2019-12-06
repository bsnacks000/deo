from aiorpc import chain
from pprint import pprint 


@chain.chained('c')
def make_c_from_ab(a, b, some_val=0):
    return a + b + some_val 



@chain.chained('d')
def make_d_from_c(c, some_val=0):
    return  c + some_val 



@chain.chained('e')
def make_e(a,b,c,d):
    return  a + b + c + d



@chain.chained('bad')
def bad():
    raise Exception('boo')
