from aiorpc import chain
from pprint import pprint 


@chain.chained('c')
def make_c_from_ab(a, b, some_val=0):
    return a + b + some_val 



@chain.chained('d')
def make_d_from_c(e, some_val=0):
    return e + some_val



@chain.chained('f')
def make_f(a,b,c,d):
    return a + b + c + d
