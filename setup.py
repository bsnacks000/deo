from setuptools import setup, find_packages

req = [
    'marshmallow>3.1',
    'orjson',
    'dask', 
    'distributed'
]

setup(
    name='aiorpc', 
    description='jsonrpc2 + asyncio',
    packages=find_packages(exclude=['tests']),
    install_requires=req, 
    entry_points={}
)