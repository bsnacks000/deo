## aiorpc
---------

A lightweight framework for building scalable JSON-RPC2 microservices with asyncio, dask and marshmallow over TCP. It combines a simple decorator interface for routing, flexible processing with ``dask.distributed`` and an optional method chain based API to help simplfy complex workflows. 

A basic example app can be found in ``examples.test_server``.

## Walkthrough


### Setup schemas 

Every route must be registered to an instance of a ``marshmallow.Schema`` subclass called ``JsonRpcSchema``. This schema class handles validation and (de)serialization for its method as well as takes care of the particulars of the RPC-protocol.   

The recommended procedure for most cases is to subclass ``JsonRpcSchema`` and override the ``params`` and ``result`` fields with ``NestedSchemas`` whose field names should map directly to arg or kwarg names in the method.  


### Register methods  


### Using the ``chained`` decorator 


### 
