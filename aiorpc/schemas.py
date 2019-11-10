import marshmallow as ma  



class StringOrInt(ma.fields.Field):
    """ Attempts to satisfy the JSON-RPC spec that states that an id can be either an Integer, String or None.
    """

    def _serialize(self, value, attr, obj, **kwargs):
        if value is None:
            return 
        
        elif isinstance(value, int):
            field_class = ma.fields.Integer
            return ma.fields.Integer._serialize(self, value, attr, obj, **kwargs)
        
        elif isinstance(value, str):
            return ma.fields.Str._serialize(self,value,attr, attr, obj, **kwargs)  

        else:
            raise ma.ValidationError('Must be a string or an int')
        

    def _deserialize(self, value, attr, data, **kwargs):
        if value is None:
            return 
        
        elif isinstance(value, int):
            field_class = ma.fields.Integer
            return ma.fields.Integer._deserialize(self, value, attr, obj, **kwargs)
        
        elif isinstance(value, str):
            return ma.fields.Str._deserialize(self,value,attr, attr, obj, **kwargs)  

        else:
            raise ma.ValidationError('Must be a string or an int')


class _JsonRPCDefaultDetailSchema(ma.Schema):
    """ This is a default detail schema that is used as a placeholder for error-detail objects 
    and result objects. It is expected that this detail will be overridden but is provided since it 
    is required in the spec
    """
    detail = ma.fields.Dict(required=False, default={ 'detail': 'ok' }) 


class _JsonRPCErrorSchema(ma.Schema):
    """ The JSON-RPC 2.0 error object as specified. The data field is an openn object that 
    can be populated an on a per app basis.
    """

    code = ma.fields.Integer(required=True)
    message = ma.fields.Str(required=True)
    data = ma.fields.Dict(required=False, default={}) 


class JsonRPCSchema(ma.Schema):
    """ The default schema for a JSON-RPC method. Note that params are optional. A method should 
    register a subclass of this class with an overridden result and params fields that will contain 
    nested implementations of the schemas. 
    """

    jsonrpc = ma.fields.Str(required=True, default="2.0")
    id = StringOrInt(required=False, allow_none=True)
    method = ma.fields.Str(required=True, load_only=True)
    error = ma.fields.Nested(_JsonRPCErrorSchema, required=False, dump_only=True)    
    result = ma.fields.Nested(_JsonRPCDefaultDetailSchema, required=True, dump_only=True)
    