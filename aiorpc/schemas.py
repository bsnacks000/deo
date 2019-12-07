import marshmallow as ma  
import orjson 



class ContextData(object):
    """ A Request Context container object.
    """

    def __init__(self, method='', jsonrpc="2.0", id=None, params=None):
        self.method = method 
        self.jsonrpc = jsonrpc 
        self.id = id 
        self.params = params


    def to_dict(self):
        return vars(self).copy()


    
class StringOrInt(ma.fields.Integer):
    """ Attempts to satisfy the JSON-RPC spec that states that an id can be either an Integer, String or None.
    """

    def _serialize(self, value, attr, obj, **kwargs):
        if value is None:
            return 
        
        elif isinstance(value, int):
            return ma.fields.Integer._serialize(self, value, attr, obj, **kwargs)
        
        elif isinstance(value, str):
            return ma.fields.Str._serialize(self, value, attr, obj, **kwargs)  

        else:
            raise ma.ValidationError('Must be a string or an int')
        

    def _deserialize(self, value, attr, data, **kwargs):
        if value is None:
            return 
        
        elif isinstance(value, int):
            return ma.fields.Integer._deserialize(self, value, attr, data, **kwargs)
        
        elif isinstance(value, str):
            return ma.fields.Str._deserialize(self, value, attr, data, **kwargs)  

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



class JsonRPCSchemaOpts(ma.SchemaOpts):

    def __init__(self, meta, **kwargs):
        super().__init__(meta, **kwargs)

        self.contextdata_class = getattr(meta, 'contextdata_class', ContextData)
        if self.contextdata_class is None:
            raise ValueError('`contextdata_class` must be set on a ContextDataSchema')
        if not isinstance(self.contextdata_class(), ContextData):
            raise ValueError('`contextdata_class` must be set to ContextData or a subclass')


class JsonRPCSchema(ma.Schema):
    """ The default schema for a JSON-RPC method. It contains read and write only fields containing metadata, error and result. 
    Note that params are optional. A method should register a subclass of this class with an overridden result and params fields that will contain 
    nested implementations of the schemas. 
    """
    jsonrpc = ma.fields.Str(required=False, default="2.0")
    id = StringOrInt(required=False, allow_none=True)
    method = ma.fields.Str(required=True, load_only=True)
    error = ma.fields.Nested(_JsonRPCErrorSchema, required=False, dump_only=True)    
    result = ma.fields.Nested(_JsonRPCDefaultDetailSchema, required=False, dump_only=True)
    params = None 
    
    OPTIONS_CLASS = JsonRPCSchemaOpts

    class Meta:
        contextdata_class = ContextData
        load_only = ('params',)
        dump_only = ('result', 'error',)


    @ma.validates_schema 
    def validate_jsonrpc(self, data, **kwargs):
        if data['jsonrpc'] != '2.0':
            raise ValidationError('jsonrpc must be set to 2.0')
    

    @ma.post_load
    def make_context(self, data, **kwargs):
        return self.opts.contextdata_class(**data)

    
    @ma.pre_dump 
    def dump_context(self, contextdata, **kwargs):
        if not isinstance(contextdata, self.opts.contextdata_class):
            raise ma.ValidationError('Object must be of type ContextData')
        return contextdata.to_dict()
