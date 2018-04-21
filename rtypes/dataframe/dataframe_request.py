class DFRequest(object):
    @property
    def type_object(self):
        return self._tp_obj

    @type_object.setter
    def type_object(self, v):
        self._tp_obj = v

class PutDFRequest(DFRequest):
    pass

class GetDFRequest(DFRequest):
    @property
    def token(self):
        return self._token

    @token.setter
    def token(self, v):
        self._token = v

    @property
    def oid(self):
        try:
            return self._oid
        except AttributeError:
            self._oid = None
            return self._oid

    @oid.setter
    def oid(self, v):
        self._oid = v

    @property
    def param(self):
        try:
            return self._param
        except AttributeError:
            self._param = None
            return self._param
        
    @param.setter
    def param(self, v):
        self._param = v

class AppendDFRequest(PutDFRequest):
    @property
    def obj(self):
        try:
            return self._obj
        except AttributeError:
            self._obj = None
            return self._obj
        
    @obj.setter
    def obj(self, v):
        self._obj = v


class ExtendDFRequest(PutDFRequest):
    @property
    def objs(self):
        try:
            return self._objs
        except AttributeError:
            self._objs = list()
            return self._objs

    @objs.setter
    def objs(self, v):
        self._objs = v

class DeleteDFRequest(PutDFRequest):
    @property
    def obj(self):
        try:
            return self._obj
        except AttributeError:
            self._obj = None
            return self._obj

    @obj.setter
    def obj(self, v):
        self._obj = v

class DeleteAllDFRequest(PutDFRequest):
    pass

class ApplyChangesDFRequest(object):
    @property
    def df_changes(self):
        try:
            return self._df_changes
        except AttributeError:
            self._df_changes = None
            return self._df_changes

    @df_changes.setter
    def df_changes(self, v):
        self._df_changes = v

    @property
    def except_app(self):
        try:
            return self._except_app
        except AttributeError:
            self._except_app = None
            return self._except_app

    @except_app.setter
    def except_app(self, v):
        self._except_app = v

    @property
    def wait_for_server(self):
        try:
            return self._wait_for_server
        except AttributeError:
            self._wait_for_server = False
            return self._wait_for_server

    @wait_for_server.setter
    def wait_for_server(self, v):
        self._wait_for_server = v

    @property
    def token(self):
        return self._token

    @token.setter
    def token(self, v):
        self._token = v


class ShutdownDFRequest(PutDFRequest):
    pass

class GetRecordDFRequest(object):
    @property
    def changelist(self):
        try:
            return self._changelist
        except AttributeError:
            self._changelist = None
            return self._changelist

    @changelist.setter
    def changelist(self, v):
        self._changelist = v

    @property
    def token(self):
        return self._token

    @token.setter
    def token(self, v):
        self._token = v

    @property
    def app(self):
        return self._app

    @app.setter
    def app(self, v):
        self._app = v


class UpdateDFRequest(DFRequest):
    # Token
    @property
    def token(self):
        return self._token
    @token.setter
    def token(self, v):
        self._token = v

    # Object
    @property
    def obj(self):
        try:
            return self._obj
        except AttributeError:
            self._obj = None
            return self._obj
    @obj.setter
    def obj(self, v):
        self._obj = v

    # Value
    @property
    def value(self):
        try:
            return self._value
        except AttributeError:
            self._value = None
            return self._value

    @value.setter
    def value(self, v):
        self._value = v

    # Dimension
    @property
    def dimension(self):
        try:
            return self._dimension
        except AttributeError:
            self._dimension = None
            return self._dimension
    @dimension.setter
    def dimension(self, v): self._dimension = v
