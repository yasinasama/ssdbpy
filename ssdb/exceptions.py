"Core exceptions raised by the ssdb client"
from ssdb._compat import unicode


class SSDBError(Exception):
    pass


# python 2.5 doesn't implement Exception.__unicode__. Add it here to all
# our exception types
if not hasattr(SSDBError, '__unicode__'):
    def __unicode__(self):
        if isinstance(self.args[0], unicode):
            return self.args[0]
        return unicode(self.args[0])
    SSDBError.__unicode__ = __unicode__


class ConnectionError(SSDBError):
    pass


class TimeoutError(SSDBError):
    pass


class ResponseError(SSDBError):
    pass


class ErrorError(ResponseError):
    pass

class FailError(ResponseError):
    pass

class ClientError(ResponseError):
    pass
