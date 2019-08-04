from . import common_type


class ExceptionWrapper:
    @staticmethod
    def exceptionToErrCode(e, code=None, errmsg=None):
        if code == None:
            code = type(e).__name__
        if errmsg == None:
            errmsg = str(e)

        return common_type.ErrCode(code=code, errmsg=errmsg)
