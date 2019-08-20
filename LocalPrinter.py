import inspect

def trace_fun(any):
    '''
    This is used as a decorator.
    '''
    def wrapper(*args, **kwargs):
        isFunc = inspect.isfunction(any)
        if isFunc: 
            trace_notification = 'Running {anyname}()...'.format(anyname = any.__name__)
            print(trace_notification)
            res = any(*args, **kwargs)
            trace = '{anyname}() complete, returned {res}.\n\n'.format(anyname = any.__name__, res = res) if res is not None else '{anyname}() complete.\n\n'.format(anyname = any.__name__) 
            print(trace)
            return res
        else: 
            print('(not a function, cannot do trace')
    return wrapper

def trace(name,val,filewise_traceid): 
    print('~'+str(filewise_traceid), name+":", str(val))
    filewise_traceid += 1
    return filewise_traceid

