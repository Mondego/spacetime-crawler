import cPickle
CACHE = dict()
def cache(func):
    def cached_func(*args, **kwargs):
        return CACHE.setdefault(
            func, dict()).setdefault(
                (args, tuple(kwargs.iteritems())), func(*args, **kwargs))
    return cached_func
