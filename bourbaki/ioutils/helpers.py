#coding:utf-8
import os
from .pickleutils import pickle_load, pickle_dump


def maybe_load(path, f, args=(), kwargs=None, loader=pickle_load, dumper=pickle_dump, recompute=False, dump=True):
    """Helpful e.g. in a Jupyter notebook setting """
    def _compute(f, args, kwargs, dump, dumper):
        if kwargs is None:
            obj = f(*args)
        else:
            obj = f(*args, **kwargs)

        if dump:
            try:
                print("attempting to save to {} using {}".format(path, dumper))
                dumper(obj, path)
            except Exception as e:
                print("!!! saving to {} failed with exception '{}'; recomputing".format(path, e))
                os.remove(path)
            else:
                print("--> saved to {} successfully".format(path))
        return obj

    if (not recompute) and os.path.exists(path):
        try:
            print("attempting to load from {} using {}".format(path, loader))
            obj = loader(path)
        except Exception as e:
            print("!!! loading from {} failed with exception '{}'; recomputing".format(path, e))
            obj = _compute(f, args, kwargs, dump, dumper)
        else:
            print("--> loaded from {} successfully".format(path))
    else:
        if not recompute:
            print("{} not found; computing".format(path))
        obj = _compute(f, args, kwargs, dump, dumper)

    return obj
