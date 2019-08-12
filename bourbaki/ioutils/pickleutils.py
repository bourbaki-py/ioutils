# coding:utf-8
from typing import Union
import io
import pickle
import platform
from pathlib import Path
from multipledispatch import Dispatcher
from logging import getLogger
from bourbaki.introspection.typechecking import type_checker

logger = getLogger(__name__)

SYSTEM_ALIAS = platform.system()
OS_IS_DARWIN = SYSTEM_ALIAS == 'Darwin'
DEFAULT_PROTOCOL = pickle.HIGHEST_PROTOCOL

_pickle_load = pickle.load
_pickle_dump = pickle.dump


class MacOSFile(io.IOBase):
    def __init__(self, f):
        super().__init__()
        self.f = f

    def __getattr__(self, item):
        return getattr(self.f, item)

    def read(self, n):
        if n >= (1 << 31):
            buffer = bytearray(n)
            idx = 0
            while idx < n:
                batch_size = min(n - idx, 1 << 31 - 1)
                buffer[idx:idx + batch_size] = self.f.read(batch_size)
                idx += batch_size
            return buffer
        return self.f.read(n)

    def write(self, buffer):
        n = len(buffer)
        idx = 0
        while idx < n:
            batch_size = min(n - idx, 1 << 31 - 1)
            self.f.write(buffer[idx:idx + batch_size])
            idx += batch_size


pickle_dump = Dispatcher("pickle_dump")


@pickle_dump.register(object, str)
def pickle_dump_str(obj, file, protocol=DEFAULT_PROTOCOL):
    with os_safe_file(open(file, 'wb')) as f:
        pickle_dump(obj, f, protocol=protocol)


@pickle_dump.register(object, Path)
def pickle_dump_path(obj, file: Path, protocol=DEFAULT_PROTOCOL):
    pickle_dump(obj, str(file), protocol=protocol)


@pickle_dump.register(object, (io.FileIO, io.IOBase))
def pickle_dump_file(obj, file: Union[io.FileIO, io.IOBase], protocol=DEFAULT_PROTOCOL):
    _pickle_dump(obj, os_safe_file(file), protocol=protocol)


pickle_load = Dispatcher("pickle_load")


@pickle_load.register(str)
def pickle_load_str(path: str):
    with open(path, 'rb') as f:
        obj = pickle_load(f)
    return obj


@pickle_load.register(Path)
def pickle_load_path(path: Path):
    return pickle_load(str(path))


@pickle_load.register((io.FileIO, io.IOBase))
def pickle_load_file(file: Union[io.FileIO, io.IOBase]):
    return _pickle_load(os_safe_file(file))


def os_safe_file(file: Union[io.IOBase, io.FileIO]):
    if OS_IS_DARWIN and not isinstance(file, MacOSFile):
        file = MacOSFile(file)
    return file


pickle.dump = pickle_dump
pickle.load = pickle_load


class Picklable:
    @classmethod
    def from_pickle(cls, path_or_file):
        """Helper to load an instance from a file handle or path"""

        obj = pickle_load(path_or_file)
        if not isinstance(obj, cls):
            try:
                raise TypeError("unpickled object {} is not an instance of {}".format(obj, cls))
            except TypeError as e:
                logger.exception("")
                raise e

        return obj

    def to_pickle(self, path_or_file):
        """Helper to dump an instance to a file handle or path"""
        pickle_dump(self, path_or_file)


class PartiallyPicklable(Picklable):
    """_unpicklable_attrs stores a tuple of either attribute names or tuples of (name, type) to specify which
    attributes should not be pickled. Usually this situation arises from the presence of attributes that may be large
    and/or stored elsewhere. The attributes can be set then by using the .from_pickles(*paths_or_files) method.
    In case any attributes are actually properties which reference an underlying cache attribute, these can be
    specified by setting the _property_caches attribute as a tuple of (property_name, attribute_name) tuples.
    At pickling time, attribute_name will be removed from the state dict, and at unpickling time, property_name
    will be set with setattr, ensuring that property setter logic is respected. The order of _unpicklable_attrs
    also specifies the order in which attributes are set at unpickling time, in case there are dependencies between
    any properties and attributes."""
    # strings identifying unpicklable attributes or tuples of (unpicklable_attribute_name, type)
    _unpicklable_attrs = ()
    _minimal_state_attrs = None

    @classmethod
    def unpicklable_types(cls):
        return list(n if isinstance(n, tuple) else (n, None) for n in cls._unpicklable_attrs)

    @classmethod
    def unpicklable_attrs(cls):
        return list(n if not isinstance(n, tuple) else n[0] for n in cls._unpicklable_attrs)

    @classmethod
    def minimial_state_attrs(cls):
        return cls.unpicklable_attrs() if cls._minimal_state_attrs is None else list(cls._minimal_state_attrs)

    @classmethod
    def minimal_state_types(cls):
        if cls._minimal_state_attrs is None:
            return cls.unpicklable_types()
        attrs = set(cls._minimal_state_attrs)
        return [tup for tup in cls.unpicklable_types() if tup[0] in attrs]

    def __getstate__(self):
        state = self.__dict__.copy()

        attrs = self.unpicklable_attrs()
        dont_pickle = tuple(a for a in attrs if a in state)
        logger.debug("not pickling attributes {}; these may be large or unpicklable and should be "
                     "persisted separately, e.g. with the .to_pickles() method on this instance".format(dont_pickle))
        for attr in dont_pickle:
            state.pop(attr)

        return state

    @classmethod
    def from_pickles(cls, path_file_or_instance: Union[str, io.IOBase, object],
                     **attr_files: Union[str, io.IOBase, object]):
        """Each argument may be a path name, file handle, or object of a specified type.
        If not a path or file handle, the first arg must be an instance of the class on which the method is being
        called. Allowable keyword args are specified in the class' _unpicklable_attrs tuple. Any entry there of
        may be a name string or (name, type) tuple. In the latter case, if the corresponding keyword argument passed to
        from_pickles() is not a file handle or str, it must be an instance of the specified type."""

        all_attrs = cls.minimial_state_attrs()
        extras = set(attr_files).difference(all_attrs)
        missing = set(all_attrs).difference(attr_files)

        if extras:
            logger.warning("{} has unpicklable attributes {} but keyword args {} were passed"
                           .format(cls, all_attrs, extras))
        if missing:
            logger.warning("{} has unpicklable_attrs {} but keyword args {} were not passed"
                           .format(cls, all_attrs, missing))

        self = cls.from_pickle(path_file_or_instance) \
            if not isinstance(path_file_or_instance, cls) \
            else path_file_or_instance

        for attr, type_ in cls.minimal_state_types():
            if attr in attr_files:
                path = attr_files[attr]
                obj = _maybe_from_pickle(path, type_, attr)
                setattr(self, attr, obj)

        return self

    def to_pickles(self, path_or_file: Union[str, io.IOBase], **attr_files):
        attrs_to_pickle = set(attr_files)
        all_attrs = set(self.minimial_state_attrs())

        extras = attrs_to_pickle.difference(all_attrs)
        missing = all_attrs.difference(attrs_to_pickle)

        if extras:
            no_attrs = [attr for attr in extras if not hasattr(self, attr)]
            if no_attrs:
                raise KeyError("{} does not have attributes {}, so they cannot be pickled"
                               .format(self, tuple(no_attrs)))
            else:
                logger.warning("{} has unpicklable attributes {} but keyword args {} were passed"
                               .format(type(self), all_attrs, extras))
        if missing:
            logger.warning("{} has unpicklable_attrs {} but keyword args {} were not passed"
                           .format(type(self), all_attrs, missing))

        badfiles = list(zip(*((k, v) for k, v in attr_files.items() if not isinstance(v, (str, io.IOBase)))))
        if badfiles:
            badattrs, badfiles = zip(*badfiles)
            raise TypeError("output locations {} for attributes {} appear not to be files or path names"
                            .format(tuple(badfiles), tuple(badattrs)))

        self.to_pickle(path_or_file)
        for attr, file in attr_files.items():
            pickle_dump(getattr(self, attr), file)


def _maybe_from_pickle(path_or_file, cls, attr=None):
    if isinstance(path_or_file, (str, io.IOBase)):
        obj = pickle_load(path_or_file)

        if cls is not None and not type_checker(cls)(obj):
            raise TypeError("Expected unpickled object{} to be an instance of {}; got {}"
                            .format("for attribute {}".format(attr) if attr is not None else "",
                                    cls, type(obj))
                            )
    else:
        if cls is not None and not type_checker(cls)(path_or_file):
            raise TypeError("Expected str, io.IOBase, or {} for path_or_file; "
                            "got {}".format(cls, type(path_or_file)))
        obj = path_or_file

    return obj
