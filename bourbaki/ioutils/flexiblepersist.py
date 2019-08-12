#coding:utf-8
from typing import Union, Callable, Iterable, Iterator, Dict, Tuple, Optional as Opt
import io
import os
import re
import codecs
import ujson as json
import base64
import pickle
import gzip
import bz2
import lzma
import msgpack
import lz4.block
from pathlib import Path
from operator import itemgetter
from itertools import starmap
from functools import partial, lru_cache
from collections import ChainMap
from cytoolz import compose, flip
from logging import getLogger
try:
    import dill
except ImportError:
    dill = None

NoneType = type(None)
logger = getLogger(__name__)
DEFAULT_TEXT_ENCODING = "utf-8"
PATH_TYPES = (str, Path)
# this will default to True in the future, allowing strings to unpack to unicode directly by default
MSGPACK_LOAD_KW = dict(raw=False)


def _maybe_add_extension(path, extension):
    if isinstance(path, Path):
        path = str(path)
    _, ext = os.path.splitext(path)
    if not ext:
        path = path + extension
    return path


def ensure_dir(dir_):
    if os.path.exists(dir_):
        if not os.path.isdir(dir_):
            raise FileExistsError("dir_ must be a directory; {} exits but is a file".format(dir_))
    else:
        os.mkdir(dir_)


class NoTextIOMethodsAvailable(AttributeError, TypeError):
    pass


class StreamSerializer:
    def __init__(self, ser):
        """turn a Callable[[obj], AnyStr] into an effectful Callable[[obj, FileIO], NoneType]"""
        self.serializer = ser

    def __call__(self, obj, file):
        s = self.serializer(obj)
        file.write(s)


class StreamDeserializer:
    def __init__(self, deser):
        print
        """turn a Callable[[AnyStr], obj] into a Callable[[FileIO], obj]"""
        self.deserializer = deser

    def __call__(self, file):
        s = file.read()
        return self.deserializer(s)


class StrSerializer:
    def __init__(self, ser, binary=True):
        """turn an effectful Callable[[obj, FileIO], NoneType] into a Callable[[obj], AnyStr]"""
        self.serializer = ser
        self._io_cls = io.BytesIO if binary else io.StringIO

    def __call__(self, obj):
        with self._io_cls() as f:
            self.serializer(obj, f)
            f.seek(0)
            return f.read()


class StrDeserializer:
    def __init__(self, deser, binary=True):
        """turn a Callable[[FileIO], obj] into a Callable[[AnyStr], obj]"""
        self.deserializer = deser
        self._io_cls = io.BytesIO if binary else io.StringIO

    def __call__(self, s):
        with self._io_cls() as f:
            f.write(s)
            f.seek(0)
            return self.deserializer(f)


class IORegistry:
    _dumpers = dict(pickle=pickle.dumps, msgpack=msgpack.dumps)
    _loaders = dict(pickle=pickle.loads, msgpack=partial(msgpack.loads, **MSGPACK_LOAD_KW))
    _file_dumpers = dict(pickle=pickle.dump, msgpack=msgpack.dump)
    _file_loaders = dict(pickle=pickle.load, msgpack=partial(msgpack.load, **MSGPACK_LOAD_KW))

    _text_dumpers = dict(json=json.dumps)
    _text_loaders = dict(json=json.loads)
    _text_file_dumpers = dict(json=json.dump)
    _text_file_loaders = dict(json=json.load)

    _compressors = dict(lzma=lzma.compress, lz4=lz4.block.compress, bz2=bz2.compress, gzip=gzip.compress)
    _decompressors = dict(lzma=lzma.decompress, lz4=lz4.block.decompress, bz2=bz2.decompress, gzip=gzip.decompress)

    _text_decoders = dict(
        base16=base64.b16decode,
        base32=base64.b32decode,
        base64=base64.b64decode,
        base85=base64.b85decode,
    )
    _text_encoders = dict(
        base16=base64.b16encode,
        base32=base64.b32encode,
        base64=base64.b64encode,
        base85=base64.b85encode,
    )

    _compression_extensions = dict(lzma='.lzma', lz4='.lz4', bz2='.bz2', gzip='.gzip')
    _serialization_extensions = dict(json='.json', pickle='.pkl', msgpack='.msgpack')
    _text_extensions = dict(base16='.b16', base32='.b32', base64='.b64', base85='.b85')
    _text_extension = '.txt'
    _char_encoding = DEFAULT_TEXT_ENCODING

    dumpers = loaders = file_dumpers = file_loaders = None
    text_dumpers = text_loaders = text_file_dumpers = text_file_loaders = None
    compressors = decompressors = text_encoders = text_decoders = None
    compression_extensions = serialization_extensions = text_extensions = None
    text_encoders_return_str = None

    def __init__(self, char_encoding: str=DEFAULT_TEXT_ENCODING):
        for attr in ('dumpers', 'loaders', 'file_dumpers', 'file_loaders',
                     'text_dumpers', 'text_loaders', 'text_file_dumpers', 'text_file_loaders',
                     'text_encoders', 'text_decoders', 'compressors', 'decompressors',
                     'compression_extensions', 'serialization_extensions', 'text_extensions',
                     ):
            # so subclasses can share without overwriting upon registry
            setattr(self, attr, ChainMap({}, getattr(self, '_' + attr)))

            self.text_encoders_return_str = set()

            self.char_encoding = char_encoding

    @property
    def char_encoding(self):
        return self._char_encoding

    @char_encoding.setter
    def char_encoding(self, char_encoding):
        try:
            _ = codecs.getencoder(char_encoding)
        except LookupError as e:
            raise e
        self._char_encoding = char_encoding

    @property
    def bytes_to_str_default(self):
        return partial(flip(bytes.decode), self.char_encoding)

    @property
    def str_to_bytes_default(self):
        return partial(flip(str.encode), self.char_encoding)

    def register_compressor(self, name, *,
                            compressor: Callable[[bytes], bytes],
                            decompressor: Callable[[bytes], bytes],
                            extension: str):
        if name in self.compressors or name in self.decompressors:
            raise KeyError("{} is already registered; choose a different name".format(name))
        for alias, f in [("compressor", compressor), ("decompressor", decompressor)]:
            if not callable(f):
                raise TypeError("{} must be callable; got {}".format(alias, type(f)))

        self.compressors[name] = compressor
        self.decompressors[name] = decompressor
        self.compression_extensions[name] = '.' + extension.lstrip('.')

    def register_text_encoder(self, name, *,
                              extension: str,
                              encoder: Union[Callable[[bytes], bytes], Callable[[bytes], str]],
                              decoder: Union[Callable[[bytes], bytes], Callable[[str], bytes]],
                              binary: bool=True):
        if name in self.text_encoders or name in self.text_decoders:
            raise KeyError("{} is already registered; choose a different name".format(name))
        for alias, f in [("compressor", encoder), ("decompressor", decoder)]:
            if not callable(f):
                raise TypeError("{} must be callable; got {}".format(alias, type(f)))

        if not binary:
            self.text_encoders_return_str.add(name)

        self.text_encoders[name] = encoder
        self.text_decoders[name] = decoder
        self.text_extensions[name] = '.' + extension.lstrip('.')

    def register_serializer(self, name: str, *,
                            extension: str,
                            serializer: Opt[Union[Callable[[object], bytes], Callable[[object], str]]]=None,
                            deserializer: Opt[Union[Callable[[bytes], object], Callable[[str], object]]]=None,
                            stream_serializer: Opt[Callable[[object, io.IOBase], NoneType]]=None,
                            stream_deserializer: Opt[Callable[[io.IOBase], object]]=None,
                            binary: Opt[bool]=True):
        loaders, dumpers, file_loaders, file_dumpers = (
            (self.loaders, self.dumpers, self.file_loaders, self.file_dumpers) if binary else
            (self.text_loaders, self.text_dumpers, self.text_file_loaders, self.text_file_dumpers)
        )

        if name in loaders or name in dumpers:
            raise KeyError("{} is already registered; choose a different name".format(name))

        for alias, f in [("serializer", serializer),
                         ("stream_serializer", stream_serializer),
                         ("deserializer", deserializer),
                         ("stream_deserializer", stream_serializer)]:
            if not callable(f):
                raise TypeError("{} must be callable; got {}".format(alias, type(f)))

        ser = serializer or StrSerializer(serializer, binary)
        stream_ser = stream_serializer or StreamSerializer(serializer)
        deser = deserializer or StrDeserializer(deserializer, binary)
        stream_deser = stream_deserializer or StreamDeserializer(deserializer)

        dumpers[name] = ser
        loaders[name] = deser
        file_dumpers[name] = stream_ser
        file_loaders[name] = stream_deser
        self.serialization_extensions[name] = '.' + extension.lstrip('.')

    def is_binary_serialization(self, serialization):
        if serialization in self.loaders:
            return True
        elif serialization in self.text_loaders:
            return False
        else:
            raise KeyError("Unknown serialization protocol: {}".format(serialization))

    def is_binary_text_encoding(self, encoding):
        if encoding in self.text_encoders_return_str:
            return False
        elif encoding in self.text_encoders:
            return True
        else:
            raise KeyError("Unknown text encoding protocol: {}".format(encoding))

    def get_serializer(self, serialization: str, string=False):
        registry = self._dumpers if string else self._file_dumpers
        return self._get_lambda(serialization, registry, "serialization")

    def get_deserializer(self, serialization: str, string=False):
        registry = self._dumpers if string else self._file_dumpers
        return self._get_lambda(serialization, registry, "serialization")

    def serialization_from_extension(self, ext: str):
        return self._name_from_ext(ext, self.serialization_extensions, "serialization extension")

    def compression_from_extension(self, ext: str):
        return self._name_from_ext(ext, self.compression_extensions, "compression extension")

    def text_encoding_from_extension(self, ext: str):
        return self._name_from_ext(ext, self.text_extensions, "text extension")

    def _name_from_ext(self, ext, dict_, kwarg):
        rev = dict(map(reversed, dict_.items()))
        ext = '.' + ext.lstrip('.')
        return self._get_lambda(ext, rev, kwarg)

    def pipeline_from_extension(self, ext: str):
        parts = ext.lstrip('.').split('.')
        comp_or_text_ext = None
        comp_ext = None
        text_ext = None
        if len(parts) == 1:
            # only serialization
            ser_ext = parts[0]
        elif len(parts) == 2:
            # ser and comp or ser and text
            ser_ext = parts[0]
            comp_or_text_ext = parts[1]
        elif len(parts) == 3:
            ser_ext, comp_ext, text_ext = parts
        else:
            raise ValueError("Too many parts in extension {}; cannot determine serialization protocol"
                             .format(repr(ext)))

        serialization = self.serialization_from_extension(ser_ext)
        compression = None
        text_encoding = None

        if comp_ext is not None or text_ext is not None:
            compression = self.compression_from_extension(comp_ext)
            text_encoding = self.text_encoding_from_extension(text_ext)
        elif comp_or_text_ext is not None:
            try:
                compression = self.compression_from_extension(comp_or_text_ext)
            except ValueError:
                text_encoding = self.text_encoding_from_extension(comp_or_text_ext)

        return serialization, compression, text_encoding

    def get_serializer_pairs(self, serialization: str):
        return (self._get_lambda(serialization, self.dumpers, "serialization"),
                self._get_lambda(serialization, self.loaders, "serialization"),
                self._get_lambda(serialization, self.file_dumpers, "serialization"),
                self._get_lambda(serialization, self.file_loaders, "serialization"))

    def get_text_serializer_pairs(self, serialization: str):
        return (self._get_lambda(serialization, self.text_dumpers, "serialization"),
                self._get_lambda(serialization, self.text_loaders, "serialization"),
                self._get_lambda(serialization, self.text_file_dumpers, "serialization"),
                self._get_lambda(serialization, self.text_file_loaders, "serialization"))

    def get_compressor_pair(self, compression: str):
        return (self._get_lambda(compression, self.compressors, "compression"),
                self._get_lambda(compression, self.decompressors, "compression"))

    def get_text_encoder_pair(self, conversion: str):
        encoder, decoder = (self._get_lambda(conversion, self.text_encoders, "conversion"),
                            self._get_lambda(conversion, self.text_decoders, "conversion"))
        if conversion not in self.text_encoders_return_str:
            encoder = compose(self.bytes_to_str_default, encoder)
            decoder = compose(decoder, self.str_to_bytes_default)

        return encoder, decoder

    @staticmethod
    def _get_lambda(key, dict_, kwarg, allow_missing=False):
        lam = dict_.get(key)
        if lam is None and not allow_missing:
            raise ValueError("{} must be one of {}; got {}".format(kwarg, tuple(dict_), key))
        return lam

    @lru_cache(None)
    def compile_pipeline(self, serialization: str, *,
                         compression: Opt[str]=None,
                         text_encoding: Opt[str]=None,
                         dump_kw: Opt[Dict[str, object]]=None,
                         load_kw: Opt[Dict[str, object]]=None,
                         compress_kw: Opt[Dict[str, object]]=None,
                         decompress_kw: Opt[Dict[str, object]]=None):
        to_str_fs, from_str_fs = [], []
        to_file_fs, from_file_fs = [], []

        # obj -> Union[bytes, str]
        if self.is_binary_serialization(serialization):
            getter = self.get_serializer_pairs
            binary = True
            file_mode = 'b'
        else:
            getter = self.get_text_serializer_pairs
            binary = False
            file_mode = ''

        to_str, from_str, to_file, from_file = getter(serialization)
        if dump_kw:
            to_str, to_file = partial(to_str, **dump_kw), partial(to_file, **dump_kw)
        if load_kw:
            from_str, from_file = partial(from_str, **load_kw), partial(from_file, **load_kw)

        to_str_fs.append(to_str)
        from_str_fs.append(from_str)
        to_file_fs.append(to_file)
        from_file_fs.append(from_file)

        # bytes -> bytes
        if compression is not None:
            # can't write direct to file now
            to_file_fs, from_file_fs = None, None
            file_mode = 'b'

            compress, decompress = self.get_compressor_pair(compression)

            if compress_kw:
                compress = partial(compress, **compress_kw)

            if decompress_kw:
                decompress = partial(decompress, **decompress_kw)

            if not binary:
                to_str_fs.append(self.str_to_bytes_default)
                from_str_fs.append(self.bytes_to_str_default)

            to_str_fs.append(compress)
            from_str_fs.append(decompress)

        # bytes -> str
        if text_encoding is not None:
            # can't write direct to file now
            to_file_fs, from_file_fs = None, None
            file_mode = ''

            if not binary and compression is None:
                to_str_fs.append(self.str_to_bytes_default)
                from_str_fs.append(self.bytes_to_str_default)
            encode, decode = self.get_text_encoder_pair(text_encoding)

            to_str_fs.append(encode)
            from_str_fs.append(decode)

        # reverse the composition
        to_str_fs = reversed(to_str_fs)
        to_str = compose(*to_str_fs)
        from_str = compose(*from_str_fs)

        if to_file_fs:
            to_file_fs = reversed(to_file_fs)
            to_file = compose(*to_file_fs)
            from_file = compose(*from_file_fs)
        else:
            to_file = StreamSerializer(to_str)
            from_file = StreamDeserializer(from_str)

        return to_str, from_str, to_file, from_file, file_mode


if dill is not None:
    IORegistry._serialization_extensions["dill"] = ".dill"
    IORegistry._loaders["dill"] = dill.loads
    IORegistry._dumpers["dill"] = dill.dumps
    IORegistry._file_loaders["dill"] = dill.load
    IORegistry._file_dumpers["dill"] = dill.dump
else:
    del dill

DefaultIORegistry = IORegistry()


class FlexiblePersist:
    def __init__(self, serialization: str, *,
                 compression: Opt[str]=None,
                 text_encoding: Opt[str]= None,
                 dump_kw: Opt[Dict[str, object]]=None,
                 load_kw: Opt[Dict[str, object]]=None,
                 compress_kw: Opt[Dict[str, object]]=None,
                 decompress_kw: Opt[Dict[str, object]]=None,
                 always_use_text_encoding: bool=False,
                 _io_registry=DefaultIORegistry):
        if always_use_text_encoding:
            if text_encoding is None:
                raise ValueError("Must pass text_encoding if always_use_text_encoding is True")
            to_str, from_str, to_file, from_file, mode = _io_registry.compile_pipeline(serialization=serialization,
                                                                                       compression=compression,
                                                                                       text_encoding=text_encoding,
                                                                                       load_kw=load_kw,
                                                                                       dump_kw=dump_kw,
                                                                                       compress_kw=compress_kw,
                                                                                       decompress_kw=decompress_kw)
            to_line, from_line = to_str, from_str
        else:
            to_str, from_str, to_file, from_file, mode = _io_registry.compile_pipeline(serialization=serialization,
                                                                                       compression=compression,
                                                                                       load_kw=load_kw,
                                                                                       dump_kw=dump_kw,
                                                                                       compress_kw=compress_kw,
                                                                                       decompress_kw=decompress_kw)
            if text_encoding is None:
                if mode != '':
                    to_line, from_line = None, None
                else:
                    to_line, from_line = to_str, from_str
            else:
                to_line, from_line, _, _, _ = _io_registry.compile_pipeline(serialization=serialization,
                                                                            compression=compression,
                                                                            text_encoding=text_encoding,
                                                                            load_kw=load_kw,
                                                                            dump_kw=dump_kw,
                                                                            compress_kw=compress_kw,
                                                                            decompress_kw=decompress_kw)

        self._io_registry = _io_registry
        self._serialization = serialization
        self._compression = compression
        self._text_encoding = text_encoding
        self._mode = mode
        self._dump_kw = dump_kw
        self._load_kw = load_kw
        self._compress_kw = compress_kw
        self._decompress_kw = decompress_kw
        self._always_use_text_encoding = always_use_text_encoding
        self._dump = to_file
        self._load = from_file
        self.dumps = to_str
        self.loads = from_str
        self.to_line = to_line
        self.from_line = from_line

        if to_line is not None or from_line is not None:
            self._text_io = _TextIOMethods(self)

    _text_io = None

    @classmethod
    def from_extension(cls, ext: str, dump_kw: Opt[Dict[str, object]]=None,
                 load_kw: Opt[Dict[str, object]]=None,
                 compress_kw: Opt[Dict[str, object]]=None,
                 decompress_kw: Opt[Dict[str, object]]=None, _io_registry=DefaultIORegistry):
        serialization, compression, text_encoding = _io_registry.pipeline_from_extension(ext)

        return cls(serialization=serialization, compression=compression, text_encoding=text_encoding,
                   always_use_text_encoding=text_encoding is not None,
                   dump_kw=dump_kw, load_kw=load_kw,
                   compress_kw=compress_kw, decompress_kw=decompress_kw,
                   _io_registry=_io_registry)

    def __str__(self):
        attrs = ((k, getattr(self, k, None))
                 for k in ['_compression', '_text_encoding', '_dump_kw', '_load_kw',
                           '_compress_kw', '_decompress_kw', '_always_use_text_encoding'
                           ]
                 )
        kw = [(k[1:], repr(v)) for k, v in attrs if v is not None]
        return "{}({}{})".format(type(self).__name__, repr(self.serialization),
                                 ', ' + ', '.join(starmap('{}={}'.format, kw)) if kw else '')

    __repr__ = __str__

    @property
    def text(self):
        if self._text_io is None:
            raise NoTextIOMethodsAvailable("{} instance does not support writing to and from text files, as its "
                                           "default mode is binary and no text_encoding arg was supplied.".format(self))
        return self._text_io

    @property
    def serialization(self):
        return self._serialization

    @property
    def compression(self):
        return self._compression

    @property
    def text_encoding(self):
        return self._text_encoding

    @property
    def extension(self):
        return self._get_extension()

    @property
    def text_extension(self):
        return self._get_extension(text=True)

    @property
    def mode(self):
        return self._mode

    def _get_extension(self, text=False):
        ext = self._io_registry.serialization_extensions[self.serialization]
        if self.compression is not None:
            ext = ext + self._io_registry.compression_extensions[self.compression]
        if text or self._always_use_text_encoding:
            ext = ext + self._io_registry.text_extensions[self.text_encoding]
        return ext

    def dump(self, obj: object, file: Union[str, io.IOBase], *, auto_extension=True):
        if isinstance(file, PATH_TYPES):
            if auto_extension:
                file = _maybe_add_extension(file, self.extension)
            with open(file, 'w' + self._mode) as f:
                self._dump(obj, f)
        else:
            self._dump(obj, file)

    def load(self, file: Union[str, io.IOBase], auto_extension=True) -> object:
        if isinstance(file, PATH_TYPES):
            if auto_extension:
                file = _maybe_add_extension(file, self.extension)
            with open(file, 'r' + self._mode) as f:
                return self._load(f)
        else:
            return self._load(file)

    def dumps(self, obj: object) -> Union[str, bytes]:
        # set in __init__
        raise NotImplementedError()

    def loads(self, s: Union[str, bytes]) -> object:
        # set in __init__
        raise NotImplementedError()

    def _dump_stream_to_dir(self, items: Iterable[object], dir_: str, prefix: str=""):
        ensure_dir(dir_)
        ext = self.extension
        prefix = os.path.join(dir_, prefix)
        template = prefix + '{}' + ext
        for k, v in items:
            with open(template.format(k), 'w' + self._mode) as outfile:
                self._dump(v, outfile)

    def dump_stream_to_dir(self, iterable: Iterable[object], dir_: str, prefix: str="", ndigits: int=6):
        items = ((str(i).rjust(ndigits, '0'), obj) for i, obj in enumerate(iterable))
        self._dump_stream_to_dir(items, dir_, prefix)

    def dump_keyed_stream_to_dir(self, iterable: Iterable[Tuple[str, object]], dir_: str, prefix: str=""):
        self._dump_stream_to_dir(iterable, dir_, prefix)

    def _load_stream_from_dir(self, dir_: str,
                              prefix: str,
                              pattern: str,
                              key_type: Opt[type]=None,
                              yield_keys: bool=False,
                              ext: Opt[str]=None,
                              sort: bool=False) -> Union[Iterator[object], Iterator[Tuple[object, object]]]:
        if ext is None:
            ext = self.extension
        else:
            ext = '.' + ext.lstrip('.')

        load = partial(self.load, auto_extension=False)
        pat = re.compile(re.escape(prefix) + r"(?P<k>{})".format(pattern) + re.escape(ext))
        ms = filter(None, map(pat.fullmatch, os.listdir(dir_)))
        fs = ((m.group('k'), os.path.join(dir_, m.group())) for m in ms)

        if key_type is not None:
            fs = ((key_type(k), v) for k, v in fs)

        if sort:
            fs = sorted(fs, key=itemgetter(0))

        if yield_keys:
            return ((k, load(f)) for k, f in fs)
        else:
            return (load(f) for k, f in fs)

    def load_stream_from_dir(self, dir_: str, prefix: str="", ext: Opt[str]=None) -> Iterator[object]:
        return self._load_stream_from_dir(dir_, prefix, pattern=r'[0-9]+',
                                          key_type=int, sort=True, yield_keys=False, ext=ext)

    def load_keyed_stream_from_dir(self, dir_: str, prefix: str = "", ext: Opt[str]=None,
                                   key_type: Opt[type]=None) -> Iterator[object]:
        return self._load_stream_from_dir(dir_, prefix, pattern=r'.*',
                                          key_type=key_type, sort=False, yield_keys=True, ext=ext)


class _TextIOMethods:
    def __init__(self, flexible_persist: FlexiblePersist):
        self._io = flexible_persist

    def dump_stream_to_file(self, iterable: Iterable[object], file: Union[str, io.IOBase], auto_extension=True):
        encode = self._io.to_line
        lines = map(encode, iterable)
        close = False

        if isinstance(file, PATH_TYPES):
            if auto_extension:
                file = _maybe_add_extension(file, self._io.text_extension)
            file = open(file, "w")
            close = True

        for line in lines:
            file.write(line)
            file.write('\n')

        if close:
            file.close()

    def load_stream_from_file(self, file: Union[str, io.IOBase], auto_extension=True) -> Iterator[object]:
        decode = self._io.from_line
        close = False

        if isinstance(file, PATH_TYPES):
            if auto_extension:
                file = _maybe_add_extension(file, self._io.text_extension)
            file = open(file, "r")
            close = True

        for line in file:
            yield decode(line.rstrip('\n'))

        if close:
            file.close()

    def dump_keyed_stream_to_file(self, iterable: Iterable[Tuple[str, object]], file: Union[str, io.IOBase],
                                  encode_keys=False, sep='\t', auto_extension=True):
        encode = self._io.to_line
        template = '{}' + sep + '{}\n'

        if encode_keys:
            lines = (template.format(encode(k), encode(v)) for k, v in iterable)
        else:
            lines = (template.format(k, encode(v)) for k, v in iterable)
        close = False

        if isinstance(file, PATH_TYPES):
            if auto_extension:
                file = _maybe_add_extension(file, self._io.text_extension)
            file = open(file, "w")
            close = True

        for line in lines:
            file.write(line)

        if close:
            file.close()

    def load_keyed_stream_from_file(self, file: Union[str, io.IOBase],
                                    decode_keys=False, sep='\t', auto_extension=True) -> Iterator[object]:
        decode = self._io.from_line
        close = False

        if isinstance(file, PATH_TYPES):
            if auto_extension:
                file = _maybe_add_extension(file, self._io.text_extension)
            file = open(file, "r")
            close = True

        def read_items(f, sep_):
            for line in f:
                line = line.rstrip('\n')
                ix = line.index(sep_)
                yield line[:ix], line[ix+1:]

        if decode_keys:
            for k, v in read_items(file, sep):
                yield decode(k), decode(v)
        else:
            for k, v in read_items(file, sep):
                yield k, decode(v)

        if close:
            file.close()
