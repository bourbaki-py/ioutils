#coding:utf-8
import pytest
from bourbaki.ioutils.flexiblepersist import FlexiblePersist, DefaultIORegistry, NoTextIOMethodsAvailable


@pytest.fixture
def data():
    return dict(foo=['a', 'b', ['c', 'd']], bar=dict(baz=[1, 2, [3, 4]]), baz=None)


@pytest.mark.parametrize('serialization', list(DefaultIORegistry.loaders.keys()))
@pytest.mark.parametrize('compression', [None, *list(DefaultIORegistry.compressors.keys())])
@pytest.mark.parametrize('text_encoding', [None, *list(DefaultIORegistry.text_encoders.keys())])
def test_persist(data, serialization, compression, text_encoding, tmpdir):
    io_ = FlexiblePersist(serialization, compression=compression, text_encoding=text_encoding)

    i, o = io_.loads, io_.dumps
    assert i(o(data)) == data

    i, o = io_.load, io_.dump
    path = str(tmpdir.join('data' + io_.extension))
    assert_io_equal(data, path, i, o)

    i, o = io_.load_stream_from_dir, io_.dump_stream_to_dir
    path = str(tmpdir.join('stream'))
    assert_io_equal(data, path, i, o, lambda d: list(d.values()), list)

    i, o = io_.load_keyed_stream_from_dir, io_.dump_keyed_stream_to_dir
    path = str(tmpdir.join('keyed_stream'))
    assert_io_equal(data, path, i, o, dict.items, dict, compare_original=True)

    if io_.mode == 'b' and text_encoding is None:
        with pytest.raises(NoTextIOMethodsAvailable):
            textio = io_.text
    else:
        textio = io_.text

        i, o = textio.load_stream_from_file, textio.dump_stream_to_file
        path = str(tmpdir.join('stream' + io_.text_extension))
        assert_io_equal(data, path, i, o, lambda d: list(d.values()), list)

        i, o = textio.load_keyed_stream_from_file, textio.dump_keyed_stream_to_file
        path = str(tmpdir.join('stream' + io_.text_extension))
        assert_io_equal(data, path, i, o, dict.items, dict, compare_original=True)


def assert_io_equal(data, path, i, o, owrapper=None, iwrapper=None, compare_original=False):
    if owrapper is None:
        odata = data
    else:
        odata = owrapper(data)

    o(odata, path)

    source = data if compare_original else odata

    if iwrapper is None:
        assert i(path) == source
    else:
        assert iwrapper(i(path)) == source
