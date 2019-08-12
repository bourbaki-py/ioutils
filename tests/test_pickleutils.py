#!/usr/bin/env python
#coding:utf-8
import os
import platform
import pytest
from warnings import warn
import pickle
_pickle_dump, _pickle_load = pickle.dump, pickle.load
from bourbaki.ioutils import pickle

BIG_FILE_SIZE = 2**31 + 1

@pytest.fixture(scope="module")
def pickle_path():
    path = "/tmp/test_pickle.pkl"
    yield path
    os.remove(path)


@pytest.fixture(scope="function")
def pickle_save_file(pickle_path):
    # append mode prevents truncation in the case of write-related tests that are meant to throw and exception
    f = open(pickle_path, "ab")
    f.seek(0)
    yield f
    f.close()


@pytest.fixture(scope="function")
def pickle_load_file(pickle_path):
    f = open(pickle_path, "rb")
    yield f
    f.seek(0)
    f.close()


@pytest.fixture(scope="module")
def big_obj():
    return bytes(BIG_FILE_SIZE)


@pytest.mark.big_io
def test_save_big_pickle(pickle_save_file, big_obj):
    pickle.dump(big_obj, pickle_save_file)
    assert os.stat(pickle_save_file.name).st_size >= BIG_FILE_SIZE


@pytest.mark.big_io
def test_load_big_pickle(pickle_load_file, big_obj):
    big_obj_ = pickle.load(pickle_load_file)
    assert big_obj == big_obj_
    assert os.stat(pickle_load_file.name).st_size >= BIG_FILE_SIZE


@pytest.mark.big_io
def test_pickle_save_fails(pickle_save_file, big_obj):
    if platform.system() == "Darwin":
        with pytest.raises(OSError):
            _pickle_dump(big_obj, pickle_save_file)


# this actually doesn't throw an exception, but I'm leaving it here in case for some reason it ever might.
# def test_pickle_load_fails(pickle_load_file):
#     with pytest.raises(OSError):
#         big_obj_ = _pickle_load(pickle_load_file)
