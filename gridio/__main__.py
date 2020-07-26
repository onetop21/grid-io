import sys
import os
import io
import tempfile
from pymongo import MongoClient
import gridfs
import boto3

class GridIOWrapper:
    def __init__(self, fs):
        self.__fs = fs
        self.__fp = None

    def __enter__(self):
        return self

    def __exit__(self, exc, val, tb):
        if self.__fp: self.close()

    def open(self, filepath):
        self.__filepath = filepath
        if not self.__fp:
            if self.__fs.exists(filename=filepath):
                self.__fp = self.__fs.new_file(filename=filepath)
            else:
                self.__fp = self.__fs.find_one(filename=filepath)
        else:
            raise IOError("Already opened.")
        
    def close(self):
        if self.__fp:
            self.__fp.close()
            self.__fp = None
        else:
            raise IOError("Not opened yet.")

    def read(self):
        if self.__fp:
            return self.__fp.read()
        else:
            raise IOError("Not opened yet.")

    def write(self, *args, **kwargs):
        if self.__fp:
            return self.__fp.write(*args, **kwargs)
        else:
            raise IOError("Not opened yet.")

    @property
    def property(self):
        return type('__property__', object, self.__property)

    @property.setter
    def property(self, **kwargs):
        self.__fp.

class GridIO:
    def __init__(self, name='__gridio__', host='localhost:27017'):
        self.__name = name
        self.__host = host
        self.__db_name = db_name
        
        self.__client = client = MongoClient(host=host)
        self.__db = db = client[db_name]
        self.__fs = gridfs.GridFS(db)
    
    def __del__(self):
        self.release()

    def release(self):
        self.__client.close()

    def open(self, filepath):
        wrapper = GridIOWrapper(self.__fs)
        wrapper.open(filepath)
        return wrapper

    def find(self, *args, **kwargs):
        return [ _.filename for _ in self.__fs.find(*args, **kwargs) ]
    
    def find_one(self, *args, **kwargs):
        fp = self.__fs.find_one(*args, **kwargs)
        if fp:
            return fp.filename
        else:
            return None