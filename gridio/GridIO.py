import sys
import os
import io
from enum import IntEnum
import tempfile
from pymongo import MongoClient
import gridfs
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import logging

loglevel = os.environ.get('loglevel', logging.ERROR)
logging.basicConfig(level=int(loglevel), datefmt='%Y-%M-%d %H:%M:%S', format='[%(levelname)s] %(asctime)s.%(msecs)03d (%(name)s) - %(message)s')
logger = logging.getLogger(__name__)

class GridIOWrapper:
    def __init__(self, fs, bucket, filename, mode='r'):
        self.__filename = filename
        self.__fs = fs
        self.__bucket = bucket
        self.__mode = mode

        self.__need_preload = True in [_ in mode for _ in ['r', 'a']]
        self.__need_update = True in [_ in mode for _ in ['w', 'a']]
        self.__need_append = 'a' in mode
        self.__temp = tempfile.TemporaryFile()

        if self.__need_update:
            self.__dict__['read'] = None
        else:
            self.__dict__['write'] = None
            
    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc, val, tb):
        self.close()

    def open(self):
        if self.__need_preload and self.__fs.exists(filename=self.__filename):
            self.__bucket.download_to_stream_by_name(self.__filename, self.__temp)
        if not self.__need_append: self.__temp.seek(0)

    def close(self):
        if self.__need_update:
            self.__temp.seek(0)
            self.__bucket.upload_from_stream(self.__filename, self.__temp.read())
        
    def read(self, size=-1):
        buf = self.__temp.read(size)
        return buf if 'b' in self.__mode else buf.decode()

    def write(self, data):
        return self.__temp.write(data if 'b' in self.__mode else data.encode())

    def open(self, filepath, mode='r'):
        wrapper = GridIOWrapper(self.__fs, self.__bucket, filepath, mode) 
        return wrapper


    def find(self, *args, **kwargs):
        return [ _.filename for _ in self.__fs.find(*args, **kwargs) ]
    
    def find_one(self, *args, **kwargs):
        fp = self.__fs.find_one(*args, **kwargs)
        if fp:
            return fp.filename
        else:
            return None

class GridIOBucket:
    def __init__(self, name, fs):
        self.__name = name
        self.__fs = fs

    def __getitem__(self, filename):
        return self.file(filename)

    def __setitem__(self, filename, values):
        if isinstance(values, tuple):
            self.commit(filename, *values)
        else:
            raise ValueError(f'Support assign (bytes, dict) tuple object only.')

    @property
    def name(self):
        return self.__name

    def files(self):
        return self.__fs.list()

    def file(self, filename, version=-1):
        return self.__fs.get_version(filename=filename, version=version)

    def isExist(self, filename):
        return self.__fs.exists(filename=filename)

    def find(self, filename, filter=None, sort=lambda x: x._uploadDate, reverse=True, limit=None):
        def filter_wrapper(cursor):
            try:
                metadata = cursor.metadata
                metadata['_uploadDate'] = cursor.upload_date
                return filter(type('metadata', (object,), metadata)) if filter else True
            except:
                return False
        def sort_wrapper(cursor):
            try:
                return sort(type('metadata', (object,), cursor.metadata))
            except:
                return False 
        _cursors = [ _ for _ in self.__fs.find({"filename": filename}) ]
        _filtered = [ _ for _ in _cursors if filter_wrapper(_) ]
        _sorted = sorted(_filtered, key=sort_wrapper, reverse=reverse)
        return _sorted[:limit]

    def findOne(self, filename, filter=None, sort=lambda x: x._uploadDate, reverse=True):
        items = self.find(filename, filter, sort, reverse, 1)
        if items: return items[0]
        else: return None

    def commit(self, filename, blob, metadata={}, bare=False):
        _metadata = metadata
        if not bare and self.isExist(filename):
            _metadata = getattr(self.file(filename), 'metadata', metadata)
            _metadata.update(metadata)
        self.__fs.put(blob, filename=filename, metadata=_metadata)

    def revert(self, filename, limit=1):
        return self.delete(filename, limit=limit)

    def delete(self, filename, filter=None, sort=lambda x: x._uploadDate, reverse=True, limit=1):
        files = self.find(filename, filter, sort, reverse, limit)
        for file in files: self.__fs.delete(file._id)
        return len(files)

    def count(self, filename):
        return self.__fs.find({"filename": filename}).count()

    def info(self, filename, version=-1):
        metadata = self.file(filename, version).metadata
        count = self.count(filename)
        metadata['_historyCount'] = count
        metadata['_commitOrder'] = (count + version) if version < 0 else version 
        return metadata

    def history(self, filename):
        cursors = self.find(filename)
        return [{'_id': _._id, **_.metadata} for _ in cursors]

class GridIO:
    def __init__(self, name='__grid_io__', host='localhost:27017'):
        self.__db_name = name
        self.__host = host
        
        self.__client = client = MongoClient(host=host)
        self.__db = client[name]

        self.__buckets = {}
        bucket_names = [''.join(_.split('.')[:-1]) for _ in self.__db.collection_names() if _.endswith('.files')]
        for name in bucket_names: self.bucket(name)

    def __getitem__(self, name):
        return self.bucket(name)

    def __setitem__(self, name, value):
        if isinstance(value, GridIOBucket):
            self.__buckets[name] = value
        else:
            raise ValueError(f'Cannot assign instance of {value.__class__.__name__} class.')
    
    def __del__(self):
        self.release()

    def isExist(self, bucket_name):
        return bucket_name in self.__buckets

    def buckets(self):
        return self.__buckets.values()

    def bucket(self, name):
        if not name in self.__buckets:
            fs = gridfs.GridFS(self.__db, name)
            bucket = gridfs.GridFSBucket(self.__db)
            self.__buckets[name] = GridIOBucket(name, fs)
        return self.__buckets[name]
    
    def release(self):
        self.__client.close()
    
    def export(self, file, target='/tmp'):
        abspath = os.path.abspath(target)
        if os.path.isdir(target):
            target = abspath + f'/{file.name}'
        else:
            target = abspath
        with open(target, 'w+b') as f:
            f.write(file.read())
        return target

    def publish(self, file, bucket, obj_name=None, config={}):
        config['endpoint_url'] = config.get('endpoint_url') or os.environ.get('S3_ENDPOINT', 'http://localhost:9000')
        config['aws_access_key_id'] = config.get('aws_access_key_id') or os.environ.get('AWS_ACCESS_KEY_ID', '')
        config['aws_secret_access_key'] = config.get('aws_secret_access_key') or os.environ.get('AWS_SECRET_ACCESS_KEY', '')
        s3 = boto3.resource('s3', **config)
        try:
            s3.upload_fileobj(file, bucket, obj_name or file.name, ExtraArgs={'Metadata': file.metadata})
        except ClientError as e:
            logger.error(e)
            return False
        return True

sys.modules[__name__] = GridIO