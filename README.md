# Grid.IO
## GridFS(MongoDB) based utility to manage file with metadata easily.
### Example

#### Initialize
```python
from gridio import GridIO
gio = GridIO(name='GridIO', host='localhost:27017')
```

#### Get bucket
```python
bucket = gio.bucket("bucket_name")
bucket = gio["bucket_name"]
```

#### Get bucket list
```python
buckets = gio.buckets()
```

#### Check bucket visibility
```python
gio.isExist("bucket_name")
```

#### Drop bucket (Not yet support)
```python
gio.drop("bucket_name")
```

#### Get files from bucket.
```python
bucket.files()
```

#### Get latest version of file from bucket.
```python
bucket.file('model.pt')
bucket.file('model.pt', version=-1)
```

#### Commit file to bucket.
```python
data = np.array(range(10))
bucket.commit('model.pt', metadata={"accuracy": 0.9}, blob=data.tobytes())
```

#### Find specific files from bucket.
```python
bucket.find('model.pt', filter=lambda x: x.accuracy > 0.8, sort=lambda x: x._uploadDate, inverse=True)
```

#### Find specific a file from bucket.
```python
file = bucket.find('model.pt', filter=lambda x: x.accuracy > 0.8, sort=lambda x: x._uploadDate, inverse=True, limit=1)[0]
with bucket.findOne('model.pt', filter=lambda x: x.accuracy > 0.8) as f:
    f.read()
```

#### Show latest commit metadata
```python
bucket.info("model.pt")
```

#### Show file commit history
```python
bucket.history("model.pt")
```

#### Revert commit history
```python
bucket.revert('model.pt')
```

#### Remove specific histories of file
```python
bucket.delete('model.pt', filter=lambda x: x.accuracy < 0.8)
```

#### Get count of file history
```python
bucket.count('model.pt')
```

#### Export to local file.
```python
with bucket.file('model.pt') as f:
    gio.export(f, target='/tmp')
```

#### Publish file to S3 storage
```python
with bucket.file('model.pt') as f:
    gio.publish(f, "s3://[bucket_name]/[object_name]")
```

#### Disconnect with MongoDB
```python
gio.close()
```

