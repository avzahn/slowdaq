"""
Defines an array serialization format, and implements it for encoding and
decoding numpy arrays. The format is intended to be reasonably space efficient
while being nearly human readable. It's also highly self explanatory.

Example:

    arr = np.random.rand(5).astype('float32')
    netarr = np_serialize(arr)
    print netarr
    
    >> {"adler32":-500036670,
        "dtype": "float32",
        "shape": [5],
        "buf": "f398e43e5434963ece1c133f8c1ced3ddf7cef3e"}
        
    arr_, checksum = np_deserialize(netarr)
    
    print arr == arr_
    >> True

"""
from binascii import hexlify, unhexlify
import datetime
import numpy as np
import zlib
import json

def np_serialize(arr):
    # ascii encoded hex on the row-major flattened array
    buf = hexlify(arr.flatten().tobytes())
    # checksum
    adler32 = a32(buf)
    
    d={'shape' : arr.shape,
        'dtype' : str(arr.dtype),
        'buf' :buf,
        'adler32': adler32}

    return json.dumps(d)
    
def np_deserialize(netarr):
    
    d = json.loads(netarr)
    buf = d['buf']
    shape = d['shape']
    dtype = d['dtype']
    arr = np.fromstring(unhexlify(buf),dtype=dtype)
    arr = arr.reshape(shape)
    
    if  'adler32' in d:
        adler32 = d['adler32']
    else:
        adler32 = None
    
    return arr, adler32
    
def a32(data,blocksize=(1<<28)):
    """
    Cyclic adler32 checksum with a default blocksize of 2^28 bytes ~= 268 MB
    """
    if len(data) < blocksize:
        block = data[0:len(data)]
        return zlib.adler32(block)
    else:    
        block = data[0:blocksize]
    
    chk = zlib.adler32(block)
    start = blocksize
    
    while start < len(data):
        
        end = min(start+blocksize,len(data))
        block = data[start:end]
        start += blocksize
                
        chk = zlib.adler32(block,chk)
        
        if end == len(data):
            break
        
    return chk

timestamp_fmt = '%Y-%m-%d:%H:%M:%S:%f'

now = datetime.datetime.utcnow

def timestamp(dt=None):
    if dt == None:
        dt = datetime.datetime.utcnow()
    return dt.strftime(timestamp_fmt)

def from_timestamp(stamp):
    return datetime.datetime.strptime(stamp,timestamp_fmt)

def apply_timestamp(d):
    d['systime'] = timestamp()
    
