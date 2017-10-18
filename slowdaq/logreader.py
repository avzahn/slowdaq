import json
import pandas as pd
from netstring import *
from netarray import from_timestamp, np_deserialize

def getDataFrames(fname,recursive=True):
    """
    Returns a dictionary indexed by publisher name of DataFrames
    containing all the log entries for each publisher, sorted by
    systime. Note that systime is converted to a datetime.datetime,
    and any netarray format arrays are converted to numpy arrays.

    Recursively flattens nested JSON objects in log lines that
    aren't netarray with recursive=True
    """
    frames = {}
    d = {}

    with File(fname,'r') as f:
        for line in f:
            line = json.loads(line)

            if recursive:
                line = flatten(line)

            name,parsed = process_line(line)

            if name in d:
                d[name].append(line)
            else:
                d[name] = [line]

    for name in d:
        frames[name] = pd.DataFrame(d[name])
        frames[name] = frames[name].sort('systime')

    return frames

def flatten(_d,parent=''):
    """
    Recursively expand nested dictionaries that aren't netarray
    """
    d = {}
    for key in _d.keys():
        d[parent+str(key)] = _d[key]

    for key in d.keys():
        
        val = d[key]

        # search for any nested dictionaries
        if isinstance(val,dict):
            if not 'adler32' in val.keys():
                d.update(flatten(val,parent=str(key)+'$'))
                del d[key]

    return d


exclude = set(['systime','event','source'])

def process_line(linedict):
    """
    Return a name, dict tuple where dict parses linedict.
    """

    out = {}

    name = linedict['source'][0]
    out['pid'] = linedict['source'][1]
    out['systime'] = from_timestamp(linedict['systime'])

    for key in linedict.keys():
        if not key in exclude:
            if isinstance(linedict[key],dict):
                if 'adler32' in linedict[key]:
                    out[key] = np_deserialize(line[key])
            else:
                out[key] = linedict[key]

    return name,out
