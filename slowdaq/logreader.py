import json
import pandas as pd
from netstring import *
from netarray import from_timestamp, np_deserialize

def getDataFrames(fname):
	"""
	Returns a dictionary indexed by publisher name of DataFrames
	containing all the log entries for each publisher, sorted by
	systime. Note that systime is converted to a datetime.datetime,
	and any netarray format arrays are converted to numpy arrays.
	"""
	frames = {}
	d = {}


	with File(fname,'r') as f:
		for line in f:
			line = json.loads(line)
			name = line['source'][0]
			line['pid'] = line['source'][1]
			del line['source']
			line['systime'] = from_timestamp(line['systime'])

			for key in line.keys():
				if isinstance(line[key],dict):
					if 'adler32' in line[key]:
						line[key] = np_deserialize(line[key])

			if name in d:
				d[name].append(line)
			else:
				d[name] = [line]

	for name in d:
		frames[name] = pd.DataFrame(d[name])
		del frames[name]['event']
		frames[name] = frames[name].sort('systime')

	return frames




