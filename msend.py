#!/usr/bin/python

import potsdb
import sys

# set this to the hostname of the host running opentsdb
METRICS_HOST = 'node71'

if (len(sys.argv) != 3):
	print "usage: %s %s %s" (sys.argv[0], "<metric_name>", "<metric_value>")
	sys.exit(-1)

MNAME = sys.argv[1]
MVALUE = int(sys.argv[2])

print "sending metric %s = %d " % (MNAME, MVALUE)
metrics = potsdb.Client(METRICS_HOST)
metrics.send(MNAME, MVALUE)
metrics.wait()
print "done"
sys.exit(0)
