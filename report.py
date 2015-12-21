#!/usr/bin/python

#
# this script pulls metrics from the MapR CLI commands to
# report on MapR Streams as OpenTSDB metrics,
# for use on a dashboard, etc.
#

import subprocess
import commands
import potsdb
import time
import ciso8601
import sys
import thread
from threading import Timer


if (len(sys.argv) != 2):
	print "usage: %s %s" (sys.argv[0], "/path_to_stream")
	sys.exit(-1)
SPATH = sys.argv[1]

# set this to the hostname of the host running opentsdb
METRICS_HOST = 'node71'

# set this to the preferred prefix of metrics reported to opentsdb
NAME_PREFIX = "streamstats"

# set this to the timeout from the command
# for example, if we can't reach the CLDB, the command will hang,
# we will pretend we received a line of data that was exactly the same
# as the previous (for time reporting purposes)
DEF_TIMEOUT = "10"

# some slack in reporting how much is left 
UNCONS_OFFSET_TOLERANCE = 12000

# for demo purposes when CLDB is unreachable
FUDGE_RATE = 5

def get_cmd_output(p, skip, dval):
	a = []
	c = 1
	while True:
		l = p.stdout.readline()
		print "line: %s" % l
		c -= 1
		if (c == 0 and skip == 1):
			continue
		if l != '':
			# fixup line
			# so comma-sep values appear as single field
			a.append(l.replace(', ', ',').rstrip())
			print "appended line %s" % l
		else:
			if (dval != None and len(a) == 0):
				for dline in dval:
					print "appended default line %s" % dval
					a.append(dline)
			break
	return (a)

def metrics_send(n, tn, v):
	name = "%s.%s.%s.%s" % (NAME_PREFIX, SPATH, tn, n)
	print "sending metric %s = %d " % (name, v)
	metrics = potsdb.Client(METRICS_HOST)
	metrics.send(name, v)
	metrics.wait()

poffset = 0
topicbuf = {}
topic_cons_cache = {}
topic_prod_cache = {}

while True:
	p = subprocess.Popen(['/usr/bin/timeout', DEF_TIMEOUT, \
	    '/usr/bin/maprcli', \
	    'stream', 'topic', 'list', '-path', SPATH ],
	    stdout=subprocess.PIPE, 
	    stderr=subprocess.PIPE)
	
	# first get the list of topics
	tdump = get_cmd_output(p, 1, None)
	tlist = []
	for l in tdump:
		t, p, ls, c, ml, ps = l.split()
		print "topic is %s" % t
		tlist.append(t)
	
	for tp in tlist:
		dval = None
		if tp not in topicbuf:
			thistopicbuf = topicbuf[tp] = []
		else:
			thistopicbuf = topicbuf[tp]

			# a bit hackish -- if we get no value, pretend like we got 
			# the previous 2 values
			if (len(thistopicbuf) > 1):
				(dtstamp1, dval1) = thistopicbuf[len(thistopicbuf) - 1]
				(dtstamp2, dval2) = thistopicbuf[len(thistopicbuf) - 2]
				dval = [ dval1, dval2 ]
		tstamp = int(time.time())

		p = subprocess.Popen(['/usr/bin/timeout', DEF_TIMEOUT, \
		    '/usr/bin/maprcli', \
		    'stream', \
		    'topic', 'info', '-path', SPATH, '-topic', tp, ],
		    stdout=subprocess.PIPE, 
		    stderr=subprocess.PIPE)
		onetopicdump = get_cmd_output(p, 1, dval)
	
		for ot in onetopicdump:
			# print "line: %s" % ot
			thistopicbuf.append([tstamp, ot])
		topicbuf[tp] = thistopicbuf

	print "gathered %d topic(s), sending metrics" % len(tlist)
	allprod_rate = 0
	for tp in topicbuf:
		data = topicbuf[tp]
		latest_ts, latest_line = data[len(data) - 1]

		# if we only have 1 data point, wait
		# until we get 2 to have a delta,
		# otherwise this is noisy
		if (len(data) == 1):
			print "skipping reporting for " \
			"topic %s, need another data point" % tp
			continue
		prev_ts, prev_line = data[(len(data) - 2)]

		fid, mintimestamp, partitionid, \
		    mintimestampacrossconsumers, servers, \
		    logicalsize, minoffsetacrossconsumers, \
 		    maxtimestamp,  \
		    maxoffset, physicalsize, master = latest_line.split()

		p_fid, p_mintimestamp, p_partitionid, \
		    p_mintimestampacrossconsumers, p_servers, \
		    p_logicalsize, p_minoffsetacrossconsumers, \
 		    p_maxtimestamp, p_maxoffset, p_physicalsize, \
		    p_master = prev_line.split()

		# this means we didn't get any data, just continue to report
		# the same metric we had (for demo purposes)
		denom = latest_ts - prev_ts
		if (denom == 0):
			inc_prod_rate = topic_prod_cache[tp]
			inc_cons_rate = topic_cons_cache[tp]
		else:
			inc_prod_rate = (int(maxoffset) - int(p_maxoffset)) \
			     / denom 
			inc_cons_rate = (int(minoffsetacrossconsumers) - \
				int(p_minoffsetacrossconsumers)) / denom

		metrics_send("producer_rate", tp, inc_prod_rate)
		allprod_rate += inc_prod_rate
		topic_prod_cache[tp] = inc_prod_rate

		metrics_send("consumer_rate", tp, inc_cons_rate)
		allprod_rate += inc_cons_rate
		topic_cons_cache[tp] = inc_cons_rate

		# send the size of the unread data
		unc_size = int(maxoffset) - int(minoffsetacrossconsumers)
		if (unc_size < UNCONS_OFFSET_TOLERANCE):
			unc_size = 0
		metrics_send("unconsumed", tp, unc_size)

		# send physical size
		metrics_send("physical_size", tp, int(physicalsize))

		# send logical size
		metrics_send("logical_size", tp, int(logicalsize))

		# send time lag
		t1 = ciso8601.parse_datetime(maxtimestamp)
		t2 = ciso8601.parse_datetime(mintimestamp)
		lag = time.mktime(t1.timetuple()) - time.mktime(t2.timetuple())
		metrics_send("time_lag", tp, int(lag))

	# finally send the total for the entire stream
	metrics_send("total_rate", tp, allprod_rate)
