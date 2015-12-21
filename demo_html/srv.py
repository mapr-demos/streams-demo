import SimpleHTTPServer
import SocketServer
import logging
import cgi
import sys
import os

if (len(sys.argv) != 4):
	logging.warning("usage: %s <port> <producer_pipe_name> " \
	"<consumer_pipe_name>")
	sys.exit(-1)
PORT = int(sys.argv[1])
PROD_PIPE_NAME = sys.argv[2]
CONS_PIPE_NAME = sys.argv[3]

# special commands for failing over
FAIL_BACK_CMD = "998";
FAIL_OVER_CMD = "999";

# ssh key to use when connecting to machines
SSH_KEYFILE = "/path/to/ssh/key.pem"

CONSUMER_HOST = "node67"
REMOTE_CLUSTER_HOSTS = "node220 node221 node222"
START_PATH = "/home/mapr/start_cons.sh"

# commands to make things happen when the buttons are pressed
CLUSTER_DISC_CMD = "for h in %s; \
	do ssh -i %s -l root 	\
	$h ip route add prohibit 172.31.43.0/24; done" % \
	(REMOTE_CLUSTER_HOSTS, SSH_KEYFILE)
CLUSTER_RECON_CMD = "for h in %s; \
	do ssh -i %s -l root 	\
	$h ip route delete prohibit 172.31.43.0/24; done" % \
	(REMOTE_CLUSTER_HOSTS, SSH_KEYFILE)
CONSUMER_START_CMD = "ssh %s -l mapr -i %s %s" % \
	(CONSUMER_HOST, SSH_KEYFILE, START_PATH)
CONSUMER_STOP_CMD = "ssh %s -l mapr -i %s pkill -TERM sensor_consumer" % \
	(CONSUMER_HOST, SSH_KEYFILE)
CONSUMER_FAIL_OVER_CMD = "ssh %s -l mapr -i %s 'echo %s > %s'" % \
    (CONSUMER_HOST, SSH_KEYFILE, FAIL_OVER_CMD, CONS_PIPE_NAME) 
CONSUMER_FAIL_BACK_CMD = "ssh %s -l mapr -i %s 'echo %s > %s'" % \
    (CONSUMER_HOST, SSH_KEYFILE, FAIL_BACK_CMD, CONS_PIPE_NAME)

def write_command(str_cmd):
	p = open(PROD_PIPE_NAME, 'w', 0)
	p.write(str_cmd)
	p.close()

def do_action(form):
	# get what happened from the cgi results
	for item in form.list:
		print "item name is %s" % item.name
		if (item.name == 'value'):
			val = item.value
		if (item.name == 'identifier'):
			#print "identifier is %s" % item.value
			which = item.value

	# now do something based on the result
	if (which == "cons"):
		# start the consumer here
		if (val == "run"):
			output = os.popen(CONSUMER_START_CMD).read()
			print "starting consumer, value %s" % val
		else:
			print "stopping consumer"
			output = os.popen(CONSUMER_STOP_CMD).read()
	elif (which == "failproducer"):
		if (val == "fail"):
			newspeed = FAIL_OVER_CMD
			print "writing producer failover code %s" % newspeed
		else:
			newspeed = FAIL_BACK_CMD
			print "writing producer failback code %s" % newspeed
		write_command(newspeed)
	elif (which == "failcons"):
		if (val == "fail"):
			print "writing consumer failover code"
			output = os.popen(CONSUMER_FAIL_OVER_CMD).read()
		else:
			print "writing consumer failback code"
			output = os.popen(CONSUMER_FAIL_BACK_CMD).read()
	elif (which == "disc"):
		print "running %s" % CLUSTER_DISC_CMD
		if (val == "disc"):
			print "disconnecting"
			output = os.popen(CLUSTER_DISC_CMD).read()
		else:
			print "connecting, value %s" % val
			output = os.popen(CLUSTER_RECON_CMD).read()
	elif (which == "slider-3"):
		# continue to write speed
		newspeed = val
		print "writing speed %s" % newspeed
		write_command(newspeed)
	else:
		print "unknown selector %s" % which

class ServerHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
	allow_reuse_address = True
	def do_GET(self):
		print "GET"
		print self.headers
		SimpleHTTPServer.SimpleHTTPRequestHandler.do_GET(self)
	def do_POST(self):
		logging.warning(self.headers)
		print "POST"
		#print self.headers
		form = cgi.FieldStorage(
		fp = self.rfile,
			headers=self.headers,
			environ={'REQUEST_METHOD':'POST',
			'CONTENT_TYPE':self.headers['Content-Type']})
		print "values:"
		do_action(form)
		SimpleHTTPServer.SimpleHTTPRequestHandler.do_GET(self)

class MyTCPServer(SocketServer.TCPServer):
	allow_reuse_address = True

Handler = ServerHandler
httpd = MyTCPServer(("", PORT), Handler)
print "Serving at: http://localhost:%d, sending producer commands to path %s" % (PORT, PROD_PIPE_NAME)
httpd.serve_forever()
