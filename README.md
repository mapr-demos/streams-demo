# streams-demo
This repo contains the source code for the MapR Streams demo.  A video of the demo can be seen here:  https://www.mapr.com/products/www.youtube.com/watch?v=hY-GJzSNQ8k

The repo is provided mainly to give access to code samples.  Instructions for setting up the demo require some configuration and cluster resources and may not be covered completely here, but it should be enough to mostly get you going.

# Prerequisites
This repo contains only the source code for the demo functionality for use as example code.  If you would like to run the demo in its entirety, you will need the following -- these require some additional setup on your own and are not covered here:
- Two MapR clusters with Streams enabled
- Two streams created with replication between the two clusters
- OpenTSDB and Grafana installed on one of the nodes in the cluster
- An edge node for running web services and the producer, with the MapR Client package installed
- A data set from which to stream data in the producer.  You can generate your own JSON data sets using Ted Dunning's log-synth tool here: https://github.com/tdunning/log-synth

For instructions on how to setup MapR Streams, consult the MapR documentation here:  http://doc.mapr.com/display/MapR/Home

For instructions on how to setup OpenTSDB on MapR, consult this page:  http://doc.mapr.com/display/MapR/Using+OpenTSDB+with+AsyncHBase+1.6+and+MapR-DB+4.1

# How it all works
The demo consists of the following parts:
- Producer and Consumer code:  (`producer.c` and `consumer.c`)
- Build script to build the above files (`build.sh`)
- Web pages with javascript to control the demo (files in the `demo_html` directory)
- Python script to receive the POST commands from the web pages and perform actions (`srv.py`)
- Python script to report Streams metrics to OpenTSDB (`report.py`)
- Helper script to run the consumer from ssh (`start_cons.sh`)

The producer must be run from the same node as the Python web script as they communicate over a named pipe.

To setup the demo, first setup the MapR topology from the 'Prerequisites' section.  The names of the streams and topics that you choose are flexible and are given as command line arguments to most of the tools, or are defined in the top of the file.

On the edge node:
- Make a named pipe on the local filesystem:  `mkfifo /tmp/thepipe`
- Edit `demo_html/srv.py` and ensure that the variables at the top are correct for your setup, for example, CONSUMER_HOST
- Run `srv.py` in the demo_html/ directory as follows:  `python srv.py 9988 /tmp/thepipe /tmp/conspipe` (this will start the web service on port 9988)
- Build the producer by running `build.sh`.  You must have at least the mapr-client package installed for this to work.
- Run the producer with:  `./producer /mapr/mdemo2/data_remote:sensors /mapr/mdemo/data_hq:sensors datafile.json /tmp/thepipe` - substitute `datafile.json` with your data set.

On one of the MapR nodes, let's call this the reporting host.  This can be on either cluster.
- Copy `report.py` to this machine and edit the top of the file to make sure variables are correct for your setup.
- Run `report.py` on each of the streams (it's easy to do this in screen(1)).  Start two screens.  In one, enter `./report.py /mapr/mdemo/data_hq`, in another, enter `./report.py /mapr/mdemo2/data_remote`.  This will start reporting stream metrics to the node running OpenTSDB.

On another one of the MapR nodes, let's call this the consumer host, it can be the same as the reporting host.  This should be on the 'remote' cluster.
- Copy the file `start-cons.sh` to the machine (this must be the same path as in START_PATH in srv.py above)
- Copy over `consumer.c` and `build.sh` to this machine as well.  Run `build.sh` to build the consumer.
- Make a named pipe for the consumer with `mkfifo /tmp/conspipe`

On another one of the MapR nodes, let's call this the OpenTSDB host, it can be the same as any of the hosts above:
- Install and start OpenTSDB according to the instructions from the first section.
- Install start Grafana according to its install instructions here:  http://docs.grafana.org/installation/

At this point you should be ready to go:
- Open a web browser and go to the url `http://<host>:9988/demo.html`, substituting `host` for the host where you ran `srv.py` above.
- Open another browser session to the Grafana session you created above: `http://<host>:3000`

You should now be able to fail-over the consumer, producer and break connectivity between the two hosts.  The OpenTSDB metric names (referenced when building the Grafana dashboard) are prefixed with `streamstats` and contain the stream name.
