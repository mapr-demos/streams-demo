#!/bin/bash

STREAM=/mapr/mdemo/data_hq:sens_hq
STREAM_LOCAL=/mapr/mdemo/data_hq:sens_local
BSTREAM=/mapr/mdemo2/data_remote:sens_hq
BSTREAM_LOCAL=/mapr/mdemo2/data_remote:sens_local
PIPE=/tmp/conspipe

# start two consumers, one for each remote/local topic combo
nohup /home/mapr/sensor_consumer $STREAM $STREAM_LOCAL \
       $BSTREAM $BSTREAM_LOCAL $PIPE all > /dev/null &
