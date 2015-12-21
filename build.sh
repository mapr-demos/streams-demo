#!/bin/bash

# Setup environment
export MAPR_HOME=/opt/mapr
GCC_OPTS="-ggdb -std=c99 \
 -Wl,--allow-shlib-undefined -I. -I${MAPR_HOME}/include \
-L${MAPR_HOME}/lib -lMapRClient -L${MAPR_HOME}/lib -lMarlinNative"

#Linking path
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${MAPR_HOME}/lib
export LD_RUN_PATH=${LD_RUN_PATH}:${MAPR_HOME}/lib

#Compile and Link
gcc ${GCC_OPTS} producer.c -o producer
gcc ${GCC_OPTS} consumer.c -o consumer
