#!/bin/bash

tar -xvzf ./oozie-1.0-SNAPSHOT-bundle.tar.gz

hadoop fs -rm -r /dturbay/poc-oozie-app
hadoop fs -put poc-oozie-app /dturbay

export OOZIE_URL=http://localhost:11000/oozie
oozie job -config job.properties -run

