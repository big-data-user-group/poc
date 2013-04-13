#!/bin/bash

hadoop fs -rm -r /out
set HADOOP_CLASSPATH=/home/cloudera/work/etl-1.0-SNAPSHOT.jar
hadoop jar etl-1.0-SNAPSHOT.jar org.bd.poc.Importer /out -libjars /home/cloudera/work/etl-1.0-SNAPSHOT.jar
