#!/bin/bash

# Small script to deploy jar files of a project in a mesos environment
all_jars=`./sbt/sbt get-jars | grep .jar`
split_jars=${all_jars//":"/" "}