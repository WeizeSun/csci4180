#!/bin/bash

hadoop fs -rm -R /p4output
rm -R p4output
cd h1p4
hadoop com.sun.tools.javac.Main NgramRF.java
jar cf ngrf.jar NgramRF*.class
hadoop jar ngrf.jar NgramRF /hw1/shakespeare /p4output $1 $2
cd -
hadoop fs -get /p4output
