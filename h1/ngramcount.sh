#!/bin/bash

hadoop fs -rm -R /p3output
rm -R p3output
cd h1p3
hadoop com.sun.tools.javac.Main NgramCount.java
jar cf ngc.jar NgramCount*.class
hadoop jar ngc.jar NgramCount /input /p3output $1
cd -
hadoop fs -get /p3output
