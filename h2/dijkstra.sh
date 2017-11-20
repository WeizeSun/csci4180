#!/bin/bash

hadoop fs -rm -R /preprocessOutput
hadoop fs -rm -R /p1output
hadoop fs -rm -R /temp*
rm -R preprocessOutput
rm -R p1output

javac -cp $(hadoop classpath):. h2p1/*.java
jar cf pd.jar h2p1/*.class
hadoop jar pd.jar h2p1.PDPreProcess /input /preprocessOutput
hadoop fs -get /preprocessOutput
hadoop jar pd.jar h2p1.ParallelDijkstra /input/test_dist.txt /p1output $1 $2
hadoop fs -get /p1output
