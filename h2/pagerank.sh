#!/bin/bash

hadoop fs -rm -R /preprocessOutput
hadoop fs -rm -R /p2output
hadoop fs -rm -R /temp*
rm -R preprocessOutput
rm -R p2output

javac -cp $(hadoop classpath):. h2p2/*.java
jar cf pr.jar h2p2/*.class
# hadoop jar pr.jar h2p1.PDPreProcess /input /preprocessOutput
# hadoop fs -get /preprocessOutput
hadoop jar pr.jar h2p2.PageRank /input/test_dist_tut.txt /p2output $1 $2 $3
hadoop fs -get /p2output
