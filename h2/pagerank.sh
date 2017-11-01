#!/bin/bash

hadoop fs -rm -R /preprocessOutput
hadoop fs -rm -R /p2output
hadoop fs -rm -R /temp*
rm -R preprocessOutput
rm -R p2output
# hadoop com.sun.tools.javac.Main h2p1/PDPreProcess.java
# hadoop com.sun.tools.javac.Main h2p1/PDNodeWritable.java
# hadoop com.sun.tools.javac.Main h2p1/ParallelDijkstra.java
javac -cp $(hadoop classpath):. h2p2/*.java
jar cf pr.jar h2p2/*.class
# hadoop jar pr.jar h2p1.PDPreProcess /input /preprocessOutput
# hadoop fs -get /preprocessOutput
hadoop jar pr.jar h2p1.ParallelDijkstra /input /p1output $1 $2
hadoop fs -get /p1output
