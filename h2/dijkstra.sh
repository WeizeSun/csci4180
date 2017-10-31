#!/bin/bash

hadoop fs -rm -R /preprocessOutput
hadoop fs -rm -R /p1output
hadoop fs -rm -R /temp*
rm -R preprocessOutput
rm -R p1output
# hadoop com.sun.tools.javac.Main h2p1/PDPreProcess.java
# hadoop com.sun.tools.javac.Main h2p1/PDNodeWritable.java
# hadoop com.sun.tools.javac.Main h2p1/ParallelDijkstra.java
javac -cp $(hadoop classpath):. h2p1/*.java
jar cf pdpp.jar h2p1/*.class
# hadoop jar pdpp.jar h2p1.PDPreProcess /input /preprocessOutput
# hadoop fs -get /preprocessOutput
hadoop jar pdpp.jar h2p1.ParallelDijkstra /input /p1output $1 $2
hadoop fs -get /p1output
