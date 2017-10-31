#!/bin/bash

hadoop fs -rm -R /p2output
cd h1p2
hadoop com.sun.tools.javac.Main WordLengthCount.java
jar cf wlc.jar WordLengthCount*.class
hadoop jar wlc.jar WordLengthCount /shakespeare /p2output
cd -
hadoop fs -get /p2output
