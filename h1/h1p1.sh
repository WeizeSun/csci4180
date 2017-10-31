#!/bin/bash

hadoop fs -rm -R /output
cd ~/wordcount
hadoop com.sun.tools.javac.Main WordCount.java
jar cf wc.jar WordCount*.class
hadoop jar wc.jar WordCount /KJV12.TXT /output
cd -
hadoop fs -get /output .
