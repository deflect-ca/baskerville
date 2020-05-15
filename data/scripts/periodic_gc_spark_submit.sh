#!/usr/bin/env bash
# until this https://jira.apache.org/jira/browse/SPARK-24437 is resolved
# call gc periodically to clean up the executors' memory
pgrep -f 'org.apache.spark.deploy.SparkSubmit'| while read -r line ; do
  echo "Processing $line"
  jcmd $line GC.run;
done
