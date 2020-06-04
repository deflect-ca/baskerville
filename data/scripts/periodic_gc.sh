#!/usr/bin/env bash

# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# until this https://jira.apache.org/jira/browse/SPARK-24437 is resolved
# call gc periodically to clean up the executors' memory
pgrep -f 'org.apache.spark.executor.CoarseGrainedExecutorBackend'| while read -r line ; do
  echo "Processing $line"
  jcmd $line GC.run;
done
