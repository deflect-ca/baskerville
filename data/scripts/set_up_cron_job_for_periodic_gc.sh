#!/usr/bin/env bash

# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

echo "Setting up cron job for periodic gc"

# This assumes $BASKERVILLE_ROOT is set
export PERIODIC_SCRIPT=$BASKERVILLE_ROOT/data/scripts/periodic_gc.sh

# uncomment to run hourly
# cp $PERIODIC_SCRIPT /etc/cron.hourly
chmod +x $PERIODIC_SCRIPT

(crontab -l ; echo "PATH=$PATH
*/5 * * * * $PERIODIC_SCRIPT";) | uniq - | crontab -
