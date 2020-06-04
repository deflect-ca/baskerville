# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import os
import sys

try:
    src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    src_dir = os.path.dirname(os.getcwd())

sys.path.append(src_dir)
