import os
import sys

try:
    src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    # the best we can do to hope that we are in the test dir
    src_dir = os.path.dirname(os.getcwd())

sys.path.append(src_dir)
