# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import os


def get_default_data_path():
    """
    Returns the absolute path to the data folder
    :return:
    """
    return f'{os.path.dirname(os.path.realpath(__file__))}/../data'


def get_default_log_path():
    """
    Returns the absolute path to the log folder
    :return:
    """
    return f'{os.path.dirname(os.path.realpath(__file__))}/../logs'


test_baskerville_conf = {
    'database': {
        'name': 'test_db',
        'user': 'postgres',
        'password': '***',
        'host': 'localhost'
    },
    'spark': {
        'db_driver': 'test',
        'storage_level': 'MEMORY_AND_DISK',
    },
    'engine': {
        'raw_log': {},
        'extra_features': ['css_to_html_ratio', ]

    }
}
