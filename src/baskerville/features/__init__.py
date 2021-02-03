# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import itertools
import os

from baskerville.features.base_feature import BaseFeature, TimeBasedFeature, UpdateableFeature

feature_dir = os.path.dirname(__file__)
feature_files = [f.name.replace('.py', '')
                 for f in os.scandir(feature_dir)
                 if f.is_file() and f.name.endswith('.py')]

for f in feature_files:
    __import__('.'.join(['baskerville', 'features', f]))

FEATURES = set(
    BaseFeature.__subclasses__() +
    TimeBasedFeature.__subclasses__() +
    list(itertools.chain(
        *[
            subclass.__subclasses__()
            for subclass in UpdateableFeature.__subclasses__()
        ]
    ))
)
FEATURE_NAME_TO_CLASS = dict(
        (f.feature_name_from_class(), f) for f in FEATURES
    )