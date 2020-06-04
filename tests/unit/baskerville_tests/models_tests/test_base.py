# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import unittest

from baskerville.models.base import PipelineBase, BaskervilleBase

from baskerville.models.config import Config


class TestBaskervilleBase(unittest.TestCase):
    def setUp(self):
        self.dummy_conf = Config({})

    def test_instance(self):
        with self.assertRaises(TypeError) as terr:
            BaskervilleBase(self.dummy_conf)
        self.assertTrue(
            'Can\'t instantiate abstract class BaskervilleBase with abstract methods run' in str(
                terr.exception)
        )


class TestPipelineBase(unittest.TestCase):

    def test_instance(self):
        with self.assertRaises(TypeError) as terr:
            PipelineBase(None, None, None)
        self.assertTrue(
            'Can\'t instantiate abstract class PipelineBase with abstract methods finish_up, initialize, run'
            in str(terr.exception)
        )
