#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014 Jean-Louis Fuchs
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

"""Tests for ktasync"""

try:
    import unittest2      as unittest
except ImportError:  # pragma: no cover
    import unittest
# try:
# TODO Use or remove
#     import unittest.mock  as mock
# except ImportError:  # pragma: no cover
#     import mock

import ktasync


class KtasyncTest(unittest.TestCase):
    def test_just_connect(self):
        a = ktasync.KyotoTycoon.embedded()
        self.assertIsInstance(a, ktasync.KyotoTycoon)

    def test_set(self):
        a = ktasync.KyotoTycoon.embedded()
        self.assertIsInstance(a, ktasync.KyotoTycoon)
        a.set(b"huhu", b"super", 0)

    def test_get(self):
        a = ktasync.KyotoTycoon.embedded()
        self.assertIsInstance(a, ktasync.KyotoTycoon)
        a.set(b"huhu", b"best", 0)
        val = a.get(b"huhu", 0)
        self.assertEqual(val, b"best")

# pylama:ignore=E0611,C0111
