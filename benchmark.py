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

import time
import uuid
import ktasync
import importlib.machinery

loader = importlib.machinery.SourceFileLoader(
    "kyototycoon", "files/kyototycoon_orig.py"
)
kyototycoon = loader.load_module()

NUM_REQUESTS = 5000
NUM_BULK = 5

def _create_request():
    return [
        {
            bytes(
                uuid.uuid1().hex, encoding="UTF-8"
            ) : b'1' for n in range(NUM_BULK)
        } for n in range(NUM_REQUESTS)
    ]


def benchmark_get_bulk():
    client = ktasync.KyotoTycoon.embedded()
    requests = _create_request()

    [client.set_bulk_kv(req) for req in requests]

    start = time.time()
    [client.get_bulk_keys(req.keys()) for req in requests]
    print(
        'get_bulk qps:', int(NUM_REQUESTS * NUM_BULK / (time.time() - start))
    )


def benchmark_set_bulk():
    client = ktasync.KyotoTycoon.embedded()
    requests = _create_request()

    start = time.time()
    [client.set_bulk_kv(req) for req in requests]
    print(
        'set_bulk qps:', int(NUM_REQUESTS * NUM_BULK / (time.time() - start))
    )


def benchmark_orig_get_bulk():
    emb = ktasync.KyotoTycoon.embedded()
    client = kyototycoon.KyotoTycoon(port=emb.port)

    requests = _create_request()

    [client.set_bulk_kv(req, db=0) for req in requests]

    start = time.time()
    [client.get_bulk_keys(req.keys(), db=0) for req in requests]
    print(
        'orig get_bulk qps:', int(NUM_REQUESTS * NUM_BULK / (time.time() - start))
    )


def benchmark_orig_set_bulk():
    emb = ktasync.KyotoTycoon.embedded()
    client = kyototycoon.KyotoTycoon(port=emb.port)

    requests = _create_request()

    start = time.time()
    [client.set_bulk_kv(req, db=0) for req in requests]
    print(
        'orig set_bulk qps:', int(NUM_REQUESTS * NUM_BULK / (time.time() - start))
    )


benchmark_get_bulk()
benchmark_set_bulk()
benchmark_orig_get_bulk()
benchmark_orig_set_bulk()
