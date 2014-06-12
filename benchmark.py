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
import asyncio
import importlib.machinery  # noqa

loader = importlib.machinery.SourceFileLoader(
    "kyototycoon", "files/kyototycoon_orig.py"
)
kyototycoon = loader.load_module()

NUM_REQUESTS = 1000
NUM_BULK = 5
NUM_BATCH = 20


loop = asyncio.get_event_loop()

client = ktasync.KyotoTycoon.embedded()
#client = ktasync.KyotoTycoon(host="harry")
orig   = kyototycoon.KyotoTycoon(host=client.host,port=client.port)

def _create_request():
    """Get requests"""
    return [
        {
            bytes(
                uuid.uuid1().hex, encoding="UTF-8"
            ) : b'1' for n in range(NUM_BULK)
        } for n in range(NUM_REQUESTS)  # noqa
    ]


def benchmark_get_bulk():
    """Standard bulk test"""
    requests = _create_request()

    @asyncio.coroutine
    def prepare():
        """Helper"""
        for req in requests:
            yield from client.set_bulk_kv(req)

    @asyncio.coroutine
    def doit():
        """Helper"""
        for req in requests:
            res = yield from client.get_bulk_keys(req.keys())

    loop.run_until_complete(prepare())

    start = time.time()
    loop.run_until_complete(doit())
    print(
        'get_bulk qps:',
        int(NUM_REQUESTS * NUM_BULK / (time.time() - start))
    )


def benchmark_batch_get_bulk():
    """Batch bulk test"""
    requests = _create_request()

    @asyncio.coroutine
    def prepare():
        """Helper"""
        for req in requests:
            yield from client.set_bulk_kv(req)

    @asyncio.coroutine
    def doit(from_, to_):
        """Helper"""
        for req in requests[from_:to_]:
            res = yield from client.get_bulk_keys(req.keys())

    loop.run_until_complete(prepare())

    start = time.time()
    batchs = []
    step = NUM_REQUESTS / NUM_BATCH
    cur = int(step)
    last = 0
    while cur <= NUM_REQUESTS:
        batchs.append(doit(last, cur))
        last = int(cur)
        cur += int(step)
    loop.run_until_complete(asyncio.wait(batchs))
    print(
        'batch get_bulk qps:',
        int(NUM_REQUESTS * NUM_BULK / (time.time() - start))
    )
    print("Connections used: %d" % len(client.free_streams))


def benchmark_set_bulk():
    """Standard bulk test"""
    requests = _create_request()

    @asyncio.coroutine
    def doit():
        """Helper"""
        for req in requests:
            yield from client.set_bulk_kv(req)

    start = time.time()
    loop.run_until_complete(doit())
    print(
        'set_bulk qps:',
        int(NUM_REQUESTS * NUM_BULK / (time.time() - start))
    )


def benchmark_orig_get_bulk():
    """Original bulk test"""

    requests = _create_request()

    [orig.set_bulk_kv(req, db=0) for req in requests]

    start = time.time()
    [orig.get_bulk_keys(req.keys(), db=0) for req in requests]
    print(
        'orig get_bulk qps:',
        int(NUM_REQUESTS * NUM_BULK / (time.time() - start))
    )


def benchmark_orig_set_bulk():
    """Original bulk test"""
    requests = _create_request()

    start = time.time()
    [orig.set_bulk_kv(req, db=0) for req in requests]
    print(
        'orig set_bulk qps:',
        int(NUM_REQUESTS * NUM_BULK / (time.time() - start))
    )


benchmark_get_bulk()
benchmark_set_bulk()
benchmark_batch_get_bulk()
benchmark_orig_get_bulk()
benchmark_orig_set_bulk()
# pylama:ignore=W0106
