#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014 Jean-Louis Fuchs
# Copyright (c) 2013 Ulrich Mierendorff
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


"""Binary protocol of Kyoto Tycoon with asyncio for io batching.

Kyoto Tycoon is a lightweight database server with impressive performance. It
can be accessed via several protocols, including an efficient binary protocol
which is used in this Python library.

The current implementation of this library provides access to the following
commands: set_bulk, get_bulk, remove_bulk (plus some wrapper functions to
easily use these commands if you only need to access a single item) and
play_script.

The library is implemented in pure Python and requires the module asyncio
and other Python standard library modules. Therefore, it is possible to use
the library with other interpreters than the standard CPython. The code has
been tested with python 3.4 since it is based on the asyncio module
introduced in 3.4. If pypy will implement asyncio in can be ported to
pypy and possibly other implementation.

"""

# TODO PEP8
# TODO Move documentation from homepage to code
# TODO Add some logging (tornados nice output?):w
# TODO embed factory method that creates a ktserver and client
#      -> find free random port automatically (within range)
#      -> with keep alive and logging of failures
# TODO compare original / asyincio wo batch / asyncio with batch
# TODO Write tests
# TODO remove that lazy stuff, since we are always lazy (batching)
# TODO sphinx doc setup (take snippets from freeze)
# TODO travis setup
# TODO github badge setup
# TODO stackoverflow question for promotion
# TODO adsy blogging


import socket
import random
import struct
import logging
import threading
import subprocess
import sys
import time
import atexit
try:
    import asyncio
except ImportError:
    import trollius as asyncio
    from trollius import From, Return

MB_SET_BULK     = 0xb8
MB_GET_BULK     = 0xba
MB_REMOVE_BULK  = 0xb9
MB_ERROR        = 0xbf
MB_PLAY_SCRIPT  = 0xb4

DEFAULT_HOST    = 'localhost'
DEFAULT_PORT    = 1978
DEFAULT_EXPIRE  = 0x7FFFFFFFFFFFFFFF
MAX_CONNECTIONS = 4

FLAG_NOREPLY = 0x01

RANGE_FROM = 2 ** 15 - 2 ** 14
RANGE_TO   = 2 ** 15 - 1


def _l():
    """Get the logger"""
    return logging.getLogger("ktasync")


class KyotoTycoonError(Exception):
    """Class for Exceptions in this module"""


class KyotoTycoon(object):
    """New connections are created using the constructor. A connection is
    automatically closed when the object is destroyed. There is the factory
    method embedded which creates a server and client connected to it.

    Keys and values of database entries are python bytes. You can pickle
    objects to bytes strings. The encoding is handled by the user when
    converting to bytes. Usually bytes(bla, encoding="UTF-8") is safe.

    """

    _client = None

    @staticmethod
    def embedded(
            args=None,
            timeout=None,
            max_connections=MAX_CONNECTIONS,
            range_from=RANGE_FROM,
            range_to=RANGE_TO
    ):
        """Start an embedded Kyoto Tycoon server and return a client conencted
        to it.

        :param args: Additional arguments for the Kyoto Tycoon server.

        :param timeout: Optional timeout for the socket. None means no timeout
                        (please also look at the Python socket manual).

        :param max_connections: Maximum connections for io batching.

        :param range_from: Port range to select a random port from (from).

        :param range_to: Port range to select a random port from (to).

        :rtype: KyotoTycoon

        """
        if KyotoTycoon._client:
            return KyotoTycoon._client
        if not args:
            args = []
        tries = 0
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        while tries < 20:
            tries += 1
            port = random.randint(range_from, range_to)
            try:
                sock.bind(("127.0.0.1", port))
                sock.listen(1)
                tries = 21
            except OSError:
                pass
            finally:
                sock.close()
                time.sleep(0.2)

        def keep_alive():
            """Helper"""
            while True:
                proc = subprocess.Popen(
                    [
                        "ktserver",
                        "-le",
                        "-host",
                        "127.0.0.1",
                        "-port",
                        str(port)
                    ] + args,
                    stderr=sys.__stderr__.fileno(),
                    stdout=sys.__stdout__.fileno(),
                )
                cleanup_done = [False]

                def cleanup():
                    """Helper"""
                    try:
                        cleanup_done[0] = True
                        proc.terminate()
                        proc.wait()
                    except ProcessLookupError:
                        pass

                atexit.register(cleanup)
                proc.wait()
                time.sleep(10)
                if not cleanup_done[0]:
                    _l().critical("ktserver died!")

        thr = threading.Thread(target=keep_alive)
        thr.setDaemon(True)
        thr.start()
        tries = 0
        while tries < 20:
            tries += 1
            try:
                KyotoTycoon._client = KyotoTycoon(
                    host="127.0.0.1",
                    port=port,
                    lazy=False,
                    timeout=timeout,
                    max_connections=max_connections
                )
                return KyotoTycoon._client
            except ConnectionRefusedError:
            # pypy #except Exception:
                time.sleep(0.2)
        raise KyotoTycoonError("Embedded server not started!")

    def __init__(
            self,
            host=DEFAULT_HOST,
            port=DEFAULT_PORT,
            lazy=True,
            timeout=None,
            max_connections=MAX_CONNECTIONS,
    ):
        """
        :param host: The hostname or IP to connect to, defaults to
                     'localhost'.

        :param port: The port number, defaults to 1978 which is the default
                     port of Kyoto Tycoon.

        :param lazy: If set to True, connection is not immediately established
                     on object creation, instead it is openend automatically
                     when required for the first time. This is the recommended
                     setting, because opening a connection is only necessary
                     if you actually use it.

        :param timeout: Optional timeout for the socket. None means no timeout
                        (please also look at the Python socket manual).
        """
        self.host            = host
        self.port            = port
        self.timeout         = timeout
        self.socket          = None
        self.loop            = asyncio.get_event_loop()
        self.max_connections = max_connections
        self.free_streams    = []
        self.semaphore       = asyncio.Semaphore(max_connections)
        if not lazy:
            self._connect()

    @asyncio.coroutine
    def set(self, key, val, db=0, expire=DEFAULT_EXPIRE, flags=0):
        """Wrapper function around set_bulk for easily storing a single item
        in the database.

        :param key: The key of the entry,

        :type key: bytes

        :param val: The value of the entry

        :type val: bytes

        :param db: Database index to store the record in. Default to 0.

        :type db: int

        :param expire: Expiration time for all entries.
                       kyototycoon.DEFAULT_EXPIRE is 0x7FFFFFFFFFFFFFFF which
                       means that the records should never expire in the
                       (near) future.

        :param flags: If set to kyototycoon.FLAG_NOREPLY, function will not
                      wait for an answer of the server.

        :return: The number of actually stored records, or None if flags was
                 set to kyototycoon.FLAG_NOREPLY.
        """
        return (yield from self.set_bulk(
        # pypy #raise Return((yield From(self.set_bulk(
            ((key, val, db, expire),), flags
        ))
        # pypy #))))

    @asyncio.coroutine
    def set_bulk_kv(self, kv, db=0, expire=DEFAULT_EXPIRE, flags=0):
        """Wrapper function around set_bulk for simplifying the process of
        storing multiple records with equal expiration times in the same
        database.

        :param kv: dict of key/value pairs.

        :param db: database index to store the values in. defaults to 0.

        :param expire: Expiration time for all entries.
                       kyototycoon.DEFAULT_EXPIRE is 0x7FFFFFFFFFFFFFFF which
                       means that the records should never expire in the
                       (near) future.

        :param flags: If set to kyototycoon.FLAG_NOREPLY, function will not
                      wait for an answer of the server.

        :return: The number of actually stored records, or None if flags was
                 set to kyototycoon.FLAG_NOREPLY.
        """
        recs = ((key, val, db, expire) for key, val in kv.items())
        return (yield from self.set_bulk(recs, flags))
        # pypy #raise Return((yield From(self.set_bulk(recs, flags))))

    @asyncio.coroutine
    def set_bulk(self, recs, flags=0):
        """Stores multiple records at once.

        :param recs: iterable (e.g. list) of records. Each record is a
                     list or tuple of 4 entries: key, val, db, expire

        :param flags: If set to kyototycoon.FLAG_NOREPLY, function will not
                      wait for an answer of the server.

        :return: The number of actually stored records, or None if flags was
                 set to kyototycoon.FLAG_NOREPLY.
        """
        sr, sw = yield from self._pop_streams()
        # pypy #sr, sw = yield From(self._pop_streams())
        try:
            request = [struct.pack('!BI', MB_SET_BULK, flags), None]

            cnt = 0
            for key, val, db, xt in recs:
                assert isinstance(key, bytes), "Please pass bytes as key"
                assert isinstance(val, bytes), "Please pass bytes as value"
                request.append(
                    struct.pack('!HIIq', db, len(key), len(val), xt)
                )
                request.append(key)
                request.append(val)
                cnt += 1

            request[1] = struct.pack('!I', cnt)

            sw.write(b''.join(request))

            if flags & FLAG_NOREPLY:
                self._push_streams(sr, sw)
                return None
                # pypy #raise Return(None)

            magic, = struct.unpack('!B', (
                yield from sr.readexactly(1))
            # pypy #    yield From(sr.readexactly(1)))
            )
            if magic == MB_SET_BULK:
                recs_cnt, = struct.unpack('!I', (
                    yield from sr.readexactly(4))
                # pypy #    yield From(sr.readexactly(4)))
                )
                self._push_streams(sr, sw)
                return recs_cnt
                # pypy #raise Return(recs_cnt)
            elif magic == MB_ERROR:
                raise KyotoTycoonError(
                    'Internal server error 0x%02x' % MB_ERROR
                )
            else:
                raise KyotoTycoonError('Unknown server error')
        finally:
            self._release_connection()

    @asyncio.coroutine
    def get(self, key, db=0, flags=0):
        """Wrapper function around get_bulk for easily retrieving a single
        item from the database.


        :param key: The key of the entry
        :type  key: bytes

        :param db: The database index. Defaults to 0.

        :param flags: reserved and not used now. (defined by protocol)

        :return: The value of the record, or None if the record could not be
                 found in the database.

        """
        recs = yield from self.get_bulk(((key, db),), flags)
        # pypy #recs = yield From(self.get_bulk(((key, db),), flags))
        if not recs:
            return None
            # pypy #raise Return(None)
        return recs[0][1]
        # pypy #raise Return(recs[0][1])

    @asyncio.coroutine
    def get_bulk_keys(self, keys, db=0, flags=0):
        """Wrapper function around get_bulk for simplifying the process of
        retrieving multiple records from the same database.

        :param keys: iterable (e.g. list) of keys.

        :param db: database index to store the values in. defaults to 0.

        :param flags: reserved and not used now. (defined by protocol)

        :return: dict of key/value pairs.
        """
        recs = ((key, db) for key in keys)
        recs = yield from self.get_bulk(recs, flags)
        return dict(((key, val) for key, val, db, xt in recs))
        # pypy #recs = yield From(self.get_bulk(recs, flags))
        # pypy #raise Return(dict(((key, val) for key, val, db, xt in recs)))

    @asyncio.coroutine
    def get_bulk(self, recs, flags=0):
        """Retrieves multiple records at once.

        :param recs: iterable (e.g. list) of record descriptions. Each
                     record is a list or tuple of 2 entries: key,db

        :param flags: reserved and not used now. (defined by protocol)

        :return: A list of records. Each record is a tuple of 4 entries: (key,
                 val, db, expire)
        """
        sr, sw = yield from self._pop_streams()
        # pypy #sr, sw = yield From(self._pop_streams())
        try:
            request = [struct.pack('!BI', MB_GET_BULK, flags), None]

            cnt = 0
            for key, db in recs:
                assert isinstance(key, bytes), "Please pass bytes as key"
                request.append(struct.pack('!HI', db, len(key)))
                request.append(key)
                cnt += 1

            request[1] = struct.pack('!I', cnt)

            sw.write(b''.join(request))
            res = yield from self._read_keys(sr, MB_GET_BULK)
            # pypy #res = yield From(self._read_keys(sr, MB_GET_BULK))
            self._push_streams(sr, sw)
            return res
            # pypy #raise Return(res)
        finally:
            self._release_connection()

    @asyncio.coroutine
    def _read_keys(self, sr, magic_expect):
        """Internal function for reading key from get_bulk or play_script"""
        data = yield from sr.readexactly(5)
        # pypy #data = yield From(sr.readexactly(5))
        magic, = struct.unpack('!B', data[:1])
        if magic == magic_expect:
            recs_cnt, = struct.unpack('!I', data[1:])
            recs_cnt -= 1
            recs = []
            # Reduce yields be reading key and next header at once
            if recs_cnt >= 0:
                data = yield from sr.readexactly(18)
                # pypy #data = yield From(sr.readexactly(18))
                pre_data = 0
                for _ in range(recs_cnt):
                    db, key_len, val_len, xt = struct.unpack(
                        '!HIIq', data[pre_data:]
                    )
                    pre_data = key_len + val_len
                    data = yield from sr.readexactly(pre_data + 18)
                    # pypy #data = yield From(sr.readexactly(pre_data + 18))
                    recs.append((data[:key_len], data[key_len:], db, xt))
                db, key_len, val_len, xt = struct.unpack(
                    '!HIIq', data[pre_data:]
                )
                pre_data = key_len + val_len
                data = yield from sr.readexactly(pre_data)
                # pypy #data = yield From(sr.readexactly(pre_data))
                recs.append((data[:key_len], data[key_len:], db, xt))
            return recs
            # pypy #raise Return(recs)
        elif magic == MB_ERROR:
            raise KyotoTycoonError(
                'Internal server error 0x%02x' % MB_ERROR
            )
        else:
            raise KyotoTycoonError('Unknown server error')

    @asyncio.coroutine
    def remove(self, key, db, flags=0):
        """Wrapper function around remove_bulk for easily removing a single
        item from the database.

        :param key: The key of the entry.
        :type  key: bytes

        :param db: database index to store the values in. defaults to 0.

        :param flags: If set to kyototycoon.FLAG_NOREPLY, function will not
                      wait for an answer of the server.

        :return: The number of removed records, or None if flags was set to
                 kyototycoon.FLAG_NOREPLY
        """
        return (yield from self.remove_bulk(((key, db),), flags))
        # pypy #raise Return((yield From(self.remove_bulk(((key, db),), flags))))

    @asyncio.coroutine
    def remove_bulk_keys(self, keys, db, flags=0):
        """Wrapper function around remove_bulk for simplifying the process of
        removing multiple records from the same database.

        :param keys: iterable (e.g. list) of keys.

        :param db: database index to store the values in. defaults to 0.

        :param flags: If set to kyototycoon.FLAG_NOREPLY, function will not
                      wait for an answer of the server.

        :return: The number of removed records, or None if flags was set to
                 kyototycoon.FLAG_NOREPLY
        """
        recs = ((key, db) for key in keys)
        return (yield from self.remove_bulk(recs, flags))
        # pypy #raise Return((yield From(self.remove_bulk(recs, flags))))

    @asyncio.coroutine
    def remove_bulk(self, recs, flags=0):
        """Remove multiple records at once.

        :param recs: iterable (e.g. list) of record descriptions. Each
                     record is a list or tuple of 2 entries: key,db

        :param flags: If set to kyototycoon.FLAG_NOREPLY, function will not
                      wait for an answer of the server.

        :return: The number of removed records, or None if flags was set to
                 kyototycoon.FLAG_NOREPLY
        """
        sr, sw = yield from self._pop_streams()
        # pypy #sr, sw = yield From(self._pop_streams())
        try:

            request = [struct.pack('!BI', MB_REMOVE_BULK, flags), None]

            cnt = 0
            for key, db in recs:
                request.append(struct.pack('!HI', db, len(key)))
                request.append(key)
                cnt += 1

            request[1] = struct.pack('!I', cnt)

            sw.write(''.join(request))

            if flags & FLAG_NOREPLY:
                self._push_streams(sr, sw)
                return None
                # pypy #raise Return(None)

            magic, = struct.unpack(
                '!B', (yield from sr.readexactly(1))
            # pypy #    '!B', (yield From(sr.readexactly(1)))
            )
            if magic == MB_REMOVE_BULK:
                recs_cnt, = struct.unpack(
                    '!I', (yield from sr.readexactly(4))
                # pypy #    '!I', (yield From(sr.readexactly(4)))
                )
                self._push_streams(sr, sw)
                return recs_cnt
                # pypy #raise Return(recs_cnt)
            elif magic == MB_ERROR:
                raise KyotoTycoonError(
                    'Internal server error 0x%02x' % MB_ERROR
                )
            else:
                raise KyotoTycoonError('Unknown server error')
        finally:
            self._release_connection()

    @asyncio.coroutine
    def play_script(self, name, recs, flags=0):
        """Calls a procedure of the LUA scripting language extension.

        :param name: The name of the LUA function.

        :param recs: iterable (e.g. list) of records. Each record is a list or
                     tuple of 2 entries: key, val

        :param flags: If set to kyototycoon.FLAG_NOREPLY, function will not
                      wait for an answer of the server.

        :return: A list of records. Each record is a tuple of 2 entries: (key,
                 val). Or None if flags was set to kyototycoon.FLAG_NOREPLY.
        """
        sr, sw = yield from self._pop_streams()
        # pypy #sr, sw = yield From(self._pop_streams())
        try:

            request = [
                struct.pack(
                    '!BII', MB_PLAY_SCRIPT, flags, len(name)
                ), None, name
            ]

            cnt = 0
            for key, val in recs:
                request.append(struct.pack('!II', len(key), len(val)))
                request.append(key)
                request.append(val)
                cnt += 1

            request[1] = struct.pack('!I', cnt)

            yield from sw.write(''.join(request))
            # pypy #yield From(sw.write(''.join(request)))

            if flags & FLAG_NOREPLY:
                self._push_streams(sr, sw)
                return None
                # pypy #raise Return(None)

            magic, = struct.unpack(
                '!B', (yield from sr.readexactly(1))
            # pypy #    '!B', (yield From(sr.readexactly(1)))
            )
            res = yield from self._read_keys(sr, MB_PLAY_SCRIPT)
            # pypy #res = yield From(self._read_keys(sr, MB_PLAY_SCRIPT))
            self._push_streams(sr, sw)
            return res
            # pypy #raise Return(res)
        finally:
            self._release_connection()

    def close(self):
        """Close the socket"""
        if self.socket is not None:
            self.socket.close()
            self.socket = None

    def _connect(self):
        """Conenct to server"""
        self.socket = socket.create_connection(
            (self.host, self.port),
            self.timeout
        )

    @asyncio.coroutine
    def _pop_streams(self):
        """Get a new stream. It will block (async) when max_connections is
        reached"""
        yield from self.semaphore.acquire()
        # pypy #yield From(self.semaphore.acquire())
        if self.free_streams:
            return self.free_streams.pop()
            # pypy #raise Return(self.free_streams.pop())
        else:
            return (yield from asyncio.open_connection(
            # pypy #raise Return((yield From(asyncio.open_connection(
                self.host,
                self.port,
            ))
            # pypy #))))

    def _release_connection(self):
        """Release the semaphore

        If a connection dies, we won't return it. Therefore release is an
        extra method."""
        self.semaphore.release()

    def _push_streams(self, sr, sw):
        """Return used stream."""
        self.free_streams.append((sr, sw))
