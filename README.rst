ktasync
=======

Kyoto Tycoon Asyncio (Python 3.4+). Binary protocol of Kyoto Tycoon with asyncio
for io batching.

Based on `Ulrich Mierendorffs`_ work.

.. _`Ulrich Mierendorffs`: http://www.ulrichmierendorff.com/software/kyoto_tycoon/python_library.html

Read the docs: http://ktasync.rtfd.org/en/dev/

PyPy: Supported via trolluis

benchmark
=========

Local::

    orig get_bulk qps: 34811
    orig set_bulk qps: 26580
    get_bulk qps: 40689
    set_bulk qps: 24900
    batch get_bulk qps: 63306 [1]
    Connections used: 20
    dbm get qps: 31883
    dbm get qps: 8803

Remote::


    orig get_bulk qps: 1658
    orig set_bulk qps: 2225
    get_bulk qps: 2094
    set_bulk qps: 2162
    batch get_bulk qps: 3907 [1]
    Connections used: 20

[1] The operation I try to improve
