ktasync
=======

Kyoto Tycoon Asyncio (Python 3.4+). Binary protocol of Kyoto Tycoon with asyncio
for io batching.

Based on `Ulrich Mierendorffs`_ work.

.. _`Ulrich Mierendorffs`: http://www.ulrichmierendorff.com/software/kyoto_tycoon/python_library.html

Read the docs: http://ktasync.rtfd.org/en/dev/

PyPy/CPython 2.7: Supported via trolluis

benchmark
=========

Local::

    Starting
    orig get_bulk qps: 57596
    orig set_bulk qps: 34485
    get_bulk qps: 56630
    set_bulk qps: 33408
    batch get_bulk qps: 71625
    Connections used: 4
    dbm get qps: 142707
    dbm set qps: 26818
    kt set qps: 197260
    kt set qps: 198865

Remote::


    orig get_bulk qps: 1658
    orig set_bulk qps: 2225
    get_bulk qps: 2094
    set_bulk qps: 2162
    batch get_bulk qps: 3907 [1]
    Connections used: 20

[1] The operation I try to improve
