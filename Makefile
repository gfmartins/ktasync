.PHONY: pypy cpy

cpy:
	sed 's/# cp #//' < benchmark.pyt > benchmark.py
	sed 's/# cp #//' < ktasync.pyt > ktasync.py

pypy:
	sed 's/# pypy #//' < benchmark.pyt > benchmark.py
	sed 's/# pypy #//' < ktasync.pyt > ktasync.py

