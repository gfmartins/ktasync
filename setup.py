"""Setup for ktasync"""

import os
from setuptools import setup


def read(fname):
    """Reads the README to long description"""
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "ktasync",
    version = "0.0.1",
    author = "Jean-Louis Fuchs",
    author_email = "ganwell@fangorn.ch",
    description = (
        "Binary protocol of Kyoto Tycoon with asyncio for io batching"
    ),
    license = "MIT",
    keywords = "kyoto tycoon key-value-store database asyncio",
    url = "https://github.com/ganwell/ktasync",
    py_modules=['ktasync', 'ktasync_test'],
    long_description=read('README.rst'),
    classifiers=[
        "Operating System :: OS Independent",
        "Natural Language :: English",
        "Intended Audience :: Developers",
        "Development Status :: 3 - Alpha",
        "Topic :: Software Development :: Libraries",
        "License :: OSI Approved :: MIT",
        "Programming Language :: Python :: 3.4",
    ],
)
