"""Setup for ktasync"""

import os
import sys
from setuptools import setup
from setuptools.command.install import install

is_cp2  = not sys.version_info[0] > 2
is_pypy = '__pypy__' in sys.builtin_module_names

install_requires = []
if is_pypy or is_cp2:
    install_requires.append("trollius")


class CustomInstallCommand(install):
    """Source convesion"""
    def run(self):
        if is_pypy or is_cp2:
            with open("ktasync.pyt") as templ:
                with open("ktasync.py", "w") as out:
                    for line in templ:
                        out.write(line.replace("# pypy #", ""))

        install.run(self)


def read(fname):
    """Reads the README to long description"""
    with open(os.path.join(os.path.dirname(__file__), fname)) as f:
        return f.read()

setup(
    name = "ktasync",
    version = "0.0.1",
    author = "Jean-Louis Fuchs",
    author_email = "ganwell@fangorn.ch",
    description = (
        "Binary protocol of Kyoto Tycoon with asyncio for io batching"
    ),
    license = "MIT",
    install_requires = install_requires,
    cmdclass = {
        'install': CustomInstallCommand,
    },
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
        "Topic :: Database :: Front-Ends",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],
)
