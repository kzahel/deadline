#!/usr/bin/env python
#
#

__author__ = 'Kyle Graehl'
__author_email__ = 'kgraehl@gmail.com'

from setuptools import setup, find_packages

setup(
    name = "deadline",
    version = "0.1",
    packages = find_packages(),
    author = __author__,
    author_email = __author_email__,
    description = "deadline",
    install_requires = [ ],
    package_data = {
        "": ['data/*', 'data/.*'],
        },
    )
