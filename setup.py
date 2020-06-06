#!/usr/bin/env python

from setuptools import setup

setup(
    name='tap-files',
    version='0.0.1',
    description='Singer.io tap for extracting data from files',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['tap_files'],
    install_requires=[
        'fsspec==0.7.4',
        'orjson==3.0.2',
        'singer-python==5.9.0'
    ],
    entry_points='''
      [console_scripts]
      tap-files=tap_files:main
    ''',
    packages=['tap_files']
)
