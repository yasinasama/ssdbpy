# -*- coding: utf-8 -*-

import os

from setuptools import setup

from ssdb import __version__


f = open(os.path.join(os.path.dirname(__file__), 'README.rst'))
long_description = f.read()
f.close()

setup(
    name='ssdb',
    version=__version__,
    description='Python client for ssdb',
    long_description=long_description,
    url='https://github.com/yasinasama/ssdbpy',
    author='yasinasama',
    author_email='yasinasama01@gmail.com',
    keywords='ssdb',
    license='MIT',
    packages=['ssdb'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ]
)