from setuptools import find_packages
from setuptools import setup

import killdupes


setup(
    name='killdupes',
    version=killdupes.__version__,
    description='Kill duplicate files, finding partial files as well',
    author='Martin Matusiak',
    author_email='numerodix@gmail.com',
    url='https://github.com/numerodix/killdupes',

    packages=find_packages('.'),
    package_dir={'': '.'},

    # don't install as zipped egg
    zip_safe=False,

    scripts=[
        'killdupes/killdupes.py',
    ],

    classifiers=[
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
)
