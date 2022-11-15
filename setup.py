import os
import sys
from setuptools import setup
from setuptools.command.test import test as TestCommand

short_description = "An object model for mapping antiSMASH data objects from and to Redis via aioredis."
long_description = open('README.md').read()

install_requires = [
    'redis'
]

tests_require = [
    'pytest',
    'coverage',
    'pytest-cov',
    'fakeredis',
    'pytest-asyncio',
    'flake8',
]


def read_version():
    for line in open(os.path.join('antismash_models', '__init__.py'), 'r'):
        if line.startswith('__version__'):
            return line.split('=')[-1].strip().strip("'")


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest
        errcode = pytest.main(self.test_args)
        sys.exit(errcode)


setup(
    name='antismash-models',
    version=read_version(),
    author='Kai Blin',
    author_email='kblin@biosustain.dtu.dk',
    description=short_description,
    long_description=long_description,
    long_description_content_type='text/markdown',
    install_requires=install_requires,
    tests_require=tests_require,
    packages=['antismash_models'],
    url='https://github.com/antismash/models/',
    license='GNU Affero General Public License v3 or later (AGPLv3+)',
    classifiers=[
        'Programming Language :: Python',
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)',
        'Operating System :: OS Independent',
    ],
    extras_require={
        'testing': tests_require,
    },
)
