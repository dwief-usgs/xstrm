#!/usr/bin/env python

"""The setup script."""
from setuptools import find_packages
from setuptools import setup

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest>=3', ]

setup(
    author="Daniel Wieferich",
    author_email='dwieferich@usgs.gov',
    python_requires='>=3.8',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.8',
    ],
    description="Python methods to assist in spatially assinging and summarizing information to stream networks.",
    install_requires=requirements,
    long_description=readme,
    include_package_data=True,
    keywords='xstrm',
    name='xstrm',
    packages=find_packages(include=['xstrm', 'xstrm.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/dwief-usgs/xstrm',
    version='0.0.1',
    zip_safe=False,
)
