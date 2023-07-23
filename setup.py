#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

with open('HISTORY.md') as history_file:
    history = history_file.read()

requirements = [ 'm2r' ]

test_requirements = ['pytest>=3', ]

setup(
    author="EASL",
    author_email='hongyuhe.cs@googlemail.com',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="''",
    entry_points={
        'console_scripts': [
            'noserver=noserver.__main__:main',
        ],
    },
    install_requires=requirements,
    license="Apache Software License 2.0",
    long_description=readme + '\n\n' + history,
    long_description_content_type='text/markdown',
    include_package_data=True,
    keywords='noserver',
    name='noserver',
    packages=find_packages(include=['noserver', 'noserver.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/eth-easl/noserver.git',
    version='0.1.0',
    zip_safe=False,
)
