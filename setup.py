#!/usr/bin/env python3

from distutils.core import setup

setup(
    name='encx_s3',
    version='0.1',
    description='Extension to Encx that adds the S3 capability',
    author='KJ',
    author_email='<redacted>',
    url='https://github.com/jdotpy/encx_s3',
    packages=[
        'encx_s3',
    ],
    install_requires=[
        'boto3',
    ],
)
