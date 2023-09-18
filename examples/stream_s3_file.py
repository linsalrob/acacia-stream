"""
Stream a file from S3 and print its contents.

Probably want to choose a small file to start with :)
"""

import os
import sys
import argparse
import boto3

__author__ = 'Rob Edwards'

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Stream an S3 file')
    parser.add_argument('-f', help='s3 file and location', required=True)
    parser.add_argument('-v', help='verbose output', action='store_true')
    args = parser.parse_args()


