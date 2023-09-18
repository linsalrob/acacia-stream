"""
Print the contents of your buckets. This is a good way to check your setup

This is taken directly from
"""

import os
import sys
import argparse
import boto3

__author__ = 'Rob Edwards'

def print_buckets():
    """
    Use the AWS SDK for Python (Boto3) to create an Amazon Simple Storage Service
    (Amazon S3) resource and list the buckets in your account.
    This example uses the default settings specified in your shared credentials
    and config files.
    """

    session = boto3.session.Session()

    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://projects.pawsey.org.au',
    )

    print("Here are your buckets:")
    # this prints the json object returned:
    # print(s3_client.list_buckets())
    for bucket in s3_client.list_buckets()['Buckets']:
        print(f"\t{bucket['Name']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=' ')
    parser.add_argument('-f', help='input file', required=True)
    parser.add_argument('-v', help='verbose output', action='store_true')
    args = parser.parse_args()

    print_buckets()
