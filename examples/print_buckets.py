"""
Print the contents of your buckets. This is a good way to check your setup

This is taken directly from https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/python/example_code/s3/s3_basics/hello.py
and then modified to work on Acacia
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


    # now create a resource and do the same thing
    s3_resource = session.resource(
        service_name='s3',
        endpoint_url='https://projects.pawsey.org.au',
    )
    print("\nHere are your resource buckets")
    for bucket in s3_resource.buckets.all():
        print(f"\t{bucket.name}")

    

if __name__ == "__main__":
    print_buckets()
