"""
List the objects in a bucket

"""

import os
import sys
import argparse
import boto3
import datetime
from dateutil.tz import tzutc

__author__ = 'Rob Edwards'

def print_objects(bucket_name):
    """
    list the objects in that bucket using the low level client
    """

    session = boto3.session.Session()

    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://projects.pawsey.org.au',
    )

    # this prints the json object returned:
    # print(s3_client.list_buckets())
    buckets = set()
    for bucket in s3_client.list_buckets()['Buckets']:
        buckets.add(bucket['Name'])

    if bucket_name not in buckets:
        print(f"Sorry, {bucket_name} not found in your available buckets. Your choices are:", file=sys.stderr)
        print("\n".join(buckets), file=sys.stderr)
        sys.exit(2)

    print("Name\tModified\tSize")
    for obj in s3_client.list_objects(Bucket=bucket_name)['Contents']:
        print(obj['Key'], end="\t")
        print(obj['LastModified'], end="\t")
        print(obj['Size'])

    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=' ')
    parser.add_argument('-b', help='bucket name', required=True)
    parser.add_argument('-v', help='verbose output', action='store_true')
    args = parser.parse_args()

    print_objects(args.b)

