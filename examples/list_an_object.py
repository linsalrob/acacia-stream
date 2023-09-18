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

def print_an_object(object_name):
    """
    print the details about an object

    Note that we can have pseudo directories, like 

    databases/human/GCA_000001405.15_GRCh38_no_alt_plus_hs38d1_analysis_set.fna.gz

    in this case databases is our bucket and human/GCA_000001405.15_GRCh38_no_alt_plus_hs38d1_analysis_set.fna.gz is our object!
    """

    bucket_name, wanted = object_name.split('/', 1)

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

    for obj in s3_client.list_objects(Bucket=bucket_name)['Contents']:
        if obj['Key'] == wanted:
            print(obj)

    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=' ')
    parser.add_argument('-o', help='object name (including bucket)', required=True)
    parser.add_argument('-v', help='verbose output', action='store_true')
    args = parser.parse_args()

    print_an_object(args.o)

