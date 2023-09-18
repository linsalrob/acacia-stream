"""
Stream a file from S3 and print its contents.

Probably want to choose a small file to start with :)

If you have the write permissions, this works

python examples/stream_s3_file.py -o databases/human/GCA_000001405.15_GRCh38_no_alt_plus_hs38d1_analysis_set.fna.gz -w ~/scratch/tmp.gz

"""

import os
import sys
import argparse
import boto3
import zlib

__author__ = 'Rob Edwards'

def stream_object(object_name, outfile):
    """
    Stream an object. We need its path

    in this case databases is our bucket and human/GCA_000001405.15_GRCh38_no_alt_plus_hs38d1_analysis_set.fna.gz is our object!
    """

    bucket_name, wanted = object_name.split('/', 1)

    session = boto3.session.Session()

    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://projects.pawsey.org.au',
    )

    # we could go directly, but this runs some sanity checks on our request
    buckets = set()
    for bucket in s3_client.list_buckets()['Buckets']:
        buckets.add(bucket['Name'])

    if bucket_name not in buckets:
        print(f"Sorry, {bucket_name} not found in your available buckets. Your choices are:", file=sys.stderr)
        print("\n".join(buckets), file=sys.stderr)
        sys.exit(2)

    for obj in s3_client.list_objects(Bucket=bucket_name)['Contents']:
        if obj['Key'] == wanted:
            if outfile:
                with open(outfile, 'wb') as out:
                    out.write(s3_client.get_object(Bucket=bucket_name, Key=wanted)['Body'].read())
            else:
                if wanted.endswith('.gz'):
                    print(zlib.decompress(s3_client.get_object(Bucket=bucket_name, Key=wanted)['Body'].read(),
                                           16 + zlib.MAX_WBITS).decode('utf-8'))
                else:
                    print(s3_client.get_object(Bucket=bucket_name, Key=wanted)['Body'].read().decode('utf-8'))
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=' ')
    parser.add_argument('-o', help='object name (including bucket)', required=True)
    parser.add_argument('-w', help='outputfile to write')
    parser.add_argument('-v', help='verbose output', action='store_true')
    args = parser.parse_args()

    stream_object(args.o, args.w)

