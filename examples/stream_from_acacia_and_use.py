"""
This is a simple example to stream a object from acacia and do something with it on setonix

In this example, we assume you know the object and its location is correct. See human_mappy.py for an
example to error check the stream

We also have a consumer process, which just counts words in the file
"""

import io
import os
import sys
import argparse
import boto3
from multiprocessing import Process
__author__ = 'Rob Edwards'


def main(objectname:str, verbose=False):

    """
    Stream an object from acacia
    :param objectname: The thing on acacia to stream
    :param fifo: the name of the fifo object to write to
    :param verbose: more output
    """

    # we need the name of the bucket
    bucket_name, wanted = objectname.split('/', 1)

    # initiate our s3 client
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://projects.pawsey.org.au',
    )

    stream = s3_client.get_object(Bucket=bucket_name, Key=wanted)['Body'].read().decode('utf-8')


    count = {}
    for x in stream.strip().split(" "):
        count[x]=count.get(x, 0)+1

    for c in count:
        print(f"{c}\t{count[c]}")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=' ')
    parser.add_argument('-o', help='object name on acacia', required=True)
    parser.add_argument('-v', help='verbose output', action='store_true')
    args = parser.parse_args()

    main(args.o, args.v)
