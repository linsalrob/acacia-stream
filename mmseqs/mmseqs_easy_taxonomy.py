"""
A wrapper to run mmseqs_easy_taxonomy using named pipes
"""

import os
import io
import subprocess
import sys
import argparse
from multiprocessing import Process

__author__ = 'Rob Edwards'

import boto3
from botocore.client import BaseClient


def get_s3client()->BaseClient:
    """
    Connect to S3 and get a client
    :return:
    """

    # initiate our s3 client
    session = boto3.session.Session()
    s3_client: BaseClient = session.client(
        service_name='s3',
        endpoint_url='https://projects.pawsey.org.au',
    )
    return s3_client

def create_a_connection(object:str, namedpipe:str, s3_client:BaseClient, verbose=False):
    """
    Create a named pipe connection
    :param object: the object to open
    :param namedpipe: the pipe to connect to
    :param s3_client: the connection to s3
    :param verbose:
    """

    # we need the name of the bucket
    bucket_name, wanted = object.split('/', 1)
    #if verbose:
    print(f"Getting {wanted} from {bucket_name}", file=sys.stderr)
    stream = s3_client.get_object(Bucket=bucket_name, Key=wanted)['Body']

    # open the fio object
    fd = os.open(namedpipe, os.O_RDWR)
    if not fd:
        print(f"Did not connect to {namedpipe}", file=sys.stderr)
        sys.exit(2)

    if verbose:
        print(f"File descriptor {fd} for {namedpipe}. From child. Child PID: {os.getpid()} Parent PID: {os.getppid()}", file=sys.stderr)
    with io.FileIO(fd, 'w') as f:
        f.write(stream.read())

def create_connections(bucket:str, database:str, datadir:str, s3_client:BaseClient, verbose:bool=False):
    """
    Create the connections to the bucket in datadir. The bucket should be the location with the
    database files
    :param bucket: the path to the data on acacia, eg. databases/mmseqs/UniRef50.20230126
    :param database: the name of the database, eg. UniRef50
    :param datadir: the datadirectory to write the files
    :param s3_client: the connection to s3
    :param verbose: more output
    :return: a dict of process names
    """

    appendices = ['', '.dbtype', '.index', '.lookup', '.source', '.version', '_h', '_h.dbtype', '_h.index', '_mapping', '_taxonomy']
    processes = {}

    for a in appendices:
        thisname = f"{database}{a}"
        fifo_name = f"{datadir}/{thisname}"
        os.mkfifo(fifo_name)
        processes[thisname] = {
            'name' : thisname,
            'process': Process(target=create_a_connection, args=(f"{bucket}/{thisname}", fifo_name, s3_client, verbose,)),
            'named_pipe': f"{datadir}/{thisname}"
        }
        if verbose:
            print(f"Starting {thisname}", file=sys.stderr)
        processes[thisname]['process'].start()

    return processes


def run_mmseqs(datadir: str, database: str, fasta: str, outputdir: str, verbose=False):
    """
    Launch and run mmseqs
    :param datadir: the directory with the mmseqs named pipes
    :param database: the name of the database
    :param outputdir: the directory for the results
    :param fasta: the fasta file
    :param verbose: more output
    :return:
    """

    # mmseqs easy-taxonomy $FASTA $BGFS/$DB/$DB $BGFS/output/$TMPOUTPUT $(mktemp -d -p $BGFS) --start-sens 1 --sens-steps 3 -s 7 --threads 32
    mmseqs_command = ['mmseqs', 'easy-taxonomy', fasta, f"{datadir}/{database}", outputdir, "/home/edwa0468/scratch/tmp/", '--threads', '8']
    if verbose:
        print(f'Forking mmseqs in child {os.getpid()}', file=sys.stderr)
    subprocess.run(mmseqs_command)

def run_search(bucket: str, database: str, datadir: str, fasta: str, outputdir: str, s3_client: BaseClient, verbose=False):
    """
    Run the search
    :param bucket: where the data resides
    :param database: the name of the database
    :param datadir: the directory to put the named pipes in
    :param fasta: the fasta file to compare
    :param s3_client: the S3 client
    :param verbose: more output
    :return:
    """

    if verbose:
        print("Starting database connections", file=sys.stderr)
    os.makedirs(datadir, exist_ok=True)
    connections = create_connections(bucket, database, datadir, s3_client, verbose)


    if verbose:
        print("Starting mmseqs", file=sys.stderr)
    mmseqs = Process(target=run_mmseqs, args=(datadir, database, fasta, outputdir, verbose,))
    mmseqs.start()
    mmseqs.join()

    print("*************WE GOT TO THE END*************")
    print("*************WE GOT TO THE END*************", file=sys.stderr)
    # for name in connections:
    #     os.unlink(connections[name]['named_pipe'])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=' ')
    parser.add_argument('-f', help='fasta file', required=True)
    parser.add_argument('-o', help='output directory', required=True)
    parser.add_argument('-m', help='mmseqs database', default="UniRef50")
    parser.add_argument('-b', help='bucket name on acacia', default="databases/mmseqs/UniRef50.20230126")
    parser.add_argument('-d', help='datadirectory for connections', default='uniref')

    parser.add_argument('-v', help='verbose output', action='store_true')
    args = parser.parse_args()

    s3_client = get_s3client()
    run_search(args.b, args.m, args.d, args.f, args.o, s3_client, args.v)
