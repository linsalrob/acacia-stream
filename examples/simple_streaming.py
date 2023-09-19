"""
This is a simple example to stream a object from acacia and have a fifo object that we can open in another program.
For this example, we just open it in the consumer function.

Note that the stream in this case is text, but if you have binary (e.g. gzip) you want to open the files
with the `b` flag.

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



def stream_from_accia(object:str, fifo:str, verbose:bool=False):
    """
    Stream an object from acacia
    :param object: The thing on acacia to stream
    :param fifo: the name of the fifo object to write to
    :param verbose: more output
    """

    # we need the name of the bucket
    bucket_name, wanted = object.split('/', 1)

    # initiate our s3 client
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://projects.pawsey.org.au',
    )

    stream = s3_client.get_object(Bucket=bucket_name, Key=wanted)['Body']

    # open the fio object
    fd = os.open(fifo, os.O_RDWR)
    if verbose:
        print(f"File descriptor {fd} for {fifo}. From child. Child PID: {os.getpid()} Parent PID: {os.getppid()}", file=sys.stderr)
    with io.FileIO(fd, 'w') as f:
        f.write(stream.read())

def consume_file(fifo:str, verbose:bool=False):
    """
    Consume the file from acacia
    :param fifo:
    :param verbose: more output
    """

    count = {}
    if verbose:
        print(f"Opening consumer in child PID {os.getpid()} from parent PID {os.getppid()}", file=sys.stderr)
    with open(fifo, 'r') as f:
        for l in f:
            for x in l.strip().split():
                count[x]=count.get(x, 0)+1
    for c in count:
        print(f"{c}\t{count[c]}")


def main(objectname:str, verbose=False):
    """
    Run the producer and consumers
    :param objectname: the name of the object to stream
    :param verbose: more output
    """

    # here we create a fifo object that we can use
    fifo_filename = f'/tmp/tmp.{os.getpid()}.fna.gz'
    if os.path.exists(fifo_filename):
        print(f"ERROR: {fifo_filename} exists. Not overwriting", file=sys.stderr)
        sys.exit(2)
    os.mkfifo(fifo_filename)
    if verbose:
        print(f"Our FIFO is at {fifo_filename}", file=sys.stderr)

    # start the process to read the genome from the pipe
    readprocess = Process(target=consume_file, args=(fifo_filename, verbose,))
    readprocess.start()

    # start the process to write the genome to the pipe
    writeprocess = Process(target=stream_from_accia, args=(objectname, fifo_filename, verbose,))
    writeprocess.start()
    writeprocess.join()

    # wait until reading is done
    readprocess.join()

    os.unlink(fifo_filename)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=' ')
    parser.add_argument('-o', help='object name on acacia', required=True)
    parser.add_argument('-v', help='verbose output', action='store_true')
    args = parser.parse_args()

    main(args.o, args.v)