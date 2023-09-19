"""
Map a fastx file against the human genome that is on acia

This is based on https://github.com/lh3/minimap2/blob/master/python/minimap2.py  (among other things!)

Basic approach:
 - we find and stream the human genome from acacia in one thread
 - we read that in another thread and process the data

Human genome target:
    At the moment you can either use:
     - databases/human/chr1.fna.gz
     - databases/human/GCA_000001405.15_GRCh38_no_alt_plus_hs38d1_analysis_set.fna.gz
"""

import os
import io
import sys
import argparse
import boto3
import mappy as mp
from multiprocessing import Process

__author__ = 'Rob Edwards'


def get_human_genome(location, verbose=False):
    """
    Get the human genome
    """

    bucket_name, wanted = location.split('/', 1)
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
            if verbose:
                print(f"Streaming {wanted}", file=sys.stderr)
            return s3_client.get_object(Bucket=bucket_name, Key=wanted)['Body']


def write_the_genome(human_genome, fifo, verbose=False):
    """
    A function to write the genome
    """
    stream = get_human_genome(human_genome, verbose=verbose)
    #fd = os.open(fifo, os.O_RDWR | os.O_NONBLOCK)
    fd = os.open(fifo, os.O_RDWR)
    if verbose:
        print(f"File descriptor {fd} for {fifo}. From child. Child PID: {os.getpid()} Parent PID: {os.getppid()}", file=sys.stderr)
    with io.FileIO(fd, 'wb') as f:
        f.write(stream.read())


def read_genome(fifo, reads, preset, min_cnt=None, min_sc=None, k=None, w=None, bw=None, out_cs=False, verbose=False):
    """
    Read the genome from the fifo and return the alignment object
    """
    if verbose:
        print(f"Aligning my PID: {os.getpid()} Parent PD {os.getppid()}", file=sys.stderr)
    print(f"Aligner: {fifo}, preset={preset}, min_cnt={min_cnt}, min_chain_score={min_sc}, k={k}, w={w}, bw={bw}")
    a = mp.Aligner(fifo, preset=preset, min_cnt=min_cnt, min_chain_score=min_sc, k=k, w=w, bw=bw)
    if not a:
        raise Exception("ERROR: failed to load/build index file for the human genome")
    for name, seq, qual in mp.fastx_read(reads):  # read one sequence
        # print(name)
        for h in a.map(seq, cs=out_cs):  # traverse hits
            print('{}\t{}\t{}'.format(name, len(seq), h))


def read_align(genome, reads, preset, min_cnt=None, min_sc=None, k=None, w=None, bw=None, out_cs=False, verbose=False):

    # here we create a fifo object that we can pass to the mp.Aligner
    fifo_filename = f'/home/edwa0468/scratch/tmp/tmp.{os.getpid()}.fna.gz'
    if os.path.exists(fifo_filename):
        print(f"ERROR: {fifo_filename} exists. Not overwriting", file=sys.stderr)
        sys.exit(2)
    os.mkfifo(fifo_filename)
    if verbose:
        print(f"Our FIFO is at {fifo_filename}", file=sys.stderr)
    
    # start the process to read the genome from the pipe
    readprocess = Process(target=read_genome, args=(fifo_filename, reads, preset, min_cnt, min_sc, k, w, bw, out_cs, verbose,))
    readprocess.start()

    # start the process to write the genome to the pipe
    writeprocess = Process(target=write_the_genome, args=(genome, fifo_filename, verbose,))
    writeprocess.start()
    writeprocess.join()
    

    # wait until reading is done
    readprocess.join()

    os.unlink(fifo_filename)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Usage: human_mappy.py [options] <ref.fa>|<ref.mmi> <query.fq>')
    parser.add_argument('-f', help='input file', required=True)
    parser.add_argument('-g', help='human genome', required=True)
    parser.add_argument('-x', help='preset: sr, map-pb, map-ont, asm5, asm10 or splice', default='sr')
    parser.add_argument('-n', help='mininum number of minimizers', type=int)
    parser.add_argument('-m', help='mininum chaining score', type=int)
    parser.add_argument('-k', help='k-mer length', type=int)
    parser.add_argument('-w', help='minimizer window length', type=int)
    parser.add_argument('-r', help='band width', type=int)
    parser.add_argument('-c', help='output the cs tag', action='store_true')
    parser.add_argument('-v', help='verbose output', action='store_true')
    args = parser.parse_args()

    read_align(genome=args.g, reads=args.f, preset=args.x, min_cnt=args.n, min_sc=args.m, k=args.k, w=args.w,
               bw=args.r, out_cs=args.c, verbose=args.v)
