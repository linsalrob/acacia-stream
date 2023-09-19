"""
Map a fastx file against the human genome that is on acia

This is based on https://github.com/lh3/minimap2/blob/master/python/minimap2.py  (among other things!)
"""

import os
import sys
import argparse
import zlib
import boto3
import mappy as mp

__author__ = 'Rob Edwards'


def get_human_genome(location='databases/human/GCA_000001405.15_GRCh38_no_alt_plus_hs38d1_analysis_set.fna.gz', verbose=False):
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
            """
            # We're going to stream this as a binary file, so no need to decompress it!
            if wanted.endswith('.gz'):
                #return zlib.decompress(s3_client.get_object(Bucket=bucket_name, Key=wanted)['Body'].read(), 16 + zlib.MAX_WBITS)
            else:
                return s3_client.get_object(Bucket=bucket_name, Key=wanted)['Body'].read()
            """

def read_align(reads, preset, min_cnt = None, min_sc = None, k = None, w = None, bw = None, out_cs = False, verbose=False):


    stream = get_human_genome(verbose=verbose)
    if verbose:
        print("Streaming the file", file=sys.stderr)

    r = stream.read()
    print("Read")

    # here we create a fifo object that we can pass to the mp.Aligner
    FIFO_PATH = f'/tmp/tmp.{os.getpid()}.fna.gz'
    if os.path.exists(FIFO_PATH):
        print(f"ERROR: {FIFO_PATH} exists. Not overwriting", file=sys.stderr)
        sys.exit(2)
    os.mkfifo(FIFO_PATH)
    with open(FIFO_PATH, 'wb') as out_fifo:
        print("Opened", file=sys.stderr)
        out_fifo.write(r)
        print("Wrote1", file=sys.stderr)

    out_fifo.flush()
    print("Wrote")

    if verbose:
        print("OPening the aligner", file=sys.stderr)

    a = mp.Aligner(FIFO_PATH, preset=preset, min_cnt=min_cnt, min_chain_score=min_sc, k=k, w=w, bw=bw)
    if not a:
        raise Exception("ERROR: failed to load/build index file for the human genome")
    for name, seq, qual in mp.fastx_read(reads): # read one sequence
        for h in a.map(seq, cs=out_cs): # traverse hits
            print('{}\t{}\t{}'.format(name, len(seq), h))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Usage: human_mappy.py [options] <ref.fa>|<ref.mmi> <query.fq>')
    parser.add_argument('-f', help='input file', required=True)
    parser.add_argument('-x', help='preset: sr, map-pb, map-ont, asm5, asm10 or splice', default='sr')
    parser.add_argument('-n', help='mininum number of minimizers', type=int)
    parser.add_argument('-m', help='mininum chaining score', type=int)
    parser.add_argument('-k', help='k-mer length', type=int)
    parser.add_argument('-w', help='minimizer window length', type=int)
    parser.add_argument('-r', help='band width', type=int)
    parser.add_argument('-c', help='output the cs tag', action='store_true')
    parser.add_argument('-v', help='verbose output', action='store_true')
    args = parser.parse_args()

    read_align(reads=args.f, preset=args.x, min_cnt=args.n, min_sc=args.m, k=args.k, w=args.w, bw=args.r, out_cs=args.c, verbose=args.v)

