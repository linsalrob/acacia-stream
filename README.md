# acacia-stream

### [Rob Edwards](https://github.com/linsalrob), 20/09/23

These are examples for streaming data from `Acacia` and consuming that data in Python. 

There are a couple of simple files that test your setup and make sure you have access to Acacia, 
and I suggest you start with those.

   - `print_buckets.py` just lists the buckets you have access to. Note that AWS S3 has two data abstractions, 
using either [resources](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html) or 
[clients](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/clients.html),
and I provide examples of both here. The currently preferred abstraction is using `clients`.

   - `list_objects.py` lists all the objects in one bucket. The format is [object name, modification date, size], 
separated by tabs.

   - `list_an_object.py` provides more details about one specific object.

   - `stream_s3_file.py` shows how to stream a file and write it either as a binary or text file, or how to 
print the contents to standard output

   - `simple_streaming.py` is a simple application that streams a (text) file and counts the words in the file.
This is designed to demonstrate how you would consume a stream in Python directly.

   - `stream_from_acacia_as_file.py` is a slightly more complex streaming scenario, where you want to stream
from a file, but then consume the contents in another application that only accepts a filename as input and
doesn't accept the data. (Yes, that was my use case.) We use 
[named pipes](https://man7.org/linux/man-pages/man7/fifo.7.html) to create file-like objects that we can
use elsewhere. In this example, I just have two threads that use the data. In the next example, I pass the
named pipe to C code.

   - `human_mappy.py` if you have a human genome and a fastq file, this will use 
[minimap2](https://github.com/lh3/minimap2) to map the reads from the fastq file to the human genome and 
print the output in PAF format. If you don't understand that last sentence, this was my use case.

Good luck!


