# Assignment 5: Implementing Distributed Applications using Spark

**Due Tue Dec 3, 11:59pm**

**100 points total**

In this assignment, you will learn how to write, compile, debug and execute simple programs using Spark, both on a single stand-alone Spark instance as well as a distributed cluster with multiple workers.

You are expected to debug your implementation on a single worker; however, later in this assignment, you will run the same code on a distributed cluster on input data that doesn't fit on a single worker!

Section 1 explains how to download and install a stand-alone Spark instance.

Section 2 explains how to launch a Spark cluster on Google Cloud.

Section 3 explains how to launch the Spark shell for interactively building Spark applications.

Section 4 explains how to use Spark to launch Spark applications written in an IDE or editor.

Section 5 details a simple word count application that also serves as a warmup exercise.

Section 6 will take you through a re-implementation of the PageRank algorithm from Assignment 4 in Spark.

## 1) Setting up a stand-alone Spark Instance
Download and install Spark 2.3.4 on your machine or Myth (you can use `wget`): `wget http://mirrors.sonic.net/apache/spark/spark-2.3.4/spark-2.3.4-bin-hadoop2.7.tgz`.

Unpack the compressed TAR ball: `tar -xvzf spark-2.3.4-bin-hadoop2.7.tgz`.

Now, define the following environment variables. One way is to append the following lines into your `~/.bashrc` file (tested on Myth):

```
export SPARK_HOME="$HOME/spark-2.3.4-bin-hadoop2.7"
export SPARK_LOCAL_IP="127.0.0.1"
export PATH="$HOME/bin:$HOME/.local/bin:$SPARK_HOME/bin:$PATH"
```

You will have to edit the `$SPARK_HOME` line if you unpacked the Spark 2.3.4 tarball in a location other than `$HOME`.

After editing your `~/.bashrc`, run `source ~/.bashrc` so the new definitions take effect.

The rest of this README assume that your environment variables are set. As a quick sanity check, running `which pyspark` should return the location of the `pyspark` binary if this is done correctly.

## 2) Launching on a Distributed Cluster

We will be using GCP's `dataproc` tool to spawn a cluster equipped with Spark.
Unlike Assignment 3, where we ran into trouble spawning instances with GPUs, you
should find here that spinning up your own cluster takes a matter of seconds,
and should be able to crank out the computation you need fairly seamlessly.

### Setting up a Cluster

Use the following command (replacing `[cluster-name]` and `[project-name]`):
```
gcloud dataproc clusters create [cluster-name] --region us-central1 --subnet default --zone us-central1-c --master-machine-type n1-standard-4 --master-boot-disk-size 93 --num-workers 5 --worker-machine-type n1-standard-4 --worker-boot-disk-size 100 --image-version 1.3-deb9 --project [project-name]
```

Once that's done, you should have a cluster of 5 worker machines, and a master
machine.  This cluster is located in the `us-central1` region, but you can feel
free to launch it it whichever region makes sense for you.

You can delete your cluster using:

```
gcloud dataproc clusters delete [cluster-name] --region us-central1
```

### Submitting a Spark job to the cluster
Submitting a Spark job is easy :) We don't even need to SSH into the master;
we can just use gcloud's CLI!

The command below will run the `page_rank.py` script (the Spark driver file) using
the cluster specified by the cluster-name and cluster-region. Since the
PageRank program takes in a CLI argument of filename, we specify that after the
final `--`, this is where we list arguments to the Spark driver script. Since it is a
file we need access to, we need to make the file available to it so we include
the `--files` flag as well. 

```
gcloud dataproc jobs submit pyspark pageRank.py --cluster=[cluster-name] --region [cluster region] --files=[gs://<data-file>] -- [gs://<data-file>]
```

See https://cloud.google.com/sdk/gcloud/reference/dataproc/jobs/submit/pyspark
for more details.

You'll notice that there are `gs://` URIs, these correspond to URIs in GCP
storage.  We will provide a set of buckets for you to run on shortly, but you can
upload your own files to GCP storage using `gsutil cp` following cp semantics.
Likewise, there's a `gsutil rm` to remove files.

More details on `gsutils` are available in the docs:
https://cloud.google.com/storage/docs/gsutil.

## 3) Running the Spark shell
The easiest way to run your Spark applications is using the Spark shell, a REPL that lets you interactively compose your application. To start the Spark shell, do the following:

### Python
Assuming your environment variables (see above) are set, running `pyspark` should open a Spark shell.

As the Spark shell starts, you may see large amounts of logging information displayed on the screen, possibly including several warnings. You can ignore that output for now. The Spark shell is a full interpreter and can be used to write and execute Python code. For example:

```
>>> print("Hello!")
Hello!
```

To learn about writing Spark applications, please read through the Spark programming guide: https://spark.apache.org/docs/2.3.4/rdd-programming-guide.html.

## 4) Launching Spark Applications
The Spark shell is great for exploring a data set or experimenting with the API, but it's often best to write your Spark applications outside of the Spark interpreter using an IDE or other smart editor. Spark accepts applications written in four languages: Scala, Java, Python, and R. In this assignment, we will use Python.

### Python
For Python, assume you have the following program in a text file called `myapp.py`:

```
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf()
sc = SparkContext(conf=conf)
print "%d lines" % sc.textFile(sys.argv[1]).count()
```

This short application opens the file path given as the first argument from the local working directory and prints the number of lines in it. To run this application, run the following:

```
spark-submit path/to/myapp.py path/to/file
```


As Spark starts, you may see large amounts of logging information displayed on the screen, possibly including several warnings. You can ignore that output for now. Regardless, near the bottom of the output you will see the output from the application. Executing the application this way causes it to be run single-threaded. To run the application with 4 threads, launch it as:

```
spark-submit --master ’local[4]’ path/to/myapp.py path/to/file
```

You can replace the “4” with any number. To use as many threads as are available on your system, launch the application as:
```
spark-submit --master ’local[*]’ path/to/myapp.py path/to/file
```

## 5) Word Count in Spark (30 Points)
The typical "Hello, world!" app for Spark applications is known as word count. The map/reduce model is particularly well suited to applications like counting words in a document.

All operations in Spark operate on data structures called RDDs, Resilient Distributed Datasets. An RDD is nothing more than a collection of objects. If you read a file into an RDD, each line will become an object (a string, actually) in the collection that is the RDD. If you ask Spark to count the number of elements in the RDD, it will tell you how many lines are in the file. If an RDD contains only two-element tuples, the RDD is known as a "pair RDD" and offers some additional functionality. The first element of each tuple is treated as a key, and the second element as a value. Note that all RDDs are immutable, and any operations that would mutate an RDD will instead create a new RDD.

We have provided starter code in `word_count.py` that loads the input file into a RDD. You are responsible for writing the rest of the application. Your application must return a list of the 10 most frequently occurring words, sorted in descending order of count.

You can use `re.split()` function with the regex `[^\w]+` to split the input text into words.

We have provided a local dataset called `pg100.txt` to experiment with; for distributed runs, we provide a large dataset at `gs://cs149-asst5/word_count/wiki`.

Here is an example of how to run the code:

```
spark-submit --master 'local[*]' word_count.py data/word_count/pg100.txt
```

For `pg100.txt`, the correct output is,

```
[('', 197060), ('the', 23455), ('I', 22225), ('and', 18715), ('to', 16433), ('of', 15830), ('a', 12851), ('you', 12236), ('my', 10840), ('in', 10074)]
```

## 6) Page Rank in Spark (70 Points)
In this problem, you will learn how to implement the PageRank algorithm in Spark. You can start experimenting with small randomly generated graphs (assume graph has no dead-ends), provided at `data/page_rank/small.txt` and `data/page_rank/full.txt`. There are 100 nodes (`n = 100`) in the small graph and 1000 nodes (`n = 1000`) in the full graph, and `m = 8192` edges, 1000 of which form a directed cycle (through all the nodes) which ensures that the graph is connected. It is easy to see that the existence of such a cycle ensures that there are no dead ends in the graph. There may be multiple directed edges between a pair of nodes, and your solution should treat them as the same edge. The first column in `data/page_rank/full.txt` refers to the source node, and the second column refers to the destination node.

Implementation hint: You may choose to store the PageRank vector `r` either in memory or as an RDD. Only the matrix of links is too large to store in memory.

Let the matrix `M` be an `(n x n)` matrix such that for any `i` and `j` between `[1, n]`, `M_{ji} = 1/deg(i)` if there exists a directed edge from `i` to `j`, and 0 otherwise (Here `M_{ji}` is the `j`'th row and `i`'th column entry of `M`). Here, `deg(i)` is the number of outgoing edges from node `i` in the graph. If there are multiple edges in the same direction between two nodes, treat them as a single edge.

By the definition of PageRank, assuming `1 − β` to be the teleport probability, and denoting the PageRank vector by the column vector `r`, we have the following equation:
```
r = 1[(1 - β)/n] + β*M*r,
```

where `1[...]` is the `(n × 1)` vector with all entries equal to `(1 - β)/n`, and `M*r` computes the matrix-vector multiplication between the matrix of links `M`, and the page rank vector `r`.

Based on this equation, the iterative procedure to compute PageRank works as follows:
```
1. Initialize r = 1[1/n]
2. For i from 1 to k, iterate: r = 1[(1 - β)/n] + β*M*r
```

You can not store the matrix M in local memory, but you can store the vector r locally. You must figure out a way to perform the matrix-vector multiplication as an RDD operation using Spark primitives. We recommend that you also use NumPy in other parts of your code to perform vector additions, dot products, etc.

Run the aforementioned iterative process in Spark for 100 iterations (assuming `β = 0.8`) and obtain the PageRank vector r. The matrix M can be large and should be processed as an RDD in your solution. Compute the top 5 node IDs with the highest PageRank scores.

For a sanity check, we have provided a smaller dataset (`small.txt`). In that dataset, the top node has ID 53 with value approximately 0.0357312 after 100 iterations (you can use this value to help debug). **We will be grading you on your results for full.txt.** We give you a file pageRank.py to write your code in, with basic starter code that starts your Spark context and reads in the input text file as an RDD. You will also be reporting the total time it took your program to run.
The starter code already wraps the code you will write with timing code (report this number in seconds).
Our reference solution takes less than 10 seconds for 100 iterations on `full.txt` on Myth when run with the following command:

```
spark-submit --master 'local[*]' page_rank.py data/page_rank/full.txt
```

For `small.txt`, the correct output is,

```
5 highest: [(0.0357312022326716, 53), (0.03417090697259137, 14), (0.03363008718974388, 40), (0.030005979479788617, 1), (0.029720144201405382, 27)]
```

For `full.txt`, the correct output is,

```
5 highest: [(0.002020291181518219, 263), (0.0019433415714531497, 537), (0.0019254478071662631, 965), (0.001852634016241731, 243), (0.0018273721700645144, 285)]
```

We expect you to use Spark for all operations on the data (including performing the matrix-vector multiply). You can use NumPy or regular python for computing dot products and other arithmetic, but any other data computation should leverage Spark.

We have provided various input graphs to test your implementation at `gs://cs149-asst5/page_rank`, including the same graphs you used in Assignment 4 at `gs://cs149-asst5/page_rank/asst4_graphs`.

## Grading

Points in this assignment will solely be based on the correctness of your implementation on the provided input files, and the quality of your writeup,
- 20 points: correctness of Word Count
- 10 points: writeup for Word Count
- 50 points: correctness of Page Rank
- 20 points: writeup for Page Rank

In your writeup, please describe at a high level your implementation strategy. For PageRank, also compare at a high level the single-machine performance of Spark compared to your assignment 4 submission for the same graphs (on Myth). Please use all cores on the Myth machine. Note the differences you see (which implementation is faster), and outline some plausible reasons why these differences might exist.

The graph available at `gs://cs149-asst5/page_rank/webgraph` is for a bonus 20 points. Please indicate in your writeup how you modified your PageRank implementation to account for this large a graph.

## Hand-in Instructions
Please submit your work using Gradescope.

- Please submit your writeup as the file `writeup.pdf`.
- Please submit your code under the folder code with the files `word_count.py` and `page_rank.py`.
