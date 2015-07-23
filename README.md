# quince
Scalable genomics variant store and analytics

## Pre-requisites and tool installation

You will need a Hadoop cluster. These instructions assume CDH 5.4.2.

We'll use the Kite command line tools. Checkout, build and install this branch,
which has a couple of fixes that we rely on (a [bug in the Hive partitioning]
(https://issues.cloudera.org/browse/KITE-1028), and [sorting within a partition]
(https://issues.cloudera.org/browse/KITE-1032)):
[https://github.com/tomwhite/kite/tree/KITE-1032-sort-in-partition](https://github.com/tomwhite/kite/tree/KITE-1032-sort-in-partition).

Unpack the Kite tarball from the _kite-tools-parent/kite-tools-cdh5/target_ directory and
 put the top-level unpacked directory on your `PATH` (along with a couple of other
 configuration environment variables):

```bash
export PATH=~/sw/kite-tools-cdh5-1.1.1-SNAPSHOT/bin:$PATH
export HADOOP_CONF_DIR=/etc/hadoop/conf
export HIVE_CONF_DIR=/etc/hive/conf
```

## Getting data

We'll start with a test file that is already in GA4GH Avro format. This was generated
using [hpg-bigdata](https://github.com/opencb/hpg-bigdata).

First convert it to Avro with no compression (deflate is not used by the Kite tools
we'll be using later):

```bash
avro-tools recodec ~/data/isaac2.vcf.gz.avro.deflate ~/data/isaac2.vcf.gz.avro
```

Then copy it to HDFS:

```bash
hadoop fs -mkdir -p datasets/variants_avro
hadoop fs -put ~/data/isaac2.vcf.gz.avro datasets/variants_avro
```

## Load variants tool

This flattens and partitions in one step:

```bash
hadoop jar target/quince-0.0.1-SNAPSHOT-job.jar \
  LoadVariantsTool \
  -D mapreduce.map.java.opts="-Djava.net.preferIPv4Stack=true -Xmx3g" \
  -D mapreduce.reduce.java.opts="-Djava.net.preferIPv4Stack=true -Xmx3g" \
  -D mapreduce.map.memory.mb=4096 \
  -D mapreduce.reduce.memory.mb=4096 \
  ga4gh-variants-partition-strategy \
  sample1 \
  datasets/variants_avro \
  dataset:hdfs:datasets/variants_flat_locuspart
```

```bash
kite-dataset create dataset:hive:/user/tom/datasets/variants_flat_locuspart
```

Try querying the table in Hive:

```bash
hive -e 'select count(*) from datasets.variants_flat_locuspart'
```

Or Impala:

```bash
impala-shell -q 'invalidate metadata'
impala-shell -q 'compute stats datasets.variants_flat_locuspart'
impala-shell -q 'select count(*) from datasets.variants_flat_locuspart'
impala-shell -q 'select count(*) from datasets.variants_flat_locuspart where referencename="chr1"'
```
