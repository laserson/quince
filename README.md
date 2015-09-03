# quince [![Build Status](https://travis-ci.org/cloudera/quince.svg?branch=master)](https://travis-ci.org/cloudera/quince)
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

## Building Quince

Build Quince by typing

```bash
mvn package
```

## Getting data

Quince can load variant data from VCF files or from files in GA4GH Avro format generated
using [hpg-bigdata](https://github.com/opencb/hpg-bigdata).

You can load your own VCF data into a _datasets/variants_vcf_ directory on HDFS, or you
 can use the test data supplied in this repository, as follows:

```bash
hadoop fs -mkdir datasets
hadoop fs -put datasets/variants_vcf datasets/variants_vcf
```

## Load variants tool

The `LoadVariantsTool` loads the variants data into HDFS and stores it in Parquet 
format, flattened to make it suitable for querying by Impala. It also partitions the 
data so that it can be efficiently queried by position or by sample.

Run it as follows:

```bash
hadoop jar target/quince-0.0.1-SNAPSHOT-job.jar \
  com.cloudera.science.quince.LoadVariantsTool \
  -D mapreduce.map.java.opts="-Djava.net.preferIPv4Stack=true -Xmx3g" \
  -D mapreduce.reduce.java.opts="-Djava.net.preferIPv4Stack=true -Xmx3g" \
  -D mapreduce.map.memory.mb=4096 \
  -D mapreduce.reduce.memory.mb=4096 \
  --sample-group sample1 \
  datasets/variants_vcf/small.vcf \
  datasets/variants_flat_locuspart
```

Note that the `--sample-group` argument is used label the samples being loaded. There 
is no fixed format for this argument; it could be a numeric label, or a date, for example.

The next step is to register the data in Hive, so we can use SQL to query it. The 
simplest way to do this is by using the Kite command line tool. Note that this may take
 a while to run if there are a large number of partitions.

```bash
kite-dataset create dataset:hive:/user/tom/datasets/variants_flat_locuspart
```

You can see the table definition by running:

```bash
hive -e 'show create table datasets.variants_flat_locuspart'
```

To test that the data is registered in the Hive metastore, try running this query:

```bash
hive -e 'select count(*) from datasets.variants_flat_locuspart'
```

You can do the same thing with Impala as follows:

```bash
impala-shell -q 'invalidate metadata'
impala-shell -q 'compute stats datasets.variants_flat_locuspart'
impala-shell -q 'select count(*) from datasets.variants_flat_locuspart'
impala-shell -q 'select count(*) from datasets.variants_flat_locuspart where referencename="chr1"'
```

## Loading new samples

Samples are loaded in batches. When you have a new group of samples to load, then run the
`LoadVariantsTool` again, with a new `--sample-group`. You also need to specify the
`--overwrite` argument; however since the data is written into a new sample group 
partition existing data won't actually be overwritten. (If you use `--overwrite` and an 
existing sample group ID it will overwrite the old data.)

For example:

```bash
hadoop jar target/quince-0.0.1-SNAPSHOT-job.jar \
  com.cloudera.science.quince.LoadVariantsTool \
  -D mapreduce.map.java.opts="-Djava.net.preferIPv4Stack=true -Xmx3g" \
  -D mapreduce.reduce.java.opts="-Djava.net.preferIPv4Stack=true -Xmx3g" \
  -D mapreduce.map.memory.mb=4096 \
  -D mapreduce.reduce.memory.mb=4096 \
  --sample-group sample2 \
  --overwrite \
  datasets/variants_vcf/small.vcf \
  datasets/variants_flat_locuspart
```

To update the partitions in the Hive metastore, run the following command:

```bash
hive -e 'msck repair table datasets.variants_flat_locuspart;'
```

You can see the partitions for the new sample group with:

```bash
hive -e "show partitions datasets.variants_flat_locuspart partition(sample_group='sample2');"
```

Or all partitions with:

```bash
hive -e "show partitions datasets.variants_flat_locuspart;"
```

For Impala, update the partition information with:

```bash
impala-shell -q 'refresh datasets.variants_flat_locuspart'
impala-shell -q 'show partitions datasets.variants_flat_locuspart'
```

## Loading from a GA4GH Avro file

As mentioned above, you can load variants data in GA4GH Avro format that was 
generated using [hpg-bigdata](https://github.com/opencb/hpg-bigdata).

First convert the file to Avro with no compression (deflate is not used by the Kite 
tools we'll be using later):

```bash
avro-tools recodec ~/data/isaac2.vcf.gz.avro.deflate ~/data/isaac2.vcf.gz.avro
```

Then copy it to HDFS:

```bash
hadoop fs -mkdir -p datasets/variants_avro
hadoop fs -put ~/data/isaac2.vcf.gz.avro datasets/variants_avro
```

You can then use the `LoadVariantsTool` command from above, changing the input to 
`datasets/variants_avro`.