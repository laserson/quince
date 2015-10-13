# quince [![Build Status](https://travis-ci.org/cloudera/quince.svg?branch=master)](https://travis-ci.org/cloudera/quince)
Scalable genomics variant store and analytics

## Pre-requisites

You will need a Hadoop cluster. These instructions assume CDH 5.4.x.

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

Note that the `--sample-group` argument is used to label the samples being loaded. There 
is no fixed format for this argument; it could be a numeric label, or a date, for 
example. You can see the data in HDFS as follows:

```bash
hadoop fs -ls -R datasets/variants_flat_locuspart
```

The next step is to register the data in Hive, so we can use SQL to query it. Run the 
following commands to create the Hive table and update its partitions (change the host 
name to the Hadoop namenode on your cluster):

```bash
hive -f sql/create-variants.sql --hiveconf namenode='bottou01.sjc.cloudera.com'
hive -f sql/update-variants-partitions.sql
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
impala-shell -q 'select count(*) from datasets.variants_flat_locuspart where referencename="1"'
```

## Loading new samples

Samples are loaded in batches. When you have a new group of samples to load, then run the
`LoadVariantsTool` again, with a new `--sample-group`.

For example:

```bash
hadoop jar target/quince-0.0.1-SNAPSHOT-job.jar \
  com.cloudera.science.quince.LoadVariantsTool \
  -D mapreduce.map.java.opts="-Djava.net.preferIPv4Stack=true -Xmx3g" \
  -D mapreduce.reduce.java.opts="-Djava.net.preferIPv4Stack=true -Xmx3g" \
  -D mapreduce.map.memory.mb=4096 \
  -D mapreduce.reduce.memory.mb=4096 \
  --sample-group sample2 \
  datasets/variants_vcf/small.vcf \
  datasets/variants_flat_locuspart
```

If you want to replace an existing group of samples, then you need to specify the 
`--overwrite` argument, which will delete the existing data for the sample group, 
before writing the new data.

To update the partitions in the Hive metastore, run the following command:

```bash
hive -f sql/update-variants-partitions.sql
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
impala-shell -q 'compute stats datasets.variants_flat_locuspart'
impala-shell -q 'show partitions datasets.variants_flat_locuspart'
```

### Loading only a subset of the samples in VCF files

By default all the samples from the VCF files are loaded. If you want to only include 
some of the samples, then use the `--samples` argument with a comma-separated list of 
sample IDs. E.g. 

```bash
hadoop jar target/quince-0.0.1-SNAPSHOT-job.jar \
  com.cloudera.science.quince.LoadVariantsTool \
...
--sample-group sample3
--samples NA12878,NA12891,NA12892
...
```

## Deleting data

You can remove all the data with the following commands. _Note that the data will be 
deleted permanently!_

```bash
hive -e 'drop table datasets.variants_flat_locuspart'
hadoop fs -rm -r datasets/variants_flat_locuspart
```

## Loading from a GA4GH Avro file

As mentioned above, you can load variants data in GA4GH Avro format that was 
generated using [hpg-bigdata](https://github.com/opencb/hpg-bigdata).

First convert the file to Avro with no compression (rather than deflate):

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

## Using Kite datasets

You can also use the Kite command line tool to manage datasets in Hive. The 
following command will infer the dataset's schema and partitioning strategy from files 
in HDFS and create the necessary tables in Hive. Note that this may take
 a while to run if there are a large number of partitions.

```bash
kite-dataset create dataset:hive:/user/tom/datasets/variants_flat_locuspart
```

## Loading the 1000 Genomes VCF data
 
There are scripts in [Eggo](https://github.com/bigdatagenomics/eggo) to do this, but they are not quite complete, so here are 
some instructions to do this manually.

The first step is to download the data and decompress it into HDFS:

```bash
./scripts/download_1000_genomes.sh
```

We are going to load the samples in batches, so use the following to find all the 
sample IDs, printing 100 samples per line:

```bash
hadoop jar target/quince-0.0.1-SNAPSHOT-job.jar \
  com.cloudera.science.quince.PrintSamplesTool \
  --samples-per-line 100 \
  vcf-1000genomes
```

Now we can load the first 100 samples by running this command:

```bash
hadoop jar target/quince-0.0.1-SNAPSHOT-job.jar \
  com.cloudera.science.quince.LoadVariantsTool \
  -D mapreduce.map.java.opts="-Djava.net.preferIPv4Stack=true -Xmx3g" \
  -D mapreduce.reduce.java.opts="-Djava.net.preferIPv4Stack=true -Xmx3g" \
  -D mapreduce.map.memory.mb=4096 \
  -D mapreduce.reduce.memory.mb=4096 \
  --sample-group 1000genomes \
  --num-reducers 3100 \
  --samples HG00096,HG00097,HG00099,HG00100,HG00101,HG00102,HG00103,HG00105,HG00106,HG00107,HG00108,HG00109,HG00110,HG00111,HG00112,HG00113,HG00114,HG00115,HG00116,HG00117,HG00118,HG00119,HG00120,HG00121,HG00122,HG00123,HG00125,HG00126,HG00127,HG00128,HG00129,HG00130,HG00131,HG00132,HG00133,HG00136,HG00137,HG00138,HG00139,HG00140,HG00141,HG00142,HG00143,HG00145,HG00146,HG00148,HG00149,HG00150,HG00151,HG00154,HG00155,HG00157,HG00158,HG00159,HG00160,HG00171,HG00173,HG00174,HG00176,HG00177,HG00178,HG00179,HG00180,HG00181,HG00182,HG00183,HG00185,HG00186,HG00187,HG00188,HG00189,HG00190,HG00231,HG00232,HG00233,HG00234,HG00235,HG00236,HG00237,HG00238,HG00239,HG00240,HG00242,HG00243,HG00244,HG00245,HG00246,HG00250,HG00251,HG00252,HG00253,HG00254,HG00255,HG00256,HG00257,HG00258,HG00259,HG00260,HG00261,HG00262 \
  vcf-1000genomes \
  datasets/variants_flat_locuspart
```

Register the data in the Hive metastore, and update Impala stats:

```bash
hive -f sql/create-variants.sql --hiveconf namenode='bottou01.sjc.cloudera.com'
hive -f sql/update-variants-partitions.sql
impala-shell -q 'invalidate metadata'
impala-shell -q 'compute stats datasets.variants_flat_locuspart'
```

Run some queries using `impala-shell`:

```sql

use datasets; 

-- total number of records
select count(*) from variants_flat_locuspart;

-- total number of samples
select count(distinct(callsetid)) from variants_flat_locuspart;   

-- number of records for a given sample
select count(*) from variants_flat_locuspart where callsetid='HG00262';

-- number of SNPs
select count(*) from variants_flat_locuspart
where length(referencebases) = 1
and length(alternatebases_1) = 1
and alternatebases_2 is null;

-- number of monomorphic references
select count(*) from variants_flat_locuspart
where alternatebases_1 is null
and alternatebases_2 is null;

-- find all variants with frequencies in a small region
select start, referencebases, alternatebases_1, genotype_1, genotype_2, count(1) from variants_flat_locuspart
where referencename='1' and start between 75068210 and 75069210
and chr='1' and pos between cast(floor(75068210 / 1000000.) AS INT) * 1000000 and cast(floor(75069210 / 1000000.) AS INT) * 1000000
group by start, referencebases, alternatebases_1, genotype_1, genotype_2
order by start; 
```