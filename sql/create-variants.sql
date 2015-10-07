-- Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
--
-- Cloudera, Inc. licenses this file to you under the Apache License,
-- Version 2.0 (the "License"). You may not use this file except in
-- compliance with the License. You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
-- CONDITIONS OF ANY KIND, either express or implied. See the License for
-- the specific language governing permissions and limitations under the
-- License.

CREATE DATABASE IF NOT EXISTS datasets;

CREATE EXTERNAL TABLE `datasets.variants_flat_locuspart` (
  `id` string,
  `variantsetid` string,
  `names_1` string,
  `names_2` string,
  `created` bigint,
  `updated` bigint,
  `referencename` string,
  `start` bigint,
  `end` bigint,
  `referencebases` string,
  `alternatebases_1` string,
  `alternatebases_2` string,
  `alleleids_1` string,
  `alleleids_2` string,
  `callsetid` string,
  `callsetname` string,
  `variantid` string,
  `genotype_1` int,
  `genotype_2` int,
  `genotypelikelihood_1` double,
  `genotypelikelihood_2` double)
PARTITIONED BY (
  `chr` string,
  `pos` bigint,
  `sample_group` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://${hiveconf:namenode}/user/tom/datasets/variants_flat_locuspart'

