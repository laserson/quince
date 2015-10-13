#!/usr/bin/env bash
#
# Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
#
# Cloudera, Inc. licenses this file to you under the Apache License,
# Version 2.0 (the "License"). You may not use this file except in
# compliance with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for
# the specific language governing permissions and limitations under the
# License.

hadoop fs -mkdir vcf-1000genomes

for chr in $(seq 1 22); do
  echo "Downloading chromosome $chr"
  hadoop fs -rm vcf-1000genomes/ALL.chr${chr}.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf
  curl ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20130502/ALL.chr${chr}.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz \
    | gunzip -c \
    | hadoop fs -put - vcf-1000genomes/ALL.chr${chr}.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf
done

echo "Downloading chromosome X"
hadoop fs -rm vcf-1000genomes/ALL.chrX.phase3_shapeit2_mvncall_integrated_v1b.20130502.genotypes.vcf
curl ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20130502/ALL.chrX.phase3_shapeit2_mvncall_integrated_v1b.20130502.genotypes.vcf.gz \
  | gunzip -c \
  | hadoop fs -put - vcf-1000genomes/ALL.chrX.phase3_shapeit2_mvncall_integrated_v1b.20130502.genotypes.vcf