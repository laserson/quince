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

hadoop fs -mkdir dbsnp

hadoop fs -rm dbsnp/00-All.vcf
curl ftp://ftp.ncbi.nih.gov/snp/organisms/human_9606/VCF/00-All.vcf.gz \
  | gunzip -c \
  | hadoop fs -put - dbsnp/00-All.vcf
