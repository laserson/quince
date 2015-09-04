/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.science.quince;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.lib.SecondarySort;

import org.ga4gh.models.FlatVariantCall;

import static org.apache.crunch.types.avro.Avros.*;

public final class CrunchUtils {

  private CrunchUtils() {
  }

  public static PTable<String, FlatVariantCall> partitionAndSort(
      PCollection<FlatVariantCall> flatRecords, long segmentSize, String sampleGroup) {
    // group by partition key (table key), then prepare for sorting by sample ID (first
    // element in value pair)
    PTable<String, Pair<String, FlatVariantCall>> partitionedRecords =
        flatRecords.parallelDo(new ExtractPartitionKeyFn(segmentSize, sampleGroup),
            tableOf(strings(), pairs(strings(), flatRecords.getPType())));
    // do the sort, and extract the partition key and full record
    PTable<String, FlatVariantCall> partitionedAndSortedRecords =
        SecondarySort.sortAndApply(partitionedRecords, new ExtractEntityFn(),
            tableOf(strings(), flatRecords.getPType()));
    return partitionedAndSortedRecords;
  }

  public static String extractPartitionKey(FlatVariantCall flat, long segmentSize,
      String sampleGroup) {
    StringBuilder sb = new StringBuilder();
    sb.append("chr=").append(flat.getReferenceName());
    sb.append("/pos=").append(getRangeStart(segmentSize, flat.getStart()));
    sb.append("/sample_group=").append(sampleGroup);
    return sb.toString();
  }

  public static long getRangeStart(long size, long value) {
    return Math.round(Math.floor(value / ((double) size))) * size;
  }

  /*
   * Turns a variant call into a (partition key, (sample group, variant call)) pair.
   */
  private static final class ExtractPartitionKeyFn
      extends DoFn<FlatVariantCall, Pair<String, Pair<String, FlatVariantCall>>> {
    private long segmentSize;
    private String sampleGroup;

    private ExtractPartitionKeyFn(long segmentSize, String sampleGroup) {
      this.segmentSize = segmentSize;
      this.sampleGroup = sampleGroup;
    }

    @Override
    public void process(FlatVariantCall input,
        Emitter<Pair<String, Pair<String, FlatVariantCall>>> emitter) {
      String partitionKey = extractPartitionKey(input, segmentSize, sampleGroup);
      emitter.emit(Pair.of(partitionKey, Pair.of(sampleGroup, input)));
    }
  }
  /*
   * Turns a (partition key, (sample group, variant call)) pair into a
   * (partition key, variant call) pair.
   */
  private static final class ExtractEntityFn extends
      DoFn<Pair<String, Iterable<Pair<String, FlatVariantCall>>>,
          Pair<String, FlatVariantCall>> {

    @Override
    public void process(Pair<String, Iterable<Pair<String, FlatVariantCall>>> input,
        Emitter<Pair<String, FlatVariantCall>> emitter) {
      String partitionKey = input.first();
      for (Pair<String, FlatVariantCall> pair : input.second()) {
        FlatVariantCall variantCall = pair.second();
        emitter.emit(Pair.of(partitionKey, variantCall));
      }
    }
  }
}
