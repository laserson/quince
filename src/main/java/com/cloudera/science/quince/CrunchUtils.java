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

import org.apache.crunch.types.avro.Avros;
import org.ga4gh.models.Call;
import org.ga4gh.models.FlatVariantCall;
import org.ga4gh.models.Variant;

import static com.cloudera.science.quince.FlattenVariantFn.flatten;
import static org.apache.crunch.types.avro.Avros.*;

public final class CrunchUtils {

  private CrunchUtils() {
  }

  public static PTable<String, FlatVariantCall> partitionAndSort(
      PCollection<Variant> records, long segmentSize, String sampleGroup) {
    // flatten variants
    PCollection<FlatVariantCall> flatRecords = records.parallelDo(
        new FlattenVariantFn(), Avros.specifics(FlatVariantCall.class));
    // group by partition key (table key), then prepare for sorting by secondary key,
    // which is the sample ID and position (first element in value pair)
    PTable<String, Pair<Pair<String, Long>, FlatVariantCall>> keyedRecords =
        flatRecords.parallelDo(new ExtractPartitionKeyFn(segmentSize, sampleGroup),
            tableOf(strings(), pairs(pairs(strings(), longs()), flatRecords.getPType())));
    // do the sort, and extract the partition key and full record
    PTable<String, FlatVariantCall> partitionedAndSortedRecords =
        SecondarySort.sortAndApply(keyedRecords, new ExtractEntityFn(),
            tableOf(strings(), Avros.specifics(FlatVariantCall.class)));
    return partitionedAndSortedRecords;
  }

  public static String extractPartitionKey(FlatVariantCall variant, long segmentSize,
      String sampleGroup) {
    StringBuilder sb = new StringBuilder();
    sb.append("chr=").append(variant.getReferenceName());
    sb.append("/pos=").append(getRangeStart(segmentSize, variant.getStart()));
    sb.append("/sample_group=").append(sampleGroup);
    return sb.toString();
  }

  public static long getRangeStart(long size, long value) {
    return Math.round(Math.floor(value / ((double) size))) * size;
  }

  /*
   * Turns a variant call into a (partition key, (secondary key, variant call)) pair,
   * where the secondary key is a (sample ID, position) pair.
   */
  private static final class ExtractPartitionKeyFn
      extends DoFn<FlatVariantCall, Pair<String, Pair<Pair<String, Long>, FlatVariantCall>>> {
    private long segmentSize;
    private String sampleGroup;

    private ExtractPartitionKeyFn(long segmentSize, String sampleGroup) {
      this.segmentSize = segmentSize;
      this.sampleGroup = sampleGroup;
    }

    @Override
    public void process(FlatVariantCall input,
        Emitter<Pair<String, Pair<Pair<String, Long>, FlatVariantCall>>> emitter) {
      String partitionKey = extractPartitionKey(input, segmentSize, sampleGroup);
      Pair<String, Long> secondaryKey = Pair.of(input.getCallSetId().toString(), input
          .getStart());
      emitter.emit(Pair.of(partitionKey, Pair.of(secondaryKey, input)));
    }
  }
  /*
   * Turns a (partition key, (secondary key, flat variant call)) pair into a
   * (partition key, flat variant call) pair.
   */
  private static final class ExtractEntityFn extends
      DoFn<Pair<String, Iterable<Pair<Pair<String, Long>, FlatVariantCall>>>,
          Pair<String, FlatVariantCall>> {

    @Override
    public void process(Pair<String, Iterable<Pair<Pair<String, Long>, FlatVariantCall>>> input,
        Emitter<Pair<String, FlatVariantCall>> emitter) {
      String partitionKey = input.first();
      for (Pair<Pair<String, Long>, FlatVariantCall> pair : input.second()) {
        FlatVariantCall variantCall = pair.second();
        emitter.emit(Pair.of(partitionKey, variantCall));
      }
    }
  }
  /*
   * Turns a (partition key, list[secondary key, variant call]) pair into a series of
   * (partition key, flat variant call) pairs, expanding the calls (samples) in the variant.
   */
  private static final class ExtractFlatVariantsFn extends
      DoFn<Pair<String, Iterable<Pair<Pair<String, Long>, Variant>>>,
          Pair<String, FlatVariantCall>> {

    @Override
    public void process(Pair<String, Iterable<Pair<Pair<String, Long>, Variant>>> input,
        Emitter<Pair<String, FlatVariantCall>> emitter) {
      String partitionKey = input.first();
      for (Pair<Pair<String, Long>, Variant> pair : input.second()) {
        Variant variant = pair.second();
        for (Call call : variant.getCalls()) {
          emitter.emit(Pair.of(partitionKey, flatten(variant, call)));
        }
      }
    }
  }
}
