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

import java.util.List;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.ga4gh.models.Call;
import org.ga4gh.models.FlatVariantCall;
import org.ga4gh.models.Variant;

class FlattenVariantFn extends DoFn<Variant, FlatVariantCall> {
  @Override
  public void process(Variant variant, Emitter<FlatVariantCall> emitter) {
    for (Call call : variant.getCalls()) {
      FlatVariantCall flatVariantCall = new FlatVariantCall();
      flatVariantCall.setId(variant.getId());
      flatVariantCall.setVariantSetId(variant.getVariantSetId());
      flatVariantCall.setNames1(get(variant.getNames(), 0));
      flatVariantCall.setNames2(get(variant.getNames(), 1));
      flatVariantCall.setCreated(variant.getCreated());
      flatVariantCall.setUpdated(variant.getUpdated());
      flatVariantCall.setReferenceName(variant.getReferenceName());
      flatVariantCall.setStart(variant.getStart());
      flatVariantCall.setEnd(variant.getEnd());
      flatVariantCall.setReferenceBases(variant.getReferenceBases());
      flatVariantCall.setAlternateBases1(get(variant.getAlternateBases(), 0));
      flatVariantCall.setAlternateBases2(get(variant.getAlternateBases(), 1));
      flatVariantCall.setAlleleIds1(get(variant.getAlleleIds(), 0));
      flatVariantCall.setAlleleIds2(get(variant.getAlleleIds(), 1));
      // variant.getInfo(); TODO: ignored for now
      flatVariantCall.setCallSetId(call.getCallSetId());
      flatVariantCall.setCallSetName(call.getCallSetName());
      flatVariantCall.setVariantId(call.getVariantId());
      flatVariantCall.setGenotype1(get(call.getGenotype(), 0));
      flatVariantCall.setGenotype2(get(call.getGenotype(), 1));
      // call.getPhaseset(); TODO: ignored for now
      flatVariantCall.setGenotypeLikelihood1(get(call.getGenotypeLikelihood(), 0));
      flatVariantCall.setGenotypeLikelihood2(get(call.getGenotypeLikelihood(), 1));
      //call.getInfo(); TODO: ignored for now
      emitter.emit(flatVariantCall);
    }
  }
  private static <T> T get(List<T> names, int index) {
    if (names == null) {
      return null;
    }
    return index < names.size() ? names.get(index) : null;
  }
}
