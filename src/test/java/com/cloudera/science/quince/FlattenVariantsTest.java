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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.crunch.Emitter;
import org.ga4gh.models.Call;
import org.ga4gh.models.FlatVariantCall;
import org.ga4gh.models.Variant;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FlattenVariantsTest {

  @Test
  public void testVCF() {
    Variant v = Variant.newBuilder()
        .setId(".")
        .setVariantSetId("")
        .setReferenceName("1")
        .setStart(14396L)
        .setEnd(14400L)
        .setReferenceBases("CTGT")
        .setAlternateBases(ImmutableList.<CharSequence>of("C"))
        .setAlleleIds(ImmutableList.<CharSequence>of("", ""))
        .setCalls(ImmutableList.of(
            Call.newBuilder()
                .setCallSetId("NA12878")
                .setGenotype(ImmutableList.of(0, 1))
                .setVariantId("")
                .build(),
            Call.newBuilder()
                .setCallSetId("NA12891")
                .setGenotype(ImmutableList.of(0, 1))
                .setVariantId("")
                .build()
        ))
        .build();

    class CapturingEmitter implements Emitter<FlatVariantCall> {
      List<FlatVariantCall> flatVariantCalls = Lists.newArrayList();
      @Override
      public void emit(FlatVariantCall flatVariantCall) {
        flatVariantCalls.add(flatVariantCall);
      }
      @Override
      public void flush() { }
    }

    FlattenVariantFn fn = new FlattenVariantFn();
    CapturingEmitter emitter = new CapturingEmitter();
    fn.process(v, emitter);

    assertEquals("Each call in the variant produces a FlatVariantCall", 2,
        emitter.flatVariantCalls.size());

    FlatVariantCall flat1 = emitter.flatVariantCalls.get(0);
    assertEquals(".", flat1.getId());
    assertEquals("1", flat1.getReferenceName());
    assertEquals(14396L, flat1.getStart().longValue());
    assertEquals(14400L, flat1.getEnd().longValue());
    assertEquals("CTGT", flat1.getReferenceBases());
    assertEquals("C", flat1.getAlternateBases1());
    assertEquals("NA12878", flat1.getCallSetId());
    assertEquals(0, flat1.getGenotype1().intValue());
    assertEquals(1, flat1.getGenotype2().intValue());

    FlatVariantCall flat2 = emitter.flatVariantCalls.get(1);
    assertEquals(".", flat2.getId());
    assertEquals("1", flat2.getReferenceName());
    assertEquals(14396L, flat2.getStart().longValue());
    assertEquals(14400L, flat2.getEnd().longValue());
    assertEquals("CTGT", flat2.getReferenceBases());
    assertEquals("C", flat2.getAlternateBases1());
    assertEquals("NA12891", flat2.getCallSetId());
    assertEquals(0, flat2.getGenotype1().intValue());
    assertEquals(1, flat2.getGenotype2().intValue());
  }
}
