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

import com.google.common.collect.Iterables;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.ga4gh.models.Call;
import org.ga4gh.models.Variant;
import org.junit.Test;
import org.seqdoop.hadoop_bam.VariantContextWritable;

import static org.junit.Assert.assertEquals;

public class VariantContextToVariantFnTest {

  @Test
  public void testVCF() throws IOException {
    String input = "datasets/variants_vcf";
    File vcf = new File(input, "small.vcf");

    Configuration conf = new Configuration();
    Path[] vcfs = new Path[] { new Path(vcf.toURI()) };
    VariantContextToVariantFn.configureHeaders(conf, vcfs, "default");
    VariantContextToVariantFn fn = new VariantContextToVariantFn();
    fn.setConfiguration(conf);
    fn.initialize();

    VCFFileReader vcfFileReader = new VCFFileReader(vcf, false);
    VariantContext vc = Iterables.getFirst(vcfFileReader, null);
    VariantContextWritable vcw = new VariantContextWritable();
    vcw.set(vc);

    class CapturingEmitter implements Emitter<Variant> {
      Variant variant;
      @Override
      public void emit(Variant v) {
        variant = v;
      }
      @Override
      public void flush() { }
    }

    CapturingEmitter emitter = new CapturingEmitter();
    fn.process(Pair.of((LongWritable) null, vcw), emitter);
    Variant v = emitter.variant;
    assertEquals(".", v.getId());
    assertEquals("", v.getVariantSetId());
    assertEquals("1", v.getReferenceName());
    assertEquals(14396L, v.getStart().longValue());
    assertEquals(14400L, v.getEnd().longValue());
    assertEquals("CTGT", v.getReferenceBases().toString());
    assertEquals("C", Iterables.getOnlyElement(v.getAlternateBases()));
    List<Call> calls = v.getCalls();
    assertEquals(3, calls.size());
    assertEquals("NA12878", calls.get(0).getCallSetId());
    assertEquals(0, calls.get(0).getGenotype().get(0).intValue());
    assertEquals(1, calls.get(0).getGenotype().get(1).intValue());
  }
}
