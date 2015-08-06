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

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import org.apache.commons.codec.binary.Base64;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.hadoop.io.LongWritable;
import org.ga4gh.models.CallSet;
import org.ga4gh.models.Variant;
import org.ga4gh.models.VariantSet;
import org.opencb.hpg.bigdata.core.converters.FullVcfCodec;
import org.opencb.hpg.bigdata.core.converters.variation.Genotype2CallSet;
import org.opencb.hpg.bigdata.core.converters.variation.VariantContext2VariantConverter;
import org.opencb.hpg.bigdata.core.converters.variation.VariantConverterContext;
import org.opencb.hpg.bigdata.core.io.VcfBlockIterator;
import org.seqdoop.hadoop_bam.VariantContextWritable;

/**
 * Convert a {@link VariantContextWritable} from a VCF file to a GA4GH
 * {@link org.ga4gh.models.Variant}.
 */
public class VariantContextToVariantFn
    extends DoFn<Pair<LongWritable, VariantContextWritable>, Variant> {

  public static final String VARIANT_HEADER = "variantHeader";

  private transient VariantContext2VariantConverter converter;
  private transient VCFHeader header;
  private transient VariantConverterContext variantConverterContext;

  @Override
  public void initialize() {
    try {
      converter = new VariantContext2VariantConverter();
      byte[] variantHeaders = Base64.decodeBase64(getConfiguration().get(VARIANT_HEADER));
      VcfBlockIterator iterator = new VcfBlockIterator(
          new ByteArrayInputStream(variantHeaders), new FullVcfCodec());

      header = iterator.getHeader();

      int gtSize = header.getGenotypeSamples().size();

      variantConverterContext = new VariantConverterContext();

      VariantSet vs = new VariantSet();
//        vs.setId(file.getName());
//        vs.setDatasetId(file.getName());
//        vs.setReferenceSetId("test");
      vs.setId("test"); //TODO
      vs.setDatasetId("test");
      vs.setReferenceSetId("test");

      List<String> genotypeSamples = header.getGenotypeSamples();
      Genotype2CallSet gtConverter = new Genotype2CallSet();
      for (int gtPos = 0; gtPos < gtSize; ++gtPos) {
        CallSet cs = gtConverter.forward(genotypeSamples.get(gtPos));
        cs.getVariantSetIds().add(vs.getId());
        variantConverterContext.getCallSetMap().put(cs.getName(), cs);
//                callWriter.write(cs);
      }

      converter.setContext(variantConverterContext);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void process(Pair<LongWritable, VariantContextWritable> input, Emitter<Variant> emitter) {
    VariantContext variantContext = input.second().get();
    Variant variant = converter.forward(variantContext);
    emitter.emit(variant);
  }
}
