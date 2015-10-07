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
import htsjdk.variant.vcf.VCFHeaderLine;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

  private static final String SAMPLE_GROUP = "sampleGroup";
  private static final String VARIANT_HEADERS = "variantHeaders";
  private static final String VARIANT_SET_ID = "quinceVariantSetId";

  public static void configureHeaders(Configuration conf, Path[] vcfs, String sampleGroup)
      throws IOException {
    List<VCFHeader> headers = new ArrayList<>();
    for (Path vcf : vcfs) {
      InputStream inputStream = vcf.getFileSystem(conf).open(vcf);
      VcfBlockIterator iterator = new VcfBlockIterator(inputStream, new FullVcfCodec());
      VCFHeader header = iterator.getHeader();
      header.addMetaDataLine(new VCFHeaderLine(VARIANT_SET_ID, vcf.getName()));
      headers.add(header);
    }
    VCFHeader[] headersArray = headers.toArray(new VCFHeader[headers.size()]);
    conf.set(VariantContextToVariantFn.VARIANT_HEADERS,
        Base64.encodeBase64String(SerializationUtils.serialize(headersArray)));
    conf.set(VariantContextToVariantFn.SAMPLE_GROUP, sampleGroup);
  }

  private transient VariantContext2VariantConverter converter;
  private transient VariantConverterContext variantConverterContext;

  @Override
  public void initialize() {
    converter = new VariantContext2VariantConverter();
    variantConverterContext = new VariantConverterContext();

    byte[] variantHeaders = Base64.decodeBase64(getConfiguration().get(VARIANT_HEADERS));
    VCFHeader[] headers = (VCFHeader[]) SerializationUtils.deserialize(variantHeaders);

    for (VCFHeader header : headers) {
      VariantSet vs = new VariantSet();
      vs.setId(header.getMetaDataLine(VARIANT_SET_ID).getValue());
      vs.setDatasetId(getConfiguration().get(SAMPLE_GROUP)); // dataset = sample group
      VCFHeaderLine reference = header.getMetaDataLine("reference");
      vs.setReferenceSetId(reference == null ? "unknown" : reference.getValue());

      Genotype2CallSet gtConverter = new Genotype2CallSet();
      for (String genotypeSample : header.getGenotypeSamples()) {
        CallSet cs = gtConverter.forward(genotypeSample);
        cs.getVariantSetIds().add(vs.getId());
        // it's OK if there are duplicate sample names from different headers since the
        // only information we require for conversion is the name itself so there's no
        // scope for conflict
        variantConverterContext.getCallSetMap().put(cs.getName(), cs);
      }
    }
    converter.setContext(variantConverterContext);
  }

  @Override
  public void process(Pair<LongWritable, VariantContextWritable> input, Emitter<Variant> emitter) {
    VariantContext variantContext = input.second().get();
    Variant variant = converter.forward(variantContext);
    emitter.emit(variant);
  }
}
