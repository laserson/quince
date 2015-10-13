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

import htsjdk.variant.vcf.VCFHeader;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.opencb.hpg.bigdata.core.converters.FullVcfCodec;
import org.opencb.hpg.bigdata.core.io.VcfBlockIterator;

public final class SampleUtils {

  private SampleUtils() {
  }

  public static Set<String> uniqueSamples(Configuration conf, Path[] vcfs)
      throws IOException {
    Set<String> samples = new LinkedHashSet<>();
    for (Path vcf : vcfs) {
      InputStream inputStream = vcf.getFileSystem(conf).open(vcf);
      VcfBlockIterator iterator = new VcfBlockIterator(inputStream, new FullVcfCodec());
      VCFHeader header = iterator.getHeader();
      samples.addAll(header.getGenotypeSamples());
    }
    return samples;
  }
}
