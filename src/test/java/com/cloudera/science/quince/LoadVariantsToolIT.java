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

import java.io.File;
import java.io.FileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.ga4gh.models.FlatVariantCall;
import org.junit.Before;
import org.junit.Test;
import parquet.avro.AvroParquetReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LoadVariantsToolIT {

  LoadVariantsTool tool;

  @Before
  public void setUp() {
    tool = new LoadVariantsTool();
    Configuration conf = new Configuration();
    tool.setConf(conf);
  }

  @Test
  public void testMissingPaths() throws Exception {
    int exitCode = tool.run(new String[0]);
    assertEquals(1, exitCode);
    // TODO: verify output
  }

  @Test
  public void testInvalidOption() throws Exception {
    int exitCode = tool.run(new String[]{"--invalid", "blah", "foo", "bar"});
    assertEquals(1, exitCode);
    // TODO: verify output
  }

  @Test
  public void testSmallAvro() throws Exception {

    String baseDir = "target/datasets";

    FileUtil.fullyDelete(new File(baseDir));

    String sampleGroup = "default";
    String input = "datasets/variants_avro/small.ga4gh.avro";
    String output = "target/datasets/variants_flat_locuspart";

    int exitCode = tool.run(new String[]{"--sample-group", sampleGroup, input, output});

    assertEquals(0, exitCode);
    File partition1 = new File(baseDir,
        "variants_flat_locuspart/chr=1/pos=0/sample_group=default");
    assertTrue(partition1.exists());

    File[] dataFiles = partition1.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return !pathname.getName().startsWith(".");
      }
    });

    assertEquals(1, dataFiles.length);
    assertTrue(dataFiles[0].getName().endsWith(".parquet"));

    // loading into the same sample group again should fail
    exitCode = tool.run(new String[]{ "--sample-group", sampleGroup, input, output });
    assertEquals(1, exitCode);

    // unless the overwrite option is specified
    exitCode = tool.run(new String[]{ "--overwrite", "--sample-group", sampleGroup,
        input, output });
    assertEquals(0, exitCode);
    assertTrue(partition1.exists());

    // loading into a new sample group should always succeed
    exitCode = tool.run(new String[]{ "--sample-group", "sample2", input, output });
    assertEquals(0, exitCode);
    File partition2 = new File(baseDir,
        "variants_flat_locuspart/chr=1/pos=0/sample_group=sample2");
    assertTrue(partition1.exists());
    assertTrue(partition2.exists());

    // overwriting an existing sample group should leave others untouched
    exitCode = tool.run(new String[]{ "--overwrite", "--sample-group", sampleGroup,
        input, output });
    assertEquals(0, exitCode);
    assertTrue(partition1.exists());
    assertTrue(partition2.exists());

  }

  @Test
  public void testSmallVCF() throws Exception {

    String baseDir = "target/datasets";

    FileUtil.fullyDelete(new File(baseDir));

    String sampleGroup = "default";
    String input = "datasets/variants_vcf";
    String output = "target/datasets/variants_flat_locuspart";

    int exitCode = tool.run(new String[]{"--sample-group", sampleGroup, input, output});

    assertEquals(0, exitCode);
    File partition = new File(baseDir,
        "variants_flat_locuspart/chr=1/pos=0/sample_group=default");
    assertTrue(partition.exists());

    File[] dataFiles = partition.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return !pathname.getName().startsWith(".");
      }
    });

    assertEquals(1, dataFiles.length);
    assertTrue(dataFiles[0].getName().endsWith(".parquet"));

    AvroParquetReader<FlatVariantCall> parquetReader =
        new AvroParquetReader<>(new Path(dataFiles[0].toURI()));

    // first record has first sample (call set) ID
    FlatVariantCall flat1 = parquetReader.read();
    assertEquals(".", flat1.getId());
    assertEquals("1", flat1.getReferenceName());
    assertEquals(14396L, flat1.getStart().longValue());
    assertEquals(14400L, flat1.getEnd().longValue());
    assertEquals("CTGT", flat1.getReferenceBases());
    assertEquals("C", flat1.getAlternateBases1());
    assertEquals("NA12878", flat1.getCallSetId());
    assertEquals(0, flat1.getGenotype1().intValue());
    assertEquals(1, flat1.getGenotype2().intValue());

    // check records are sorted by sample id, then ref and start position
    String previousCallSetId = flat1.getCallSetId().toString();
    String previousRef = flat1.getReferenceName().toString();
    Long previousStart = flat1.getStart();
    while (true) {
      FlatVariantCall flat = parquetReader.read();
      if (flat == null) {
        break;
      }
      String callSetId = flat.getCallSetId().toString();
      String ref = flat1.getReferenceName().toString();
      Long start = flat1.getStart();
      assertTrue("Should be sorted by callSetId",
          previousCallSetId.compareTo(callSetId) <= 0);

      if (previousCallSetId.compareTo(callSetId) == 0) {
        assertTrue("Should be sorted by ref within callSetId",
            previousRef.compareTo(ref) <= 0);
        assertTrue("Should be sorted by start within callSetId",
            previousStart.compareTo(start) <= 0);
      }

      previousCallSetId = callSetId;
      previousRef = ref;
      previousStart = start;
    }
  }

}
