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
import org.junit.Before;
import org.junit.Test;

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

    String sampleGroup = "sample1";
    String input = "datasets/variants_avro/small.ga4gh.avro";
    String output = "dataset:file:target/datasets/variants_flat_locuspart";

    int exitCode = tool.run(new String[]{ "--sample-group", sampleGroup, input, output });

    assertEquals(0, exitCode);
    File partition = new File(baseDir,
        "variants_flat_locuspart/chr=1/pos=0/sample_group=sample1");
    assertTrue(partition.exists());

    File[] dataFiles = partition.listFiles(new FileFilter() {
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

    // loading into a new sample group should always succeed
    exitCode = tool.run(new String[]{ "--sample-group", "sample2", input, output });
    assertEquals(0, exitCode);

  }

  @Test
  public void testSmallVCF() throws Exception {

    String baseDir = "target/datasets";

    FileUtil.fullyDelete(new File(baseDir));

    String sampleGroup = "sample1";
    String input = "datasets/variants_vcf/small.vcf";
    String output = "dataset:file:target/datasets/variants_flat_locuspart";

    int exitCode = tool.run(new String[]{"--sample-group", sampleGroup, input, output});

    assertEquals(0, exitCode);
    File partition = new File(baseDir,
        "variants_flat_locuspart/chr=1/pos=0/sample_group=sample1");
    assertTrue(partition.exists());

    File[] dataFiles = partition.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return !pathname.getName().startsWith(".");
      }
    });

    assertEquals(1, dataFiles.length);
    assertTrue(dataFiles[0].getName().endsWith(".parquet"));
  }
}
