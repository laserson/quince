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

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemOutRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PrintSamplesToolIT {

  private PrintSamplesTool tool;

  @Rule
  public final SystemOutRule systemOutRule = new SystemOutRule().enableLog();

  @Before
  public void setUp() {
    tool = new PrintSamplesTool();
    Configuration conf = new Configuration();
    tool.setConf(conf);
  }

  @Test
  public void testOneSamplePerLine() throws Exception {
    int exitCode = tool.run(new String[]{"datasets/variants_vcf"});
    assertEquals(0, exitCode);
    assertTrue(systemOutRule.getLog().contains("NA12878\nNA12891\nNA12892\n"));
  }

  @Test
  public void testTwoPerLine() throws Exception {
    int exitCode = tool.run(new String[]{"datasets/variants_vcf", "--samples-per-line", "2"});
    assertEquals(0, exitCode);
    assertTrue(systemOutRule.getLog().contains("NA12878,NA12891\nNA12892\n"));
  }

  @Test
  public void testAllOnOneLine() throws Exception {
    int exitCode = tool.run(new String[]{"datasets/variants_vcf", "--samples-per-line", "0"});
    assertEquals(0, exitCode);
    assertTrue(systemOutRule.getLog().contains("NA12878,NA12891,NA12892\n"));
  }

}
