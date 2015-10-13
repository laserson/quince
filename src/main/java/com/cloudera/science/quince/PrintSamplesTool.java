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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Prints the unique sample IDs stored in VCF files.
 */
@Parameters(commandDescription = "Print samples tool")
public class PrintSamplesTool extends Configured implements Tool {

  @Parameter(description="<input-path>")
  private List<String> paths;

  @Parameter(names="--samples-per-line")
  private int samplesPerLine = 1;

  @Override
  public int run(String[] args) throws Exception {
    JCommander jc = new JCommander(this);
    try {
      jc.parse(args);
    } catch (ParameterException e) {
      jc.usage();
      return 1;
    }

    if (paths == null || paths.size() != 1) {
      jc.usage();
      return 1;
    }

    String inputPathString = paths.get(0);

    Configuration conf = getConf();
    Path inputPath = new Path(inputPathString);

    Path[] vcfs = FileUtils.findVcfs(inputPath, conf);
    Set<String> samples = SampleUtils.uniqueSamples(conf, vcfs);

    if (samplesPerLine == 0) { // all on one line
      System.out.println(Joiner.on(',').join(samples));
    } else {
      for (List<String> line : Iterables.partition(samples, samplesPerLine)) {
        System.out.println(Joiner.on(',').join(line));
      }
    }

    return 0;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new PrintSamplesTool(), args);
    System.exit(exitCode);
  }

}
