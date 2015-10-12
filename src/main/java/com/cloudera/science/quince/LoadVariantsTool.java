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
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.parquet.AvroParquetPathPerKeyTarget;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.ga4gh.models.FlatVariantCall;
import org.ga4gh.models.Variant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads Variants stored in VCF, or Avro or Parquet GA4GH format, into a Hadoop
 * filesystem, ready for querying with Hive or Impala.
 */
@Parameters(commandDescription = "Load variants tool")
public class LoadVariantsTool extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory
      .getLogger(LoadVariantsTool.class);

  @Parameter(description="<input-path> <output-path>")
  private List<String> paths;

  @Parameter(names="--overwrite",
      description="Allow data for an existing sample group to be overwritten.")
  private boolean overwrite = false;

  @Parameter(names="--sample-group",
      description="An identifier for the group of samples being loaded.")
  private String sampleGroup = "default";

  @Parameter(names="--samples",
      description="Comma-separated list of samples to include.")
  private String samples;

  @Parameter(names="--segment-size",
      description="The number of base pairs in each segment partition.")
  private long segmentSize = 1000000;

  @Parameter(names="--sort-reduce-side",
      description="Sorting is done on the reduce side (takes more memory) rather than " +
          "using the shuffle (slower).")
  private boolean sortReduceSide = false;

  @Parameter(names="--num-reducers",
      description="The number of reducers to use.")
  private int numReducers = 0;

  @Override
  public int run(String[] args) throws Exception {
    JCommander jc = new JCommander(this);
    try {
      jc.parse(args);
    } catch (ParameterException e) {
      jc.usage();
      return 1;
    }

    if (paths == null || paths.size() != 2) {
      jc.usage();
      return 1;
    }

    String inputPathString = paths.get(0);
    String outputPathString = paths.get(1);

    Configuration conf = getConf();
    Path inputPath = new Path(inputPathString);

    Path file = FileUtils.findFile(inputPath, conf);
    if (file.getName().endsWith(".vcf")) {
      VariantContextToVariantFn.configureHeaders(conf,
          FileUtils.findVcfs(inputPath, conf), sampleGroup);
    }

    Pipeline pipeline = new MRPipeline(getClass(), conf);
    PCollection<Variant> records = FileUtils.readVariants(inputPath, conf, pipeline);

    if (numReducers == 0) {
      numReducers = conf.getInt("mapreduce.job.reduces", 1);
      System.out.println("Set num reducers from mapreduce.job.reduces to: " +
          numReducers);
    }

    Set<String> sampleSet = samples == null ? null :
        Sets.newLinkedHashSet(Splitter.on(',').split(samples));

    PTable<String, FlatVariantCall> partitioned =
        sortReduceSide ?
        CrunchUtils.partitionAndSortReduceSide(records, segmentSize, sampleGroup,
            sampleSet, numReducers) :
        CrunchUtils.partitionAndSortUsingShuffle(records, segmentSize, sampleGroup,
            sampleSet, numReducers);

    try {
      Path outputPath = new Path(outputPathString);
      outputPath = outputPath.getFileSystem(conf).makeQualified(outputPath);

      if (FileUtils.sampleGroupExists(outputPath, conf, sampleGroup)) {
        if (overwrite) {
          FileUtils.deleteSampleGroup(outputPath, conf, sampleGroup);
        } else {
          LOG.error("Sample group already exists: " + sampleGroup);
          return 1;
        }
      }

      pipeline.write(partitioned, new AvroParquetPathPerKeyTarget(outputPath),
          Target.WriteMode.APPEND);
    } catch (CrunchRuntimeException e) {
      LOG.error("Crunch runtime error", e);
      return 1;
    }

    PipelineResult result = pipeline.done();
    return result.succeeded() ? 0 : 1;

  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new LoadVariantsTool(), args);
    System.exit(exitCode);
  }

}
