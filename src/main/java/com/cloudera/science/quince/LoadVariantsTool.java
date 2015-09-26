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
import java.io.IOException;
import java.util.List;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Source;
import org.apache.crunch.Target;
import org.apache.crunch.TableSource;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.parquet.AvroParquetFileSource;
import org.apache.crunch.io.parquet.AvroParquetPathPerKeyTarget;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.ga4gh.models.FlatVariantCall;
import org.ga4gh.models.Variant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.seqdoop.hadoop_bam.VCFInputFormat;
import org.seqdoop.hadoop_bam.VariantContextWritable;

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
  private String sampleGroup = "sample1";

  @Parameter(names="--segment-size",
      description="The number of base pairs in each segment partition.")
  private long segmentSize = 1000000;

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

    Path file = findFile(inputPath, conf);
    if (file.getName().endsWith(".vcf")) {
      VariantContextToVariantFn.configureHeaders(conf, findVcfs(inputPath, conf), sampleGroup);
    }

    Pipeline pipeline = new MRPipeline(getClass(), conf);
    PCollection<Variant> records = readVariants(inputPath, conf, pipeline);

    int numReducers = conf.getInt("mapreduce.job.reduces", 1);
    System.out.println("Num reducers: " + numReducers);

    PTable<String, FlatVariantCall> partitioned =
        CrunchUtils.partitionAndSort(records, segmentSize, sampleGroup);

    try {
      Path outputPath = new Path(outputPathString);
      outputPath = outputPath.getFileSystem(conf).makeQualified(outputPath);

      if (sampleGroupExists(outputPath, conf, sampleGroup)) {
        if (overwrite) {
          deleteSampleGroup(outputPath, conf, sampleGroup);
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

  private static PCollection<Variant> readVariants(Path path, Configuration conf,
      Pipeline pipeline) throws IOException {
    Path file = findFile(path, conf);
    if (file.getName().endsWith(".avro")) {
      return pipeline.read(From.avroFile(path, Avros.specifics(Variant.class)));
    } else if (file.getName().endsWith(".parquet")) {
      @SuppressWarnings("unchecked")
      Source<Variant> source = new AvroParquetFileSource(path,
          Avros.specifics(Variant.class));
      return pipeline.read(source);
    } else if (file.getName().endsWith(".vcf")) {
      TableSource<LongWritable, VariantContextWritable> vcfSource =
          From.formattedFile(path, VCFInputFormat.class, LongWritable.class,
          VariantContextWritable.class);
      return pipeline.read(vcfSource).parallelDo(new VariantContextToVariantFn(),
          Avros.specifics(Variant.class));
    }
    throw new IllegalStateException("Unrecognized format for " + file);
  }

  private static Path findFile(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    if (fs.isDirectory(path)) {
      FileStatus[] fileStatuses = fs.listStatus(path, new HiddenPathFilter());
      return fileStatuses[0].getPath();
    } else {
      return path;
    }
  }

  private static Path[] findVcfs(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    if (fs.isDirectory(path)) {
      FileStatus[] fileStatuses = fs.listStatus(path, new HiddenPathFilter());
      Path[] vcfs = new Path[fileStatuses.length];
      int i = 0;
      for (FileStatus status : fileStatuses) {
        vcfs[i++] = status.getPath();
      }
      return vcfs;
    } else {
      return new Path[] { path };
    }
  }


  private static boolean sampleGroupExists(Path path, Configuration conf, String sampleGroup)
      throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    if (!fs.exists(path)) {
      return false;
    }
    for (FileStatus chrStatus : fs.listStatus(path, new PartitionPathFilter("chr"))) {
      for (FileStatus posStatus : fs.listStatus(chrStatus.getPath(),
          new PartitionPathFilter("pos"))) {
        if (fs.listStatus(posStatus.getPath(),
            new PartitionPathFilter("sample_group", sampleGroup)).length > 0) {
          return true;
        }
      }
    }
    return false;
  }

  private static void deleteSampleGroup(Path path, Configuration conf, String sampleGroup)
      throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    if (!fs.exists(path)) {
      return;
    }
    for (FileStatus chrStatus : fs.listStatus(path, new PartitionPathFilter("chr"))) {
      for (FileStatus posStatus : fs.listStatus(chrStatus.getPath(),
          new PartitionPathFilter("pos"))) {
        for (FileStatus sampleGroupStatus : fs.listStatus(posStatus.getPath(),
            new PartitionPathFilter("sample_group", sampleGroup))) {
          fs.delete(sampleGroupStatus.getPath(), true);
        }
      }
    }
  }

  static class HiddenPathFilter implements PathFilter {
    @Override
    public boolean accept(Path p) {
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  }

  static class PartitionPathFilter implements PathFilter {
    private final String partitionName;
    private final String partitionValue;
    public PartitionPathFilter(String partitionName) {
      this(partitionName, null);
    }
    public PartitionPathFilter(String partitionName, String partitionValue) {
      this.partitionName = partitionName;
      this.partitionValue = partitionValue;
    }
    @Override
    public boolean accept(Path path) {
      if (partitionValue == null) {
        return path.getName().startsWith(partitionName + "=");
      } else {
        return path.getName().equals(partitionName + "=" + partitionValue);
      }
    }
  }

}
