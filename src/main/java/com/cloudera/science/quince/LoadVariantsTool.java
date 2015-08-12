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
import java.io.InputStream;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.commons.codec.binary.Base64;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Source;
import org.apache.crunch.Target;
import org.apache.crunch.TableSource;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.parquet.AvroParquetFileSource;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.ga4gh.models.FlatVariant;
import org.ga4gh.models.Variant;
import org.kitesdk.data.CompressionType;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Formats;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.View;
import org.kitesdk.data.crunch.CrunchDatasets;
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.seqdoop.hadoop_bam.VCFInputFormat;
import org.seqdoop.hadoop_bam.VariantContextWritable;

/**
 * Loads Variants stored in Avro or Parquet GA4GH format into a Hadoop filesystem,
 * ready for querying with Hive or Impala.
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

    String inputPath = paths.get(0);
    String outputPath = paths.get(1);

    Configuration conf = getConf();
    // Copy records to avoid problem with Parquet string statistics not being correct.
    // This can be removed from parquet 1.8.0
    // (see https://issues.apache.org/jira/browse/PARQUET-251).
    conf.setBoolean(DatasetKeyOutputFormat.KITE_COPY_RECORDS, true);

    Path path = new Path(inputPath);

    if (path.getName().endsWith(".vcf")) {
      int size = 500000;
      byte[] bytes = new byte[size];
      InputStream inputStream = path.getFileSystem(conf).open(path);
      inputStream.read(bytes, 0, size);
      conf.set(VariantContextToVariantFn.VARIANT_HEADER, Base64.encodeBase64String(bytes));
    }

    Pipeline pipeline = new MRPipeline(getClass(), conf);
    PCollection<Variant> records = readVariants(path, conf, pipeline);

    PCollection<FlatVariant> flatRecords = records.parallelDo(
        new FlattenVariantFn(), Avros.specifics(FlatVariant.class));

    DatasetDescriptor desc = new DatasetDescriptor.Builder()
        .schema(FlatVariant.getClassSchema())
        .partitionStrategy(buildPartitionStrategy(segmentSize))
        .format(Formats.PARQUET)
        .compressionType(CompressionType.Uncompressed)
        .build();

    View<FlatVariant> dataset;
    if (Datasets.exists(outputPath)) {
      dataset = Datasets.load(outputPath, FlatVariant.class)
          .getDataset().with("sample_group", sampleGroup);
    } else {
      dataset = Datasets.create(outputPath, desc, FlatVariant.class)
          .getDataset().with("sample_group", sampleGroup);
    }

    int numReducers = conf.getInt("mapreduce.job.reduces", 1);
    System.out.println("Num reducers: " + numReducers);

    final Schema sortKeySchema = SchemaBuilder.record("sortKey")
        .fields().requiredString("sampleId").endRecord();

    PCollection<FlatVariant> partitioned =
        CrunchDatasetsExtension.partitionAndSort(flatRecords, dataset, new
            FlatVariantRecordMapFn(sortKeySchema), sortKeySchema, numReducers, 1);

    try {
      Target.WriteMode writeMode =
          overwrite ? Target.WriteMode.OVERWRITE : Target.WriteMode.DEFAULT;
      pipeline.write(partitioned, CrunchDatasets.asTarget(dataset), writeMode);
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
    Path file = SchemaUtils.findFile(path, conf);
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

  static PartitionStrategy buildPartitionStrategy(long segmentSize) throws IOException {
    return new PartitionStrategy.Builder()
        .identity("referenceName", "chr")
        .fixedSizeRange("start", "pos", segmentSize)
        .provided("sample_group", "string")
        .build();
  }

  private static class FlatVariantRecordMapFn extends MapFn<FlatVariant, GenericData.Record> {

    private final String sortKeySchemaString; // TODO: improve

    public FlatVariantRecordMapFn(Schema sortKeySchema) {
      this.sortKeySchemaString = sortKeySchema.toString();
    }

    @Override
    public GenericData.Record map(FlatVariant input) {
      GenericData.Record record =
          new GenericData.Record(new Schema.Parser().parse(sortKeySchemaString));
      record.put("sampleId", input.getCallSetId());
      return record;
    }
  }
}
