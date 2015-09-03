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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Source;
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
import org.ga4gh.models.FlatVariantCall;
import org.ga4gh.models.Variant;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat;
import org.seqdoop.hadoop_bam.VCFInputFormat;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;


/**
 * Rewrites datasets of objects partitioned by genome locus: /chr=.../pos=.../sample_group=... .
 */
@Parameters(commandDescription = "Locus partition tool")
public class LocusPartitionTool extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory
      .getLogger(LocusPartitionTool.class);

  private static final String DATASET_SCHEME = "dataset";

  @Parameter(description="<input-path> <output-path>")
  private List<String> paths;

  @Parameter(names="--schema",
      description="A path to the Avro schema of the input data.")
  private String schemaPath;

  @Parameter(names="--contig-field",
      description="A path to the Avro schema of the input data.")
  private String contigSrc;

  @Parameter(names="--pos-field",
      description="A path to the Avro schema of the input data.")
  private String posSrc;

  @Parameter(names="--segment-size",
      description="The number of base pairs in each segment partition.")
  private long segmentSize = 1000000;

  @Parameter(names="--sample-group",
      description="An identifier for the group of samples being loaded.")
  private String sampleGroup = "default";

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

    Path inputPath = new Path(paths.get(0));
    Path outputPath = new Path(paths.get(1));

    Schema inputSchema = new Schema.Parser().parse(new File(schemaPath));

    Configuration conf = getConf();
    String inputFormat = getFormat(inputPath, conf);

    PartitionStrategy partitionStrategy = buildPartitionStrategy(contigSrc, posSrc, segmentSize);

    // Copy records to avoid problem with Parquet string statistics not being correct.
    // This can be removed from parquet 1.8.0
    // (see https://issues.apache.org/jira/browse/PARQUET-251).
    conf.setBoolean(DatasetKeyOutputFormat.KITE_COPY_RECORDS, true);

    Pipeline pipeline = new MRPipeline(getClass(), "LocusPartitionTool");
    PCollection<GenericData.Record> records;
    if (inputFormat.equals("avro")) {
      records = pipeline.read(From.avroFile(inputPath, Avros.generics(inputSchema)));
    } else if (inputFormat.equals("parquet")) {
      Source<GenericData.Record> source =
          new AvroParquetFileSource(inputPath, Avros.generics(inputSchema));
      records = pipeline.read(source);
    }

    PCollection<GenericData.Record> partitioned =
        CrunchDatasetsExtension.partitionAndSort(records, dataset, new
            FlatVariantCallRecordMapFn(sortKeySchema), sortKeySchema, numReducers, 1);







//
//  = readVariants(inputPath, conf, pipeline);
//
//    PCollection<FlatVariantCall> flatRecords = records.parallelDo(
//        new FlattenVariantFn(), Avros.specifics(FlatVariantCall.class));
//
//    DatasetDescriptor desc = new DatasetDescriptor.Builder()
//        .schema(FlatVariantCall.getClassSchema())
//        .partitionStrategy(buildPartitionStrategy(segmentSize))
//        .format(Formats.PARQUET)
//        .compressionType(CompressionType.Uncompressed)
//        .build();
//
//    View<FlatVariantCall> dataset;
//    String outputKiteUri;
//    if (outputPathString.startsWith(DATASET_SCHEME + ":")) {
//      outputKiteUri = outputPathString;
//    } else {
//      Path outputPath = new Path(outputPathString);
//      outputPath = outputPath.getFileSystem(conf).makeQualified(outputPath);
//      outputKiteUri = DATASET_SCHEME + ":" + outputPath.toUri();
//    }
//    if (Datasets.exists(outputKiteUri)) {
//      dataset = Datasets.load(outputKiteUri, FlatVariantCall.class)
//          .getDataset().with("sample_group", sampleGroup);
//    } else {
//      dataset = Datasets.create(outputKiteUri, desc, FlatVariantCall.class)
//          .getDataset().with("sample_group", sampleGroup);
//    }
//
//    int numReducers = conf.getInt("mapreduce.job.reduces", 1);
//    System.out.println("Num reducers: " + numReducers);
//
//    final Schema sortKeySchema = SchemaBuilder.record("sortKey")
//        .fields().requiredString("sampleId").endRecord();
//
//    PCollection<FlatVariantCall> partitioned =
//        CrunchDatasetsExtension.partitionAndSort(flatRecords, dataset, new
//            FlatVariantCallRecordMapFn(sortKeySchema), sortKeySchema, numReducers, 1);
//
//    try {
//      Target.WriteMode writeMode =
//          overwrite ? Target.WriteMode.OVERWRITE : Target.WriteMode.DEFAULT;
//      pipeline.write(partitioned, CrunchDatasets.asTarget(dataset), writeMode);
//    } catch (CrunchRuntimeException e) {
//      LOG.error("Crunch runtime error", e);
//      return 1;
//    }

    PipelineResult result = pipeline.done();
    return result.succeeded() ? 0 : 1;

  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new LoadVariantsTool(), args);
    System.exit(exitCode);
  }

  private static String getFormat(Path path, Configuration conf) throws IOException {
    Path file = SchemaUtils.findFile(path, conf);
    String format;
    if (file.getName().endsWith(".avro")) {
      return "avro";
    } else if (file.getName().endsWith(".parquet")) {
      return "parquet";
    } else {
      throw new IllegalStateException("Unrecognized format for " + file);
    }
  }



//  private static PCollection<Variant> readVariants(Path path, Configuration conf,
//                                                   Pipeline pipeline) throws IOException {
//    Path file = SchemaUtils.findFile(path, conf);
//    if (file.getName().endsWith(".avro")) {
//      return pipeline.read(From.avroFile(path, Avros.specifics(Variant.class)));
//    } else if (file.getName().endsWith(".parquet")) {
//      @SuppressWarnings("unchecked")
//      Source<Variant> source = new AvroParquetFileSource(path,
//          Avros.specifics(Variant.class));
//      return pipeline.read(source);
//    } else if (file.getName().endsWith(".vcf")) {
//      TableSource<LongWritable, VariantContextWritable> vcfSource =
//          From.formattedFile(path, VCFInputFormat.class, LongWritable.class,
//              VariantContextWritable.class);
//      return pipeline.read(vcfSource).parallelDo(new VariantContextToVariantFn(),
//          Avros.specifics(Variant.class));
//    }
//    throw new IllegalStateException("Unrecognized format for " + file);
//  }

  static PartitionStrategy buildPartitionStrategy(
      String contigSrc,
      String posSrc,
      long segmentSize) throws IOException {
    return new PartitionStrategy.Builder()
        .identity(contigSrc, "chr")
        .fixedSizeRange(posSrc, "pos", segmentSize)
        .provided("sample_group", "string")
        .build();
  }

  private static class FlatVariantCallRecordMapFn
      extends MapFn<FlatVariantCall, GenericData.Record> {

    private final String sortKeySchemaString; // TODO: improve

    public FlatVariantCallRecordMapFn(Schema sortKeySchema) {
      this.sortKeySchemaString = sortKeySchema.toString();
    }

    @Override
    public GenericData.Record map(FlatVariantCall input) {
      GenericData.Record record =
          new GenericData.Record(new Schema.Parser().parse(sortKeySchemaString));
      record.put("sampleId", input.getCallSetId());
      return record;
    }
  }
}
