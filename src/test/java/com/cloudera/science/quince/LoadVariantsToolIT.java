package com.cloudera.science.quince;

import java.io.File;
import java.io.FileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LoadVariantsToolIT {
  @Test
  public void test() throws Exception {

    String baseDir = "target/datasets";

    FileUtil.fullyDelete(new File(baseDir));

    LoadVariantsTool tool = new LoadVariantsTool();
    Configuration conf = new Configuration();
    tool.setConf(conf);

    String sampleGroup = "sample1";
    String input = "datasets/variants_avro";
    String output = "dataset:file:target/datasets/variants_flat_locuspart";

    int exitCode = tool.run(new String[]{ sampleGroup, input, output });

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
