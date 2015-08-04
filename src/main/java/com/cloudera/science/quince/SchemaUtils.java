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

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.crunch.Source;
import org.apache.crunch.io.From;
import org.apache.crunch.io.parquet.AvroParquetFileSource;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.spi.Schemas;

class SchemaUtils {
  private SchemaUtils() {
  }

  public static Format readFormat(Path path) {
    if (path.getName().endsWith(".avro")) {
      return Formats.AVRO;
    } else if (path.getName().endsWith(".parquet")) {
      return Formats.PARQUET;
    }
    throw new IllegalStateException("Unrecognized format for " + path);
  }

  public static Path findFile(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    if (fs.isDirectory(path)) {
      FileStatus[] fileStatuses = fs.listStatus(path, new PathFilter() {
        @Override
        public boolean accept(Path p) {
          String name = p.getName();
          return !name.startsWith("_") && !name.startsWith(".");
        }
      });
      return fileStatuses[0].getPath();
    } else {
      return path;
    }
  }
}
