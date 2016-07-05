/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.hydrator.plugin.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.ReferenceBatchSource;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.hydrator.common.batch.JobUtils;
import co.cask.hydrator.plugin.common.BatchXMLFileFilter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * XML Reader Batch Source Plugin
 * It is used to read XML files from HDFS with specified file properties and filters.
 * It parses the read file into specified Output Schema.
 * A {@link FileBatchSource} to use any distributed file system as a Source.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("XMLReader")
@Description("Batch source for XML read from HDFS")
public class XMLReaderBatchSource extends ReferenceBatchSource<LongWritable, Object, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(XMLReaderBatchSource.class);
  private static final Gson GSON = new Gson();
  private static final Type ARRAYLIST_PREPROCESSED_FILES  = new TypeToken<ArrayList<String>>() { }.getType();

  public static final Schema DEFAULT_XML_SCHEMA = Schema.recordOf(
    "xmlSchema",
    Schema.Field.of("offset", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("filename", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("record", Schema.of(Schema.Type.STRING))
  );

  private final XMLReaderConfig config;

  private KeyValueTable processedFileTrackingTable;
  private FileSystem fileSystem;
  private Path tempDirectoryPath;

  public XMLReaderBatchSource(XMLReaderConfig config) {
    super(config);
    this.config = config;
  }

  @VisibleForTesting
  XMLReaderConfig getConfig() {
    return config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    config.validateConfig();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(DEFAULT_XML_SCHEMA);
    pipelineConfigurer.createDataset(config.tableName, KeyValueTable.class.getName());
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();
    conf.set(XMLInputFormat.XML_INPUTFORMAT_PATH_NAME, config.path);
    conf.set(XMLInputFormat.XML_INPUTFORMAT_NODE_PATH, config.nodePath);
    if (StringUtils.isNotEmpty(config.pattern)) {
      conf.set(XMLInputFormat.XML_INPUTFORMAT_PATTERN, config.pattern);
    }
    setFileTrackingInfo(context, conf);

    //create a temporary directory, in which XMLRecordReader will add file tracking information.
    fileSystem = FileSystem.get(conf);
    long startTime = context.getLogicalStartTime();
    //create temp file name using start time to make it unique.
    String tempDirectory = "/tmp/" + config.tableName + startTime;
    tempDirectoryPath = new Path(tempDirectory);
    fileSystem.mkdirs(tempDirectoryPath);
    fileSystem.deleteOnExit(tempDirectoryPath);
    conf.set(XMLInputFormat.XML_INPUTFORMAT_PROCESSED_DATA_TEMP_FOLDER, tempDirectoryPath.toUri().toString());

    XMLInputFormat.addInputPaths(job, config.path);
    XMLInputFormat.setInputPathFilter(job, BatchXMLFileFilter.class);
    context.setInput(Input.of(config.referenceName, new SourceInputFormatProvider(XMLInputFormat.class, conf)));
  }

  /**
   * Method to set file tracking information in to configuration.
   */
  private void setFileTrackingInfo(BatchSourceContext context, Configuration conf) {
    //For reprocessing not required, set processed file name to configuration.
    processedFileTrackingTable = context.getDataset(config.tableName);
    if (processedFileTrackingTable != null && !config.isReprocessingRequired()) {
      List<String> processedFiles = new ArrayList<String>();
      Calendar cal = Calendar.getInstance();
      cal.add(Calendar.DATE, -Integer.valueOf(config.tableExpiryPeriod));
      Date expiryDate = cal.getTime();

      try (CloseableIterator<KeyValue<byte[], byte[]>> iterator = processedFileTrackingTable.scan(null, null)) {
        while (iterator.hasNext()) {
          KeyValue<byte[], byte[]> keyValue = iterator.next();
          //delete record before expiry time period
          Long time = Bytes.toLong(keyValue.getValue());
          Date processedDate = new Date(time);
          if (processedDate.before(expiryDate)) {
            processedFileTrackingTable.delete(keyValue.getKey());
          } else {
            processedFiles.add(Bytes.toString(keyValue.getKey()));
          }
        }
      }
      //File name use by BatchXMLFileFilter to filter already processed files.
      conf.set(XMLInputFormat.XML_INPUTFORMAT_PROCESSED_FILES,
               GSON.toJson(processedFiles, ARRAYLIST_PREPROCESSED_FILES));
    }
  }

  @Override
  public void transform(KeyValue<LongWritable, Object> input, Emitter<StructuredRecord> emitter) throws Exception {
    Map<String, String> xmlRecord = (Map<String, String>) input.getValue();
    Set<String> keySet = xmlRecord.keySet();
    Iterator<String>  itr = keySet.iterator();
    String fileName = Iterators.getOnlyElement(itr);
    String record = xmlRecord.get(fileName);

    StructuredRecord output = StructuredRecord.builder(DEFAULT_XML_SCHEMA)
      .set("offset", input.getKey().get())
      .set("filename", fileName)
      .set("record", record)
      .build();
    emitter.emit(output);
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSourceContext context) {
    super.onRunFinish(succeeded, context);
    try {
      FileStatus[] status = fileSystem.listStatus(tempDirectoryPath);
      long processingTime = new Date().getTime();
      Path[] paths = FileUtil.stat2Paths(status);
      if (paths != null && paths.length > 0) {
        for (Path path : paths) {
          try (FSDataInputStream input = fileSystem.open(path)) {
            String key = input.readUTF();
            processedFileTrackingTable.write(Bytes.toBytes(key), Bytes.toBytes(processingTime));
          }
        }
      }
    } catch (IOException exception) {
      LOG.error("IOException occurred while reading temp directory path : " + exception.getMessage());
    }
  }

  /**
   * Config class that contains all the properties needed for the XML Reader.
   */
  public static class XMLReaderConfig extends ReferencePluginConfig {
    @Description("Path to file(s) to be read. If a directory is specified, terminate the path name with a \'/\'.")
    private final String path;

    @Nullable
    @Description("Pattern to select specific file(s)." +
      "Example - " +
      "1. Use '^' to select file with name start with 'catalog', like '^catalog'." +
      "2. Use '$' to select file with name end with 'catalog.xml', like 'catalog.xml$'." +
      "3. Use '*' to select file with name contains 'catalogBook', like 'catalogBook*'.")
    private final String pattern;

    @Description("Node path to emit individual event from the schema. " +
      "Example - '/book/price' to read only price under the book node")
    private final String nodePath;

    @Description("Specifies whether the file(s) should be preprocessed.")
    private final String reprocessingRequired;

    @Description("Name of the table to keep track of processed file(s).")
    private final String tableName;

    @Description("Expiry period (days) for data in the table. Default is 30 days." +
      "Example - For tableExpiryPeriod = 30, data before 30 days get deleted from the table.")
    private final String tableExpiryPeriod;

    @VisibleForTesting
    XMLReaderConfig(String referenceName, String path, @Nullable String pattern,
                           @Nullable String nodePath, String reprocessingRequired, String tableName,
                           String tableExpiryPeriod) {
      super(referenceName);
      this.path = path;
      this.pattern = pattern;
      this.nodePath = nodePath;
      this.reprocessingRequired = reprocessingRequired;
      this.tableName = tableName;
      this.tableExpiryPeriod = tableExpiryPeriod;
    }

    @VisibleForTesting
    String getTableName() {
      return tableName;
    }

    boolean isReprocessingRequired() {
      return reprocessingRequired.equalsIgnoreCase("YES") ? true : false;
    }

    @VisibleForTesting
    String getPath() {
      return path;
    }

    @VisibleForTesting
    String getNodePath() {
      return nodePath;
    }

    void validateConfig() {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(path), "Path cannot be empty.");
      Preconditions.checkArgument(!Strings.isNullOrEmpty(nodePath), "Node path cannot be empty.");
      Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "Table Name cannot be empty.");
      Preconditions.checkArgument(tableExpiryPeriod != null, "Table expiry period cannot be empty.");
    }
  }
}
