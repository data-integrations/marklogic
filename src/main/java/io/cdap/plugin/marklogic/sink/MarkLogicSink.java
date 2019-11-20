/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.marklogic.sink;

import com.google.common.base.Strings;
import com.marklogic.client.DatabaseClient;
import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicConstants;
import com.marklogic.mapreduce.utilities.TextArrayWritable;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.batch.sink.SinkOutputFormatProvider;
import io.cdap.plugin.marklogic.BaseBatchMarkLogicConfig;
import io.cdap.plugin.marklogic.BaseMarkLogicConfig;
import io.cdap.plugin.marklogic.MarkLogicPluginConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * A {@link BatchSink} that writes data to MarkLogic.
 * This {@link MarkLogicSink} takes a {@link StructuredRecord} in,
 * converts it to {@link Text}, and writes it to MarkLogic.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(MarkLogicPluginConstants.PLUGIN_NAME)
@Description("MarkLogic Batch Sink writes to a MarkLogic file.")
public class MarkLogicSink extends BatchSink<StructuredRecord, DocumentURI, Text> {
  public static final String SCHEMA = "schema";
  private final MarkLogicSinkConfig config;

  private RecordToTextTransformer transformer;
  private String fileNamePattern;

  public MarkLogicSink(MarkLogicSinkConfig config) {
    this.config = config;
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    transformer = new RecordToTextTransformer(config.getFormat(), config.getDelimiter());

    ContentType contentType = getContentType();
    fileNamePattern = getFileNamePattern(contentType);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer configurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = configurer.getFailureCollector();

    config.validate(collector);
    Schema inputSchema = configurer.getInputSchema();
    validateSchema(inputSchema, collector);

    collector.getOrThrowException();
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();

    Configuration conf = new Configuration();
    setConfiguration(conf);

    emitLineage(context);
    context.addOutput(Output.of(config.getReferenceName(),
                                new SinkOutputFormatProvider(ConfiguredContentOutputFormat.class, conf)));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<DocumentURI, Text>> emitter) throws Exception {
    DocumentURI uri = getDocumentPath(input);
    Text data = transformer.transform(input);

    emitter.emit(new KeyValue<>(uri, data));
  }

  private void validateSchema(Schema schema, FailureCollector collector) {
    String fileNameField = config.getFileNameField();
    if (!Strings.isNullOrEmpty(fileNameField) && schema.getField(fileNameField) == null) {
      collector.addFailure(String.format("Schema must contain file name field '%s'.", fileNameField), null)
        .withConfigProperty(SCHEMA);
    }
  }

  private void setConfiguration(Configuration conf) throws IOException {
    Text[] hosts = {new Text(config.getHost())};
    TextArrayWritable hostsArray = new TextArrayWritable(hosts);
    DefaultStringifier.store(conf, hostsArray, MarkLogicConstants.OUTPUT_FOREST_HOST);

    conf.set(MarkLogicConstants.OUTPUT_HOST, config.getHost());
    conf.setInt(MarkLogicConstants.OUTPUT_PORT, config.getPort());
    conf.set(MarkLogicConstants.OUTPUT_USERNAME, config.getUser());
    conf.set(MarkLogicConstants.OUTPUT_PASSWORD, config.getPassword());

    conf.setInt(MarkLogicConstants.BATCH_SIZE, config.getBatchSize());

    ContentType contentType = getContentType();
    conf.set(MarkLogicConstants.CONTENT_TYPE, contentType.toString());

    if (config.getAuthenticationType() == BaseMarkLogicConfig.AuthenticationType.SSL) {
      conf.setBoolean(MarkLogicConstants.OUTPUT_USE_SSL, true);
    }

    if (config.getConnectionType() == DatabaseClient.ConnectionType.DIRECT) {
      conf.setBoolean(MarkLogicConstants.OUTPUT_RESTRICT_HOSTS, true);
    }

    if (!Strings.isNullOrEmpty(config.getDatabase())) {
      conf.set(MarkLogicConstants.OUTPUT_DATABASE_NAME, config.getDatabase());
    }
  }

  private void emitLineage(BatchSinkContext context) {
    if (Objects.nonNull(context.getInputSchema())) {
      LineageRecorder lineageRecorder = new LineageRecorder(context, config.getReferenceName());
      lineageRecorder.createExternalDataset(context.getInputSchema());
      List<Schema.Field> fields = context.getInputSchema().getFields();
      if (fields != null && !fields.isEmpty()) {
        lineageRecorder.recordWrite("Write",
                                    String.format("Wrote to '%s' MarkLogic path.", config.getPath()),
                                    fields.stream().map(Schema.Field::getName).collect(Collectors.toList()));
      }
    }
  }

  private ContentType getContentType() {
    BaseBatchMarkLogicConfig.Format format = config.getFormat();
    switch (format) {
      case DELIMITED:
        return ContentType.TEXT;
      case JSON:
        return ContentType.JSON;
      case XML:
        return ContentType.XML;
      default:
        throw new IllegalStateException("Unsupported format: " + format);
    }
  }

  private DocumentURI getDocumentPath(StructuredRecord record) {
    String fileNameField = config.getFileNameField();

    String fileName;
    if (fileNameField != null) {
      fileName = record.get(fileNameField).toString();
    } else {
      fileName = UUID.randomUUID().toString();
    }

    String path = String.format(fileNamePattern, fileName);
    return new DocumentURI(path);
  }

  private String getFileNamePattern(ContentType contentType) {
    String fileExtension;
    switch (contentType) {
      case XML:
        fileExtension = ".xml";
        break;
      case JSON:
        fileExtension = ".json";
        break;
      case TEXT:
        fileExtension = ".txt";
        break;
      default:
        throw new IllegalArgumentException("Unsupported content type: " + contentType);
    }

    String path = config.getPath();
    if (!path.endsWith("/")) {
      path += "/";
    }

    return path + "%s" + fileExtension;
  }
}
