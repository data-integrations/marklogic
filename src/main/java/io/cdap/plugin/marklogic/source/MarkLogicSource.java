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

package io.cdap.plugin.marklogic.source;

import com.google.common.base.Strings;
import com.marklogic.client.DatabaseClient;
import com.marklogic.mapreduce.DatabaseDocument;
import com.marklogic.mapreduce.DocumentInputFormat;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicConstants;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.marklogic.BaseMarkLogicConfig;
import io.cdap.plugin.marklogic.MarkLogicPluginConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A {@link BatchSource} that reads data from MarkLogic and converts each document into
 * a {@link StructuredRecord} using the specified Schema.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(MarkLogicPluginConstants.PLUGIN_NAME)
@Description("MarkLogic Batch Source will read documents from MarkLogic and convert each document " +
  "into a StructuredRecord with the help of the specified Schema. ")
public class MarkLogicSource extends BatchSource<DocumentURI, DatabaseDocument, StructuredRecord> {
  private static final String PATH_QUERY_TEMPLATE = "xquery version \"1.0-ml\";\nxdmp:directory(\"%s\")";

  private final MarkLogicSourceConfig config;
  private DatabaseDocumentToRecordTransformer transformer;

  public MarkLogicSource(MarkLogicSourceConfig config) {
    this.config = config;
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    transformer = new DatabaseDocumentToRecordTransformer(
      config.getParsedSchema(),
      config.getFormat(),
      config.getDelimiter(),
      config.getFileField(),
      config.getPayloadField()
    );
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();

    config.validate(collector);
    collector.getOrThrowException();

    stageConfigurer.setOutputSchema(config.getParsedSchema());
  }

  @Override
  public void prepareRun(BatchSourceContext context) {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();

    Configuration conf = new Configuration();
    setConfiguration(conf);

    emitLineage(context);
    context.setInput(Input.of(config.getReferenceName(),
                              new SourceInputFormatProvider(DocumentInputFormat.class, conf)));
  }

  @Override
  public void transform(KeyValue<DocumentURI, DatabaseDocument> input, Emitter<StructuredRecord> emitter) {
    List<StructuredRecord> records = transformer.transform(input.getKey().getUri(), input.getValue());
    records.forEach(emitter::emit);
  }

  private void setConfiguration(Configuration conf) {
    conf.set(MarkLogicConstants.INPUT_HOST, config.getHost());
    conf.setInt(MarkLogicConstants.INPUT_PORT, config.getPort());
    conf.set(MarkLogicConstants.INPUT_USERNAME, config.getUser());
    conf.set(MarkLogicConstants.INPUT_PASSWORD, config.getPassword());

    if (!Strings.isNullOrEmpty(config.getDatabase())) {
      conf.set(MarkLogicConstants.INPUT_DATABASE_NAME, config.getDatabase());
    }

    if (config.getAuthenticationType() == BaseMarkLogicConfig.AuthenticationType.SSL) {
      conf.setBoolean(MarkLogicConstants.INPUT_USE_SSL, true);
    }

    if (config.getConnectionType() == DatabaseClient.ConnectionType.DIRECT) {
      conf.setBoolean(MarkLogicConstants.INPUT_RESTRICT_HOSTS, true);
    }

    conf.setClass(MarkLogicPluginConstants.INPUT_FORMAT_CLASS_ATTR, DocumentInputFormat.class, InputFormat.class);

    MarkLogicSourceConfig.InputMethod method = config.getInputMethod();
    if (method != null) {
      if (method.equals(MarkLogicSourceConfig.InputMethod.PATH) && !Strings.isNullOrEmpty(config.getPath())) {
        // Generating query according to path
        conf.set(MarkLogicConstants.INPUT_QUERY, getPathQuery(config.getPath()));
      } else if (method.equals(MarkLogicSourceConfig.InputMethod.QUERY) && !Strings.isNullOrEmpty(config.getQuery())) {
        conf.set(MarkLogicConstants.INPUT_QUERY, config.getQuery());
      } else {
        // If 'Query' or 'Path' is not set, than advanced properties should not be set
        return;
      }

      conf.set(MarkLogicConstants.INPUT_MODE, MarkLogicConstants.ADVANCED_MODE);
      conf.setLong(MarkLogicConstants.MAX_SPLIT_SIZE, config.getMaxSplits() != null ? config.getMaxSplits() :
        MarkLogicConstants.DEFAULT_MAX_SPLIT_SIZE);
      conf.set(MarkLogicConstants.SPLIT_QUERY, config.getBoundingQuery());
    }

  }

  private void emitLineage(BatchSourceContext context) {
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.getReferenceName());
    lineageRecorder.createExternalDataset(config.getParsedSchema());
    List<Schema.Field> fields = Objects.requireNonNull(config.getParsedSchema()).getFields();
    if (fields != null && !fields.isEmpty()) {
      lineageRecorder.recordRead("Read", String.format("Read from '%s' MarkLogic.", config.getHost()),
                                 fields.stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }

  private static String getPathQuery(String path) {
    return String.format(PATH_QUERY_TEMPLATE, path);
  }
}
