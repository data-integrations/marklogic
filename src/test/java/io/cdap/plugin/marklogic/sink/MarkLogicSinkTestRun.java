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

import com.google.common.collect.ImmutableMap;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.DocumentDescriptor;
import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.document.TextDocumentManager;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.DOMHandle;
import com.marklogic.client.io.StringHandle;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.marklogic.BaseBatchMarkLogicConfig;
import io.cdap.plugin.marklogic.BaseMarkLogicTest;
import io.cdap.plugin.marklogic.MarkLogicPluginConstants;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Test class for {@link MarkLogicSink}.
 */
public class MarkLogicSinkTestRun extends BaseMarkLogicTest {
  private static final int DATA_AMOUNT = 5;
  private static final String PATH_PATTERN = "%s%s%s";
  private static final String OUTPUT_FOLDER = "/out/";
  private static final String FILE_NAME_FIELD = "string_field";

  private static final List<StructuredRecord> INPUT_DATA = createInputData();

  @Test
  public void testWriteJsonFormat() throws Exception {
    ETLPlugin sinkConfig = getSinkConfig(BaseBatchMarkLogicConfig.Format.JSON);
    runPipeline("test-json-format", "input-json", sinkConfig);

    DatabaseClient client = getDatabaseClient();
    try {
      JSONDocumentManager docMgr = client.newJSONDocumentManager();
      for (StructuredRecord record : INPUT_DATA) {
        String path = String.format(PATH_PATTERN, OUTPUT_FOLDER, record.get(FILE_NAME_FIELD), ".json");
        DocumentDescriptor descriptor = docMgr.exists(path);
        Assert.assertNotNull(descriptor);
        StringHandle handle = new StringHandle();
        docMgr.read(path, handle);
        JSONObject json = MarkLogicSinkTestDataHelper.loadJSONFromString(handle.get());
        MarkLogicSinkTestDataHelper.assertJSONObject(record, json);
      }
    } finally {
      client.release();
    }
  }

  @Test
  public void testWriteXmlFormat() throws Exception {
    ETLPlugin sinkConfig = getSinkConfig(BaseBatchMarkLogicConfig.Format.XML);
    runPipeline("test-xml-format", "input-xml", sinkConfig);

    DatabaseClient client = getDatabaseClient();
    try {
      XMLDocumentManager docMgr = client.newXMLDocumentManager();
      for (StructuredRecord record : INPUT_DATA) {
        String path = String.format(PATH_PATTERN, OUTPUT_FOLDER, record.get(FILE_NAME_FIELD), ".xml");
        DocumentDescriptor descriptor = docMgr.exists(path);
        Assert.assertNotNull(descriptor);
        DOMHandle handle = new DOMHandle();
        docMgr.read(path, handle);
        MarkLogicSinkTestDataHelper.assertXMLDocument(record, handle.get());
      }
    } finally {
      client.release();
    }
  }

  @Test
  public void testWriteDelimitedFormat() throws Exception {
    ETLPlugin sinkConfig = getSinkConfig(BaseBatchMarkLogicConfig.Format.DELIMITED);
    runPipeline("test-delimited-format", "input-delimited", sinkConfig);

    DatabaseClient client = getDatabaseClient();
    try {
      TextDocumentManager docMgr = client.newTextDocumentManager();
      for (StructuredRecord record : INPUT_DATA) {
        String path = String.format(PATH_PATTERN, OUTPUT_FOLDER, record.get(FILE_NAME_FIELD), ".txt");
        DocumentDescriptor descriptor = docMgr.exists(path);
        Assert.assertNotNull(descriptor);
        StringHandle handle = new StringHandle();
        docMgr.read(path, handle);
        MarkLogicSinkTestDataHelper.assertDelimitedString(record, handle.get());
      }
    } finally {
      client.release();
    }
  }

  private void runPipeline(String appName, String inputDatasetName, ETLPlugin sinkConfig) throws Exception {
    ETLPlugin sourceConfig = MockSource.getPlugin(inputDatasetName, MarkLogicSinkTestDataHelper.SCHEMA);
    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig, DATAPIPELINE_ARTIFACT, appName);

    // Prepare test input data
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    MockSource.writeInput(inputManager, INPUT_DATA);
    runETLOnce(appManager, ImmutableMap.of("logical.start.time", String.valueOf(CURRENT_TS)));
  }

  private ETLPlugin getSinkConfig(MarkLogicSinkConfig.Format format) {
    return new ETLPlugin(
      MarkLogicPluginConstants.PLUGIN_NAME,
      BatchSink.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .putAll(BASE_PROPS)
        .put(MarkLogicSinkConfig.PATH, OUTPUT_FOLDER)
        .put(MarkLogicSinkConfig.DELIMITER, MarkLogicSinkTestDataHelper.DELIMITER)
        .put(MarkLogicSinkConfig.FILE_NAME_FIELD, FILE_NAME_FIELD)
        .put(MarkLogicSinkConfig.FORMAT, format.toString().toLowerCase())
        .put(MarkLogicSinkConfig.BATCH_SIZE, "100")
        .put(Constants.Reference.REFERENCE_NAME, "DBTest")
        .build(),
      null);
  }

  private static List<StructuredRecord> createInputData() {
    return IntStream.rangeClosed(1, DATA_AMOUNT)
      .mapToObj(MarkLogicSinkTestDataHelper::getRecord)
      .collect(Collectors.toList());
  }
}
