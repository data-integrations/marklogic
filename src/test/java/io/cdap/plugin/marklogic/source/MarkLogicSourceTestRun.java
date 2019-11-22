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

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.marklogic.BaseBatchMarkLogicConfig;
import io.cdap.plugin.marklogic.BaseMarkLogicTest;
import io.cdap.plugin.marklogic.MarkLogicPluginConstants;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Test class for {@link MarkLogicSource}.
 */
public class MarkLogicSourceTestRun extends BaseMarkLogicTest {
  private static final String BOUNDING_QUERY = "xquery version \"1.0-ml\";" +
    "\nimport module namespace hadoop = \"http://marklogic.com/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";" +
    "\nhadoop:get-splits('', 'fn:doc()', 'cts:and-query(())')";

  @Test
  public void testSourceReadJSONDocument() throws Exception {
    ETLPlugin sourceConfig = getSourcePluginConfig(BaseBatchMarkLogicConfig.Format.JSON, JSON_FOLDER,
                                                   MarkLogicSourceTestDataHelper.SCHEMA_WITH_BINARY_FIELD);
    List<StructuredRecord> outputRecords = runPipeline("testDBSource-json", "output-dbsourcetest-json",
                                                       sourceConfig);

    Assert.assertEquals(2, outputRecords.size());
    String userid = outputRecords.get(0).get("string_field");
    StructuredRecord row1 = "user1".equals(userid) ? outputRecords.get(0) : outputRecords.get(1);
    StructuredRecord row2 = "user1".equals(userid) ? outputRecords.get(1) : outputRecords.get(0);

    // Verify data
    MarkLogicSourceTestDataHelper.assertStructuredRecordWithBinaryField(JSON_DOCUMENT, 1, row1);
    MarkLogicSourceTestDataHelper.assertStructuredRecordWithBinaryField(JSON_DOCUMENT, 2, row2);
  }

  @Test
  public void testSourceReadXMLDocument() throws Exception {
    ETLPlugin sourceConfig = getSourcePluginConfig(BaseBatchMarkLogicConfig.Format.XML, XML_FOLDER,
                                                   MarkLogicSourceTestDataHelper.SCHEMA_WITH_BINARY_FIELD);
    List<StructuredRecord> outputRecords = runPipeline("testDBSource-xml", "output-dbsourcetest-xml",
                                                       sourceConfig);

    Assert.assertEquals(2, outputRecords.size());
    String userid = outputRecords.get(0).get("string_field");
    StructuredRecord row1 = "user1".equals(userid) ? outputRecords.get(0) : outputRecords.get(1);
    StructuredRecord row2 = "user1".equals(userid) ? outputRecords.get(1) : outputRecords.get(0);

    // Verify data
    MarkLogicSourceTestDataHelper.assertStructuredRecordWithBinaryField(XML_DOCUMENT, 1, row1);
    MarkLogicSourceTestDataHelper.assertStructuredRecordWithBinaryField(XML_DOCUMENT, 2, row2);
  }

  @Test
  public void testSourceReadDelimitedDocument() throws Exception {
    ETLPlugin sourceConfig = getSourcePluginConfig(BaseBatchMarkLogicConfig.Format.DELIMITED, DELIMITED_FOLDER,
                                                   MarkLogicSourceTestDataHelper.SCHEMA);
    List<StructuredRecord> outputRecords = runPipeline("testDBSource-delimited", "output-dbsourcetest-delimited",
                                                       sourceConfig);

    Assert.assertEquals(2, outputRecords.size());
    String userid = outputRecords.get(0).get("string_field");
    StructuredRecord row1 = "user1".equals(userid) ? outputRecords.get(0) : outputRecords.get(1);
    StructuredRecord row2 = "user1".equals(userid) ? outputRecords.get(1) : outputRecords.get(0);

    // Verify data
    MarkLogicSourceTestDataHelper.assertStructuredRecordWithoutBinaryField(DELIMITED_DOCUMENT, 1, row1);
    MarkLogicSourceTestDataHelper.assertStructuredRecordWithoutBinaryField(DELIMITED_DOCUMENT, 2, row2);
  }

  private List<StructuredRecord> runPipeline(String appName, String dataSetName, ETLPlugin sourceConfig)
    throws Exception {
    ETLPlugin sinkConfig = MockSink.getPlugin(dataSetName);
    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig, DATAPIPELINE_ARTIFACT, appName);

    runETLOnce(appManager);

    DataSetManager<Table> outputManager = getDataset(dataSetName);
    return MockSink.readOutput(outputManager);
  }

  private ETLPlugin getSourcePluginConfig(MarkLogicSourceConfig.Format format, String folderPath, Schema schema) {
    return new ETLPlugin(
      MarkLogicPluginConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .putAll(BASE_PROPS)
        .put(Constants.Reference.REFERENCE_NAME, "DBSourceTest")
        .put(MarkLogicSourceConfig.INPUT_METHOD, MarkLogicSourceConfig.InputMethod.PATH.toString().toLowerCase())
        .put(MarkLogicSourceConfig.PATH, folderPath)
        .put(MarkLogicSourceConfig.BOUNDING_QUERY, BOUNDING_QUERY)
        .put(MarkLogicSourceConfig.FORMAT, format.toString().toLowerCase())
        .put(MarkLogicSourceConfig.DELIMITER, MarkLogicSourceTestDataHelper.DELIMITER)
        .put(MarkLogicSourceConfig.FILE_FIELD, MarkLogicSourceTestDataHelper.FILE_NAME_FIELD)
        .put(MarkLogicSourceConfig.SCHEMA, schema.toString())
        .build(),
      null
    );
  }
}
