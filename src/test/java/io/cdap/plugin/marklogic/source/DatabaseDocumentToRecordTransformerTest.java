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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.marklogic.BaseBatchMarkLogicConfig;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * Test class for {@link DatabaseDocumentToRecordTransformer}.
 */
public class DatabaseDocumentToRecordTransformerTest {

  @Test
  public void testTransformXml() throws IOException {
    String file = "test.xml";

    String path = DatabaseDocumentToRecordTransformerTest.class
      .getClassLoader()
      .getResource(file)
      .getFile();

    DatabaseDocumentToRecordTransformer transformer = new DatabaseDocumentToRecordTransformer(
      MarkLogicSourceTestDataHelper.SCHEMA_WITH_BINARY_FIELD, BaseBatchMarkLogicConfig.Format.XML, null,
      MarkLogicSourceTestDataHelper.FILE_NAME_FIELD, null
    );

    String xml = new String(Files.readAllBytes(Paths.get(path)));
    List<StructuredRecord> outputRecords = transformer.transformXml(file, xml);

    Assert.assertEquals(2, outputRecords.size());
    String userid = outputRecords.get(0).get("string_field");
    StructuredRecord row1 = "user1".equals(userid) ? outputRecords.get(0) : outputRecords.get(1);
    StructuredRecord row2 = "user1".equals(userid) ? outputRecords.get(1) : outputRecords.get(0);

    // Verify data
    MarkLogicSourceTestDataHelper.assertStructuredRecordWithBinaryField(file, 1, row1);
    MarkLogicSourceTestDataHelper.assertStructuredRecordWithBinaryField(file, 2, row2);
  }

  @Test
  public void testTransformJson() throws IOException {
    String file = "test.json";

    String path = DatabaseDocumentToRecordTransformerTest.class
      .getClassLoader()
      .getResource(file)
      .getFile();

    DatabaseDocumentToRecordTransformer transformer = new DatabaseDocumentToRecordTransformer(
      MarkLogicSourceTestDataHelper.SCHEMA_WITH_BINARY_FIELD, BaseBatchMarkLogicConfig.Format.JSON, null,
      MarkLogicSourceTestDataHelper.FILE_NAME_FIELD, null
    );

    String json = new String(Files.readAllBytes(Paths.get(path)));
    List<StructuredRecord> outputRecords = transformer.transformJson(file, json);

    Assert.assertEquals(2, outputRecords.size());
    String userid = outputRecords.get(0).get("string_field");
    StructuredRecord row1 = "user1".equals(userid) ? outputRecords.get(0) : outputRecords.get(1);
    StructuredRecord row2 = "user1".equals(userid) ? outputRecords.get(1) : outputRecords.get(0);

    // Verify data
    MarkLogicSourceTestDataHelper.assertStructuredRecordWithBinaryField(file, 1, row1);
    MarkLogicSourceTestDataHelper.assertStructuredRecordWithBinaryField(file, 2, row2);
  }

  @Test
  public void testTransformDelimited() throws IOException {
    String file = "test.txt";

    String path = DatabaseDocumentToRecordTransformerTest.class
      .getClassLoader()
      .getResource(file)
      .getFile();

    DatabaseDocumentToRecordTransformer transformer = new DatabaseDocumentToRecordTransformer(
      MarkLogicSourceTestDataHelper.SCHEMA, BaseBatchMarkLogicConfig.Format.DELIMITED,
      MarkLogicSourceTestDataHelper.DELIMITER, MarkLogicSourceTestDataHelper.FILE_NAME_FIELD, null
    );

    String delimited = new String(Files.readAllBytes(Paths.get(path)));
    List<StructuredRecord> outputRecords = transformer.transformDelimited(file, delimited);

    Assert.assertEquals(2, outputRecords.size());
    String userid = outputRecords.get(0).get("string_field");
    StructuredRecord row1 = "user1".equals(userid) ? outputRecords.get(0) : outputRecords.get(1);
    StructuredRecord row2 = "user1".equals(userid) ? outputRecords.get(1) : outputRecords.get(0);

    // Verify data
    MarkLogicSourceTestDataHelper.assertStructuredRecordWithoutBinaryField(file, 1, row1);
    MarkLogicSourceTestDataHelper.assertStructuredRecordWithoutBinaryField(file, 2, row2);
  }
}
