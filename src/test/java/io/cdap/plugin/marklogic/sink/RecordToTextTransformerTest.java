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

import io.cdap.cdap.api.data.format.StructuredRecord;
import org.apache.hadoop.io.Text;
import org.json.JSONObject;
import org.junit.Test;
import org.w3c.dom.Document;

/**
 * Test class for {@link RecordToTextTransformer}.
 */
public class RecordToTextTransformerTest {
  public static final int USER_ID = 100;

  @Test
  public void testTransformXml() throws Exception {
    RecordToTextTransformer transformer = new RecordToTextTransformer(MarkLogicSinkConfig.Format.XML, null);
    StructuredRecord record = MarkLogicSinkTestDataHelper.getRecord(USER_ID);

    Text text = transformer.transform(record);
    Document document = MarkLogicSinkTestDataHelper.loadXMLFromString(text.toString());

    MarkLogicSinkTestDataHelper.assertXMLDocument(record, document);
  }

  @Test
  public void testTransformJson() throws Exception {
    RecordToTextTransformer transformer = new RecordToTextTransformer(MarkLogicSinkConfig.Format.JSON, null);
    StructuredRecord record = MarkLogicSinkTestDataHelper.getRecord(USER_ID);

    Text text = transformer.transform(record);
    JSONObject object = MarkLogicSinkTestDataHelper.loadJSONFromString(text.toString());

    MarkLogicSinkTestDataHelper.assertJSONObject(record, object);
  }

  @Test
  public void testTransformDelimited() throws Exception {
    RecordToTextTransformer transformer = new RecordToTextTransformer(
      MarkLogicSinkConfig.Format.DELIMITED,
      MarkLogicSinkTestDataHelper.DELIMITER
    );

    StructuredRecord record = MarkLogicSinkTestDataHelper.getRecord(USER_ID);
    Text text = transformer.transform(record);
    MarkLogicSinkTestDataHelper.assertDelimitedString(record, text.toString());
  }
}
