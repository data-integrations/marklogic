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
import io.cdap.cdap.format.StructuredRecordStringConverter;
import org.apache.hadoop.io.Text;
import org.json.JSONObject;
import org.json.XML;

import java.io.IOException;

/**
 * Transforms {@link StructuredRecord} to {@link Text}.
 */
public class RecordToTextTransformer {
  private static final String XML_ROOT_PATTERN = "<root>%s</root>";
  private final MarkLogicSinkConfig.Format format;
  private final String delimiter;

  public RecordToTextTransformer(MarkLogicSinkConfig.Format format, String delimiter) {
    this.format = format;
    this.delimiter = delimiter;
  }

  public Text transform(StructuredRecord record) throws IOException {
    String data;

    switch (format) {
      case JSON:
        data = StructuredRecordStringConverter.toJsonString(record);
        break;
      case DELIMITED:
        data = StructuredRecordStringConverter.toDelimitedString(record, delimiter);
        break;
      case XML:
        data = transformXml(record);
        break;
      default:
        throw new IllegalStateException("Unsupported format: " + format);
    }

    return new Text(data);
  }

  private String transformXml(StructuredRecord record) throws IOException {
    String json = StructuredRecordStringConverter.toJsonString(record);
    JSONObject jsonObject = new JSONObject(json);
    String xml = XML.toString(jsonObject);

    return String.format(XML_ROOT_PATTERN, xml);
  }
}
