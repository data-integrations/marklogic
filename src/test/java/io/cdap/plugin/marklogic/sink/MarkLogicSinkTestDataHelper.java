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
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.ByteArrayInputStream;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

public final class MarkLogicSinkTestDataHelper {
  public static final String DELIMITER = ";";

  public static final Schema SCHEMA = Schema.recordOf(
    "dbRecord",
    Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("boolean_field", Schema.of(Schema.Type.BOOLEAN)),
    Schema.Field.of("int_field", Schema.of(Schema.Type.INT)),
    Schema.Field.of("long_field", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("float_field", Schema.of(Schema.Type.FLOAT)),
    Schema.Field.of("double_field", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("nullable_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("binary_field", Schema.of(Schema.Type.BYTES))
  );

  public static StructuredRecord getRecord(int id) {
    String name = "user" + id;

    StructuredRecord.Builder builder = StructuredRecord.builder(SCHEMA)
      .set("int_field", id)
      .set("string_field", name)
      .set("binary_field", name.getBytes())
      .set("boolean_field", (id % 2 == 0))
      .set("long_field", 3456987L + id)
      .set("float_field", 3.45f + id)
      .set("double_field", 5.78951d + id);

    return builder.build();
  }

  public static Document loadXMLFromString(String xml) throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

    factory.setNamespaceAware(true);
    DocumentBuilder builder = factory.newDocumentBuilder();

    return builder.parse(new ByteArrayInputStream(xml.getBytes()));
  }

  public static JSONObject loadJSONFromString(String json) throws JSONException {
    return new JSONObject(json);
  }

  public static void assertXMLDocument(StructuredRecord record, Document document) {
    NodeList nodeList = document.getChildNodes();
    Assert.assertEquals(1, nodeList.getLength());

    Node node = nodeList.item(0);
    Assert.assertEquals("root", node.getNodeName());

    Assert.assertEquals((int) record.get("int_field"), Integer.parseInt(getTextContent(document, "int_field")));
    Assert.assertEquals(record.get("string_field"), getTextContent(document, "string_field"));
    Assert.assertEquals(record.get("boolean_field"), Boolean.valueOf(getTextContent(document, "boolean_field")));
    Assert.assertEquals((double) record.get("double_field"),
                        Double.parseDouble(getTextContent(document, "double_field")), 0.000001);
    Assert.assertEquals((float) record.get("float_field"), Float.parseFloat(getTextContent(document, "float_field")),
                        0.000001);
    Assert.assertEquals((long) record.get("long_field"), Long.parseLong(getTextContent(document, "long_field")));
    Assert.assertEquals(new String((byte[]) record.get("binary_field")),
                        new String(getByteContent(document, "binary_field")));
    Assert.assertEquals("null", getTextContent(document, "nullable_field"));
  }

  public static void assertJSONObject(StructuredRecord record, JSONObject json) throws JSONException {
    Assert.assertEquals(record.get("int_field"), json.get("int_field"));
    Assert.assertEquals(record.get("string_field"), json.get("string_field"));
    Assert.assertEquals(record.get("boolean_field"), json.get("boolean_field"));
    Assert.assertEquals((long) record.get("long_field"), ((Integer) json.get("long_field")).longValue());
    Assert.assertEquals((Float) record.get("float_field"), ((Double) json.get("float_field")).floatValue(), 0.000001);
    Assert.assertEquals((double) record.get("double_field"), (double) json.get("double_field"), 0.000001);
    Assert.assertEquals(new String((byte[]) record.get("binary_field")),
                        new String(getByteContent(json, "binary_field")));
    Assert.assertEquals(JSONObject.NULL, json.get("nullable_field"));
  }

  public static void assertDelimitedString(StructuredRecord record, String line) {
    String[] values = line.split(DELIMITER);

    Assert.assertEquals(record.get("string_field"), values[0]);
    Assert.assertEquals(record.get("boolean_field"), Boolean.valueOf(values[1]));
    Assert.assertEquals(record.get("int_field"), Integer.valueOf(values[2]));
    Assert.assertEquals(record.get("long_field"), Long.valueOf(values[3]));
    Assert.assertEquals(record.get("float_field"), Float.valueOf(values[4]));
    Assert.assertEquals(record.get("double_field"), Double.valueOf(values[5]));
    Assert.assertTrue(Strings.isNullOrEmpty(values[6]));
  }

  private static String getTextContent(Document document, String field) {
    return document.getElementsByTagName(field).item(0).getTextContent();
  }

  private static byte[] getByteContent(JSONObject json, String field) throws JSONException {
    JSONArray array = json.getJSONArray(field);
    int size = array.length();

    byte[] bytes = new byte[size];
    for (int i = 0; i < size; i++) {
      bytes[i] = new Byte(array.getString(i));
    }

    return bytes;
  }

  private static byte[] getByteContent(Document document, String field) {
    NodeList nodeList = document.getElementsByTagName(field);
    int size = nodeList.getLength();

    byte[] bytes = new byte[size];
    for (int i = 0; i < size; i++) {
      bytes[i] = new Byte(nodeList.item(i).getTextContent());
    }

    return bytes;
  }
}
