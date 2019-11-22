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

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;

import java.nio.ByteBuffer;

public final class MarkLogicSourceTestDataHelper {
  public static final String FILE_NAME_FIELD = "file";
  public static final String DELIMITER = ";";

  public static final Schema SCHEMA_WITH_BINARY_FIELD = Schema.recordOf(
    "output",
    Schema.Field.of("file", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("boolean_field", Schema.of(Schema.Type.BOOLEAN)),
    Schema.Field.of("int_field", Schema.of(Schema.Type.INT)),
    Schema.Field.of("long_field", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("float_field", Schema.of(Schema.Type.FLOAT)),
    Schema.Field.of("double_field", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("binary_field", Schema.of(Schema.Type.BYTES))
  );

  public static final Schema SCHEMA = Schema.recordOf(
    "output",
    Schema.Field.of("file", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("boolean_field", Schema.of(Schema.Type.BOOLEAN)),
    Schema.Field.of("int_field", Schema.of(Schema.Type.INT)),
    Schema.Field.of("long_field", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("float_field", Schema.of(Schema.Type.FLOAT)),
    Schema.Field.of("double_field", Schema.of(Schema.Type.DOUBLE))
  );

  public static void assertStructuredRecordWithoutBinaryField(String fileName, int userId, StructuredRecord record) {
    String userName = "user" + userId;
    assertStructuredRecord(fileName, userName, userId, record);
  }

  public static void assertStructuredRecordWithBinaryField(String fileName, int userId, StructuredRecord record) {
    String userName = "user" + userId;
    assertStructuredRecord(fileName, userName, userId, record);
    Assert.assertEquals(userName, Bytes.toString(((ByteBuffer) record.get("binary_field")).array(), 0, 5));
  }

  private static void assertStructuredRecord(String fileName, String userName, int userId, StructuredRecord record) {
    Assert.assertEquals(userName, record.get("string_field"));
    Assert.assertEquals(userId % 2 == 0, record.get("boolean_field"));
    Assert.assertEquals(userId, (int) record.get("int_field"));
    Assert.assertEquals(userId, (long) record.get("long_field"));
    Assert.assertEquals(userId + 0.1 * userId, (float) record.get("float_field"), 0.00001);
    Assert.assertEquals(userId + 0.1 * userId, record.get("double_field"), 0.00001);
    Assert.assertEquals(fileName, record.get("file"));
  }
}
