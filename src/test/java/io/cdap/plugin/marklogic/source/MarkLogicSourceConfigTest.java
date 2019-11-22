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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.marklogic.BaseBatchMarkLogicConfig;
import io.cdap.plugin.marklogic.ValidationAssertions;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for {@link MarkLogicSourceConfig}.
 */
public class MarkLogicSourceConfigTest {

  private static final String MOCK_STAGE = "mockStage";
  private static final Schema DEFAULT_OUTPUT_SCHEMA = Schema.recordOf(
    "output",
    Schema.Field.of("file", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("payload", Schema.of(Schema.Type.BYTES))
  );

  private static final MarkLogicSourceConfig VALID_CONFIG = new MarkLogicSourceConfig(
    "ref",
    MarkLogicSourceConfig.InputMethod.QUERY.toString().toLowerCase(),
    "localhost",
    8002,
    "mydb",
    "user",
    "password",
    "DIGEST",
    "DIRECT",
    "xquery version \"1.0-ml\";\nxdmp:directory(\"/example/\")",
    "blob",
    "",
    DEFAULT_OUTPUT_SCHEMA.toString(),
    "xquery version \"1.0-ml\";\n" +
      "import module namespace hadoop = \"http://marklogic.com/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";\n" +
      "hadoop:get-splits('', 'fn:doc()', 'cts:and-query(())')",
    10,
    "file",
    "payload",
    ""
  );

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testReferenceNameMethod() {
    MarkLogicSourceConfig config = MarkLogicSourceConfig.builder(VALID_CONFIG)
      .setReferenceName("@")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, Constants.Reference.REFERENCE_NAME);
  }

  @Test
  public void testEmptyHost() {
    MarkLogicSourceConfig config = MarkLogicSourceConfig.builder(VALID_CONFIG)
      .setHost("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, MarkLogicSourceConfig.HOST);
  }

  @Test
  public void testEmptyUser() {
    MarkLogicSourceConfig config = MarkLogicSourceConfig.builder(VALID_CONFIG)
      .setUser("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, MarkLogicSourceConfig.USER);
  }

  @Test
  public void testEmptyPassword() {
    MarkLogicSourceConfig config = MarkLogicSourceConfig.builder(VALID_CONFIG)
      .setPassword("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, MarkLogicSourceConfig.PASSWORD);
  }

  @Test
  public void testInvalidAuthenticationType() {
    MarkLogicSourceConfig config = MarkLogicSourceConfig.builder(VALID_CONFIG)
      .setAuthenticationType("123")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, MarkLogicSourceConfig.AUTHENTICATION_TYPE);
  }

  @Test
  public void testInvalidConnectionType() {
    MarkLogicSourceConfig config = MarkLogicSourceConfig.builder(VALID_CONFIG)
      .setConnectionType("123")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, MarkLogicSourceConfig.CONNECTION_TYPE);
  }

  @Test
  public void testInvalidFormat() {
    MarkLogicSourceConfig config = MarkLogicSourceConfig.builder(VALID_CONFIG)
      .setFormat("123")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, MarkLogicSourceConfig.FORMAT);
  }

  @Test
  public void testEmptyDelimiterWithDelimitedFormat() {
    MarkLogicSourceConfig config = MarkLogicSourceConfig.builder(VALID_CONFIG)
      .setFormat(BaseBatchMarkLogicConfig.Format.DELIMITED.toString().toLowerCase())
      .setDelimiter("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, MarkLogicSourceConfig.DELIMITER);
  }

  @Test
  public void testInvalidInputMethod() {
    MarkLogicSourceConfig config = MarkLogicSourceConfig.builder(VALID_CONFIG)
      .setInputMethod("123")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, MarkLogicSourceConfig.INPUT_METHOD);
  }

  @Test
  public void testEmptyBoundingQueryWithNotEmptyQuery() {
    MarkLogicSourceConfig config = MarkLogicSourceConfig.builder(VALID_CONFIG)
      .setBoundingQuery("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, MarkLogicSourceConfig.BOUNDING_QUERY);
  }

  @Test
  public void testEmptyBoundingQueryWithNotEmptyPath() {
    MarkLogicSourceConfig config = MarkLogicSourceConfig.builder(VALID_CONFIG)
      .setInputMethod(MarkLogicSourceConfig.InputMethod.PATH.toString().toLowerCase())
      .setPath("/example/")
      .setBoundingQuery("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, MarkLogicSourceConfig.BOUNDING_QUERY);
  }

  @Test
  public void testInvalidSchema() {
    MarkLogicSourceConfig config = MarkLogicSourceConfig.builder(VALID_CONFIG)
      .setSchema("123")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, MarkLogicSourceConfig.SCHEMA);
  }

  @Test
  public void testNotExistingFileFieldInSchema() {
    MarkLogicSourceConfig config = MarkLogicSourceConfig.builder(VALID_CONFIG)
      .setFileField("123")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, MarkLogicSourceConfig.SCHEMA);
  }

  @Test
  public void testInvalidFileFieldFormatInSchema() {
    String fileFieldName = "fileField";

    Schema invalidSchema = Schema.recordOf(
      "output",
      Schema.Field.of(fileFieldName, Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("payload", Schema.of(Schema.Type.BYTES))
    );

    MarkLogicSourceConfig config = MarkLogicSourceConfig.builder(VALID_CONFIG)
      .setFileField(fileFieldName)
      .setSchema(invalidSchema.toString())
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertOutputSchemaFieldFailed(failureCollector, fileFieldName);
  }

  @Test
  public void testNotExistingPayloadFieldInSchema() {
    MarkLogicSourceConfig config = MarkLogicSourceConfig.builder(VALID_CONFIG)
      .setPayloadField("123")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, MarkLogicSourceConfig.SCHEMA);
  }

  @Test
  public void testInvalidPayloadFormatInSchemaForBlob() {
    String payloadFieldName = "payloadField";

    Schema invalidSchema = Schema.recordOf(
      "output",
      Schema.Field.of("file", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(payloadFieldName, Schema.of(Schema.Type.STRING))
    );

    MarkLogicSourceConfig config = MarkLogicSourceConfig.builder(VALID_CONFIG)
      .setFormat(BaseBatchMarkLogicConfig.Format.BLOB.toString().toLowerCase())
      .setPayloadField(payloadFieldName)
      .setSchema(invalidSchema.toString())
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertOutputSchemaFieldFailed(failureCollector, payloadFieldName);
  }

  @Test
  public void testInvalidPayloadFormatInSchemaForText() {
    String payloadFieldName = "payloadField";

    Schema invalidSchema = Schema.recordOf(
      "output",
      Schema.Field.of("file", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(payloadFieldName, Schema.of(Schema.Type.BYTES))
    );

    MarkLogicSourceConfig config = MarkLogicSourceConfig.builder(VALID_CONFIG)
      .setFormat(BaseBatchMarkLogicConfig.Format.TEXT.toString().toLowerCase())
      .setPayloadField(payloadFieldName)
      .setSchema(invalidSchema.toString())
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertOutputSchemaFieldFailed(failureCollector, payloadFieldName);
  }

  @Test
  public void testInvalidPayloadFormatInSchemaForAuto() {
    String payloadFieldName = "payloadField";

    Schema invalidSchema = Schema.recordOf(
      "output",
      Schema.Field.of("file", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(payloadFieldName, Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );

    MarkLogicSourceConfig config = MarkLogicSourceConfig.builder(VALID_CONFIG)
      .setFormat(BaseBatchMarkLogicConfig.Format.AUTO.toString().toLowerCase())
      .setPayloadField(payloadFieldName)
      .setSchema(invalidSchema.toString())
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertOutputSchemaFieldFailed(failureCollector, payloadFieldName);
  }

  @Test
  public void testNotNullFieldsInSchemaForAuto() {
    String notNullFieldName = "notNullField";

    Schema invalidSchema = Schema.recordOf(
      "output",
      Schema.Field.of("file", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(notNullFieldName, Schema.of(Schema.Type.STRING)),
      Schema.Field.of("payload", Schema.nullableOf(Schema.of(Schema.Type.BYTES)))
    );

    MarkLogicSourceConfig config = MarkLogicSourceConfig.builder(VALID_CONFIG)
      .setFormat(BaseBatchMarkLogicConfig.Format.AUTO.toString().toLowerCase())
      .setSchema(invalidSchema.toString())
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertOutputSchemaFieldFailed(failureCollector, notNullFieldName);
  }
}
