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

import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.marklogic.BaseBatchMarkLogicConfig;
import io.cdap.plugin.marklogic.ValidationAssertions;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for {@link MarkLogicSinkConfig}.
 */
public class MarkLogicSinkConfigTest {

  private static final String MOCK_STAGE = "mockStage";

  private static final MarkLogicSinkConfig VALID_CONFIG = new MarkLogicSinkConfig(
    "ref",
    "localhost",
    8002,
    "mydb",
    "user",
    "password",
    "DIGEST",
    "DIRECT",
    MarkLogicSinkConfig.Format.JSON.toString().toLowerCase(),
    "",
    "/",
    "",
    100
  );

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testReferenceNameMethod() {
    MarkLogicSinkConfig config = MarkLogicSinkConfig.builder(VALID_CONFIG)
      .setReferenceName("@")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, Constants.Reference.REFERENCE_NAME);
  }

  @Test
  public void testEmptyHost() {
    MarkLogicSinkConfig config = MarkLogicSinkConfig.builder(VALID_CONFIG)
      .setHost("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, MarkLogicSinkConfig.HOST);
  }

  @Test
  public void testEmptyUser() {
    MarkLogicSinkConfig config = MarkLogicSinkConfig.builder(VALID_CONFIG)
      .setUser("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, MarkLogicSinkConfig.USER);
  }

  @Test
  public void testEmptyPassword() {
    MarkLogicSinkConfig config = MarkLogicSinkConfig.builder(VALID_CONFIG)
      .setPassword("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, MarkLogicSinkConfig.PASSWORD);
  }

  @Test
  public void testInvalidAuthenticationType() {
    MarkLogicSinkConfig config = MarkLogicSinkConfig.builder(VALID_CONFIG)
      .setAuthenticationType("123")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, MarkLogicSinkConfig.AUTHENTICATION_TYPE);
  }

  @Test
  public void testInvalidConnectionType() {
    MarkLogicSinkConfig config = MarkLogicSinkConfig.builder(VALID_CONFIG)
      .setConnectionType("123")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, MarkLogicSinkConfig.CONNECTION_TYPE);
  }

  @Test
  public void testInvalidFormat() {
    MarkLogicSinkConfig config = MarkLogicSinkConfig.builder(VALID_CONFIG)
      .setFormat("123")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, MarkLogicSinkConfig.FORMAT);
  }

  @Test
  public void testEmptyDelimiterWithDelimitedFormat() {
    MarkLogicSinkConfig config = MarkLogicSinkConfig.builder(VALID_CONFIG)
      .setFormat(BaseBatchMarkLogicConfig.Format.DELIMITED.toString().toLowerCase())
      .setDelimiter("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, MarkLogicSinkConfig.DELIMITER);
  }
}
