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

package io.cdap.plugin.marklogic.action;

import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.marklogic.ValidationAssertions;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for {@link MarkLogicActionConfig}.
 */
public class MarkLogicActionConfigTest {

  private static final String MOCK_STAGE = "mockStage";

  private static final MarkLogicActionConfig VALID_CONFIG = new MarkLogicActionConfig(
    "localhost",
    8002,
    "mydb",
    "user",
    "password",
    "DIGEST",
    "DIRECT",
    "xquery version \"1.0-ml\";\nxdmp:directory(\"/example/\")"
  );

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testEmptyHost() {
    MarkLogicActionConfig config = MarkLogicActionConfig.builder(VALID_CONFIG)
      .setHost("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, MarkLogicActionConfig.HOST);
  }

  @Test
  public void testEmptyUser() {
    MarkLogicActionConfig config = MarkLogicActionConfig.builder(VALID_CONFIG)
      .setUser("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, MarkLogicActionConfig.USER);
  }

  @Test
  public void testEmptyPassword() {
    MarkLogicActionConfig config = MarkLogicActionConfig.builder(VALID_CONFIG)
      .setPassword("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, MarkLogicActionConfig.PASSWORD);
  }

  @Test
  public void testInvalidAuthenticationType() {
    MarkLogicActionConfig config = MarkLogicActionConfig.builder(VALID_CONFIG)
      .setAuthenticationType("123")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, MarkLogicActionConfig.AUTHENTICATION_TYPE);
  }

  @Test
  public void testInvalidConnectionType() {
    MarkLogicActionConfig config = MarkLogicActionConfig.builder(VALID_CONFIG)
      .setConnectionType("123")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, MarkLogicActionConfig.CONNECTION_TYPE);
  }

  @Test
  public void testEmptyQuery() {
    MarkLogicActionConfig config = MarkLogicActionConfig.builder(VALID_CONFIG)
      .setQuery("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, MarkLogicActionConfig.QUERY);
  }
}
