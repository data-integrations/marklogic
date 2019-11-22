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

package io.cdap.plugin.marklogic;

import com.google.common.base.Strings;
import com.marklogic.client.DatabaseClient;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

import javax.annotation.Nullable;

/**
 * Defines a base that MarkLogic config.
 */
public abstract class BaseMarkLogicConfig extends PluginConfig {
  public static final String HOST = "host";
  public static final String PORT = "port";
  public static final String DATABASE = "database";
  public static final String USER = "user";
  public static final String PASSWORD = "password";
  public static final String AUTHENTICATION_TYPE = "authenticationType";
  public static final String CONNECTION_TYPE = "connectionType";

  @Name(HOST)
  @Macro
  @Description("The host running the MarkLogic Server")
  private final String host;

  @Name(PORT)
  @Description("The port that the Marklogic Server listens on")
  private final Integer port;

  @Name(DATABASE)
  @Macro
  @Nullable
  @Description("Database to connect to")
  private final String database;

  @Name(USER)
  @Macro
  @Description("The user to perform operations as. The user should have appropriate read privileges")
  private final String user;

  @Name(PASSWORD)
  @Macro
  @Description("The password for the user")
  private final String password;

  @Name(AUTHENTICATION_TYPE)
  @Description("The type of authentication to use")
  private final String authenticationType;

  @Name(CONNECTION_TYPE)
  @Description("The type of connection to use - Direct or Gateway")
  private final String connectionType;

  public BaseMarkLogicConfig(String host, Integer port, String database, String user,
                             String password, String authenticationType, String connectionType) {
    this.host = host;
    this.port = port;
    this.database = database;
    this.user = user;
    this.password = password;
    this.authenticationType = authenticationType;
    this.connectionType = connectionType;
  }

  public String getHost() {
    return host;
  }

  public Integer getPort() {
    return port;
  }

  public String getDatabase() {
    return database;
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }

  public String getAuthenticationTypeString() {
    return authenticationType;
  }

  public String getConnectionTypeString() {
    return connectionType;
  }

  public AuthenticationType getAuthenticationType() {
    if (authenticationType == null) {
      throw new IllegalStateException("Authentication type cannot be 'null'");
    }

    try {
      return AuthenticationType.valueOf(authenticationType.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException("Unknown authentication type for value: " + authenticationType, e);
    }
  }

  public DatabaseClient.ConnectionType getConnectionType() {
    if (connectionType == null) {
      throw new IllegalStateException("Connection type cannot be 'null'");
    }

    try {
      return DatabaseClient.ConnectionType.valueOf(connectionType.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException("Unknown connection type for value: " + connectionType, e);
    }
  }

  public void validate(FailureCollector collector) {
    if (!containsMacro(HOST) && Strings.isNullOrEmpty(host)) {
      collector.addFailure("Host must be specified.", null).withConfigProperty(HOST);
    }

    if (!containsMacro(USER) && Strings.isNullOrEmpty(user)) {
      collector.addFailure("User must be specified.", null).withConfigProperty(USER);
    }

    if (!containsMacro(PASSWORD) && Strings.isNullOrEmpty(password)) {
      collector.addFailure("Password must be specified.", null).withConfigProperty(PASSWORD);
    }

    try {
      getAuthenticationType();
    } catch (IllegalStateException e) {
      collector.addFailure(e.getMessage(), null)
        .withConfigProperty(AUTHENTICATION_TYPE);
    }

    try {
      getConnectionType();
    } catch (IllegalStateException e) {
      collector.addFailure(e.getMessage(), null)
        .withConfigProperty(CONNECTION_TYPE);
    }
  }

  /**
   * Defines enum with possible authentication type values.
   */
  public enum AuthenticationType {
    DIGEST,
    SSL
  }
}
