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

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.marklogic.BaseMarkLogicConfig;

/**
 * Config class for {@link MarkLogicAction}.
 */
public class MarkLogicActionConfig extends BaseMarkLogicConfig {
  public static final String QUERY = "query";

  @Name(QUERY)
  @Macro
  @Description("Query for data search")
  private final String query;

  public MarkLogicActionConfig(String host, Integer port, String database, String user, String password,
                               String authenticationType, String connectionType, String query) {
    super(host, port, database, user, password, authenticationType, connectionType);
    this.query = query;
  }

  private MarkLogicActionConfig(Builder builder) {
    super(
      builder.host,
      builder.port,
      builder.database,
      builder.user,
      builder.password,
      builder.authenticationType,
      builder.connectionType
    );
    query = builder.query;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(MarkLogicActionConfig copy) {
    Builder builder = new Builder();

    builder.setHost(copy.getHost());
    builder.setPort(copy.getPort());
    builder.setDatabase(copy.getDatabase());
    builder.setUser(copy.getUser());
    builder.setPassword(copy.getPassword());
    builder.setAuthenticationType(copy.getAuthenticationTypeString());
    builder.setConnectionType(copy.getConnectionTypeString());
    builder.setQuery(copy.getQuery());

    return builder;
  }

  public String getQuery() {
    return query;
  }

  @Override
  public void validate(FailureCollector collector) {
    super.validate(collector);

    if (!containsMacro(QUERY) && Strings.isNullOrEmpty(query)) {
      collector.addFailure("Query must be specified.", null).withConfigProperty(QUERY);
    }
  }

  /**
   * Builder for creating a {@link MarkLogicActionConfig}.
   */
  public static final class Builder {

    private String host;
    private Integer port;
    private String database;
    private String user;
    private String password;
    private String authenticationType;
    private String connectionType;
    private String query;

    private Builder() {
    }

    public Builder setHost(String host) {
      this.host = host;
      return this;
    }

    public Builder setPort(Integer port) {
      this.port = port;
      return this;
    }

    public Builder setDatabase(String database) {
      this.database = database;
      return this;
    }

    public Builder setUser(String user) {
      this.user = user;
      return this;
    }

    public Builder setPassword(String password) {
      this.password = password;
      return this;
    }

    public Builder setAuthenticationType(String authenticationType) {
      this.authenticationType = authenticationType;
      return this;
    }

    public Builder setConnectionType(String connectionType) {
      this.connectionType = connectionType;
      return this;
    }

    public Builder setQuery(String query) {
      this.query = query;
      return this;
    }

    public MarkLogicActionConfig build() {
      return new MarkLogicActionConfig(this);
    }
  }
}
