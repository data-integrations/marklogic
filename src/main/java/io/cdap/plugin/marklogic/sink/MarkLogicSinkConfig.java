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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.plugin.marklogic.BaseBatchMarkLogicConfig;

import javax.annotation.Nullable;

/**
 * Config class for {@link MarkLogicSink}.
 */
public class MarkLogicSinkConfig extends BaseBatchMarkLogicConfig {
  public static final String PATH = "path";
  public static final String FILE_NAME_FIELD = "fileNameField";
  public static final String BATCH_SIZE = "batchSize";

  @Name(PATH)
  @Description("Destination path")
  private final String path;

  @Name(FILE_NAME_FIELD)
  @Nullable
  @Description("Field which contains unique value for filename, otherwise UUID generation will be used")
  private final String fileNameField;

  @Name(BATCH_SIZE)
  @Description("The batch size for writing to MarkLogic")
  private final Integer batchSize;

  public MarkLogicSinkConfig(String referenceName, String host, Integer port, String database, String user,
                             String password, String authenticationType, String connectionType, String format,
                             @Nullable String delimiter, String path, @Nullable String fileNameField,
                             Integer batchSize) {
    super(referenceName, host, port, database, user, password, authenticationType, connectionType, format, delimiter);
    this.path = path;
    this.fileNameField = fileNameField;
    this.batchSize = batchSize;
  }

  private MarkLogicSinkConfig(Builder builder) {
    super(
      builder.referenceName,
      builder.host,
      builder.port,
      builder.database,
      builder.user,
      builder.password,
      builder.authenticationType,
      builder.connectionType,
      builder.format,
      builder.delimiter
    );

    path = builder.path;
    fileNameField = builder.fileNameField;
    batchSize = builder.batchSize;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(MarkLogicSinkConfig copy) {
    Builder builder = new Builder();

    builder.setHost(copy.getHost());
    builder.setPort(copy.getPort());
    builder.setDatabase(copy.getDatabase());
    builder.setUser(copy.getUser());
    builder.setPassword(copy.getPassword());
    builder.setAuthenticationType(copy.getAuthenticationTypeString());
    builder.setConnectionType(copy.getConnectionTypeString());
    builder.setReferenceName(copy.getReferenceName());
    builder.setFormat(copy.getFormatString());
    builder.setDelimiter(copy.getDelimiter());
    builder.setPath(copy.getPath());
    builder.setFileNameField(copy.getFileNameField());
    builder.setBatchSize(builder.batchSize);

    return builder;
  }

  public String getPath() {
    return path;
  }

  @Nullable
  public String getFileNameField() {
    return fileNameField;
  }

  public Integer getBatchSize() {
    return batchSize;
  }

  /**
   * Builder for creating a {@link MarkLogicSinkConfig}.
   */
  public static final class Builder {
    private String host;
    private Integer port;
    private String database;
    private String user;
    private String password;
    private String authenticationType;
    private String connectionType;
    private String referenceName;
    private String format;
    private String delimiter;
    private String path;
    private String fileNameField;
    private Integer batchSize;

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

    public Builder setReferenceName(String referenceName) {
      this.referenceName = referenceName;
      return this;
    }

    public Builder setFormat(String format) {
      this.format = format;
      return this;
    }

    public Builder setDelimiter(String delimiter) {
      this.delimiter = delimiter;
      return this;
    }

    public Builder setPath(String path) {
      this.path = path;
      return this;
    }

    public Builder setFileNameField(String fileNameField) {
      this.fileNameField = fileNameField;
      return this;
    }

    public void setBatchSize(Integer batchSize) {
      this.batchSize = batchSize;
    }

    public MarkLogicSinkConfig build() {
      return new MarkLogicSinkConfig(this);
    }
  }
}
