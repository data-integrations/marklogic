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

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.marklogic.BaseBatchMarkLogicConfig;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Config class for {@link MarkLogicSource}.
 */
public class MarkLogicSourceConfig extends BaseBatchMarkLogicConfig {
  public static final String INPUT_METHOD = "inputMethod";
  public static final String QUERY = "query";
  public static final String PATH = "path";
  public static final String BOUNDING_QUERY = "boundingQuery";
  public static final String MAX_SPLITS = "maxSplits";
  public static final String FILE_FIELD = "fileField";
  public static final String PAYLOAD_FIELD = "payloadField";
  public static final String SCHEMA = "schema";

  @Name(INPUT_METHOD)
  @Nullable
  private final String inputMethod;

  @Name(QUERY)
  @Macro
  @Nullable
  @Description("Query for data search")
  private final String query;

  @Name(PATH)
  @Macro
  @Nullable
  @Description()
  private final String path;

  @Name(BOUNDING_QUERY)
  @Macro
  @Nullable
  @Description("Query for generate splits")
  private final String boundingQuery;

  @Name(MAX_SPLITS)
  @Macro
  @Nullable
  @Description("Maximum amounts of splits")
  private final Integer maxSplits;

  @Name(FILE_FIELD)
  @Macro
  @Nullable
  @Description("Field to set file name")
  private final String fileField;

  @Name(PAYLOAD_FIELD)
  @Nullable
  @Description("Field to set payload")
  private final String payloadField;

  @Name(SCHEMA)
  @Macro
  @Nullable
  @Description("The schema of the data.")
  private final String schema;

  public MarkLogicSourceConfig(String referenceName, String inputMethod, String host, Integer port, String database,
                               String user, String password, String authenticationType, String connectionType,
                               @Nullable String query, String format, @Nullable String delimiter, String schema,
                               @Nullable String boundingQuery, @Nullable Integer maxSplits, @Nullable String fileField,
                               @Nullable String payloadField, @Nullable String path) {
    super(referenceName, host, port, database, user, password, authenticationType, connectionType, format, delimiter);
    this.inputMethod = inputMethod;
    this.query = query;
    this.path = path;
    this.boundingQuery = boundingQuery;
    this.maxSplits = maxSplits;
    this.fileField = fileField;
    this.payloadField = payloadField;
    this.schema = schema;
  }

  private MarkLogicSourceConfig(Builder builder) {
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

    inputMethod = builder.inputMethod;
    query = builder.query;
    path = builder.path;
    boundingQuery = builder.boundingQuery;
    maxSplits = builder.maxSplits;
    fileField = builder.fileField;
    payloadField = builder.payloadField;
    schema = builder.schema;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(MarkLogicSourceConfig copy) {
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
    builder.setInputMethod(copy.getInputMethodString());
    builder.setQuery(copy.getQuery());
    builder.setPath(copy.getPath());
    builder.setBoundingQuery(copy.getBoundingQuery());
    builder.setMaxSplits(copy.getMaxSplits());
    builder.setFileField(copy.getFileField());
    builder.setPayloadField(copy.getPayloadField());
    builder.setSchema(copy.getSchema());

    return builder;
  }

  @Nullable
  public String getQuery() {
    return query;
  }

  @Nullable
  public String getPath() {
    return path;
  }

  @Nullable
  public String getBoundingQuery() {
    return boundingQuery;
  }

  @Nullable
  public Integer getMaxSplits() {
    return maxSplits;
  }

  @Nullable
  public String getFileField() {
    return fileField;
  }

  @Nullable
  public String getPayloadField() {
    return payloadField;
  }

  public String getInputMethodString() {
    return inputMethod;
  }

  public InputMethod getInputMethod() {
    if (inputMethod == null) {
      return null;
    }

    try {
      return InputMethod.valueOf(inputMethod.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException("Unknown input method for value: " + inputMethod, e);
    }
  }

  public String getSchema() {
    return schema;
  }

  public Schema getParsedSchema() {
    try {
      return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid schema: " + e.getMessage(), e);
    }
  }

  public void validate(FailureCollector collector) {
    super.validate(collector);

    InputMethod method = null;
    try {
      method = getInputMethod();
    } catch (IllegalStateException e) {
      collector.addFailure(e.getMessage(), null)
        .withConfigProperty(INPUT_METHOD);
    }

    // If 'Query' or 'Path' are specified than 'Bounding query' must be set
    if ((method != null && ((!containsMacro(QUERY) && !Strings.isNullOrEmpty(query)) ||
      (!containsMacro(PATH) && !Strings.isNullOrEmpty(path))) &&
      !containsMacro(BOUNDING_QUERY) && Strings.isNullOrEmpty(boundingQuery))) {
      collector.addFailure("Bounding query must be specified.", null)
        .withConfigProperty(BOUNDING_QUERY);
    }

    if (containsMacro(SCHEMA)) {
      // Skip further validation if schema is macro
      return;
    }

    Schema schema;
    try {
      schema = getParsedSchema();
    } catch (IllegalArgumentException e) {
      collector.addFailure(e.getMessage(), null)
        .withConfigProperty(SCHEMA);

      // Skip further validation if there is an error in schema
      return;
    }

    // If file field is set it should be in schema with type 'String'
    if (!containsMacro(FILE_FIELD) && !Strings.isNullOrEmpty(fileField)) {
      if (schema.getField(fileField) == null) {
        collector.addFailure(String.format("Schema must contain file field '%s'.", fileField), null)
          .withConfigProperty(SCHEMA);
      } else if (getFieldType(schema.getField(fileField)) != Schema.Type.STRING) {
        collector.addFailure(String.format("File field '%s' must have type String.", fileField), null)
          .withOutputSchemaField(fileField);
      }
    }

    Format format;
    try {
      format = getFormat();
    } catch (IllegalStateException e) {
      // Skip further validation if there is an error in format.
      // Validation failure is already added in BaseBatchMarkLogicConfig
      return;
    }

    if (!containsMacro(PAYLOAD_FIELD) && !Strings.isNullOrEmpty(payloadField)) {
      if (schema.getField(payloadField) == null) {
        // If payload field is set it must be in schema
        collector.addFailure(String.format("Schema must contain payload field '%s'.", payloadField), null)
          .withConfigProperty(SCHEMA);
      } else if (getFieldType(schema.getField(payloadField)) != Schema.Type.STRING && getFormat() == Format.TEXT) {
        // For 'TEXT' format payload field must have type 'string'
        collector.addFailure(String.format("Payload field '%s' must have type Bytes.", payloadField), null)
          .withOutputSchemaField(payloadField);
      } else if (getFieldType(schema.getField(payloadField)) != Schema.Type.BYTES &&
        (format == Format.AUTO || format == Format.BLOB)) {
        // For 'AUTO' and 'BLOB' formats payload field must have type 'Bytes'
        collector.addFailure(String.format("Payload field '%s' must have type Bytes.", payloadField), null)
          .withOutputSchemaField(payloadField);
      }
    }

    // For 'AUTO' format all fields except file field should be nullable
    if (format == Format.AUTO) {
      for (Schema.Field field : schema.getFields()) {
        if ((!field.getName().equals(fileField)) && !field.getSchema().isNullable()) {
          collector.addFailure(String.format("Field '%s' must be nullable for 'AUTO' format.", field.getName()), null)
            .withOutputSchemaField(field.getName());
        }
      }
    }
  }

  private static Schema.Type getFieldType(Schema.Field field) {
    return field.getSchema().isNullable() ? field.getSchema().getNonNullable().getType() : field.getSchema().getType();
  }

  /**
   * Defines enum with possible input method values.
   */
  public enum InputMethod {
    QUERY,
    PATH
  }

  /**
   * Builder for creating a {@link MarkLogicSourceConfig}.
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
    private String inputMethod;
    private String query;
    private String path;
    private String boundingQuery;
    private Integer maxSplits;
    private String fileField;
    private String payloadField;
    private String schema;

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

    public Builder setInputMethod(String inputMethod) {
      this.inputMethod = inputMethod;
      return this;
    }

    public Builder setQuery(String query) {
      this.query = query;
      return this;
    }

    public Builder setPath(String path) {
      this.path = path;
      return this;
    }

    public Builder setBoundingQuery(String boundingQuery) {
      this.boundingQuery = boundingQuery;
      return this;
    }

    public Builder setMaxSplits(Integer maxSplits) {
      this.maxSplits = maxSplits;
      return this;
    }

    public Builder setFileField(String fileField) {
      this.fileField = fileField;
      return this;
    }

    public Builder setPayloadField(String payloadField) {
      this.payloadField = payloadField;
      return this;
    }

    public Builder setSchema(String schema) {
      this.schema = schema;
      return this;
    }

    public MarkLogicSourceConfig build() {
      return new MarkLogicSourceConfig(this);
    }
  }
}
