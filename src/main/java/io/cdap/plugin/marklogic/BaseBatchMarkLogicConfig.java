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
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.IdUtils;

import javax.annotation.Nullable;

/**
 * Defines a base MarkLogic config for batch plugins.
 */
public abstract class BaseBatchMarkLogicConfig extends BaseMarkLogicConfig {
  public static final String DELIMITER = "delimiter";
  public static final String FORMAT = "format";

  @Name("referenceName")
  @Description("This will be used to uniquely identify this source/sink for lineage, annotating metadata, etc.")
  private final String referenceName;

  @Name(FORMAT)
  @Description("Type of document, default: Text")
  private final String format;

  @Name(DELIMITER)
  @Nullable
  @Description("The delimiter to use if the format is 'delimited'. The delimiter will be ignored if the format "
    + "is anything other than 'delimited'.")
  private final String delimiter;

  public BaseBatchMarkLogicConfig(String referenceName, String host, Integer port, String database, String user,
                                  String password, String authenticationType, String connectionType, String format,
                                  String delimiter) {
    super(host, port, database, user, password, authenticationType, connectionType);
    this.referenceName = referenceName;
    this.format = format;
    this.delimiter = delimiter;
  }

  public String getReferenceName() {
    return referenceName;
  }

  public Format getFormat() {
    if (format == null) {
      throw new IllegalStateException("Format cannot be 'null'");
    }

    try {
      return Format.valueOf(format.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException("Unknown format for value: " + format, e);
    }
  }

  public String getFormatString() {
    return format;
  }

  @Nullable
  public String getDelimiter() {
    return delimiter;
  }

  @Override
  public void validate(FailureCollector collector) {
    super.validate(collector);

    IdUtils.validateReferenceName(referenceName, collector);

    Format format;

    try {
      format = getFormat();
    } catch (IllegalStateException e) {
      collector.addFailure(e.getMessage(), null)
        .withConfigProperty(FORMAT);
      return;
    }

    if (format == Format.DELIMITED && Strings.isNullOrEmpty(delimiter)) {
      collector.addFailure("The delimiter must be set for format 'delimited'.", null)
        .withConfigProperty(DELIMITER);
    }
  }

  /**
   * Defines enum with possible format values.
   */
  public enum Format {
    AUTO,
    JSON,
    XML,
    DELIMITED,
    TEXT,
    BLOB
  }
}
