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

  public String getQuery() {
    return query;
  }

  @Override
  public void validate(FailureCollector collector) {
    super.validate(collector);

    if (!containsMacro(QUERY) && Strings.isNullOrEmpty(query)) {
      collector.addFailure("Host must be specified.", null).withConfigProperty(QUERY);
    }
  }
}
