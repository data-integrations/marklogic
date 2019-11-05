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

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.eval.ServerEvaluationCall;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.marklogic.MarkLogicPluginConstants;

/**
 * Action that runs MarkLogic command.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(MarkLogicPluginConstants.PLUGIN_NAME)
@Description("Action that runs a MarkLogic xquery command")
public class MarkLogicAction extends Action {
  private final MarkLogicActionConfig config;

  public MarkLogicAction(MarkLogicActionConfig config) {
    this.config = config;
  }

  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();
  }

  @Override
  public void run(ActionContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();

    DatabaseClientFactory.SecurityContext securityContext = getSecurityContext();
    DatabaseClient.ConnectionType connectionType = config.getConnectionType();

    DatabaseClient client = DatabaseClientFactory.newClient(
      config.getHost(),
      config.getPort(),
      config.getDatabase(),
      securityContext,
      connectionType
    );

    ServerEvaluationCall theCall = client.newServerEval();
    theCall.xquery(config.getQuery());
    theCall.eval();
    client.release();
  }

  private DatabaseClientFactory.SecurityContext getSecurityContext() {
    switch (config.getAuthenticationType()) {
      case DIGEST:
        return new DatabaseClientFactory.DigestAuthContext(config.getUser(), config.getPassword());
      case SSL:
        return new DatabaseClientFactory.BasicAuthContext(config.getUser(), config.getPassword());
      default:
        throw new IllegalArgumentException("Unsupported 'Authentication type': " +
                                             config.getAuthenticationTypeString());
    }
  }
}
