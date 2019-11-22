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

import com.google.common.collect.ImmutableMap;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.document.TextDocumentManager;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.eval.ServerEvaluationCall;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.mapreduce.DatabaseDocument;
import com.marklogic.mapreduce.DocumentInputFormat;
import com.marklogic.mapreduce.DocumentURI;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.marklogic.action.MarkLogicAction;
import io.cdap.plugin.marklogic.sink.ConfiguredContentOutputFormat;
import io.cdap.plugin.marklogic.sink.MarkLogicSink;
import io.cdap.plugin.marklogic.source.MarkLogicSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class BaseMarkLogicTest extends HydratorTestBase {
  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", "3.2.0");
  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");
  protected static final long CURRENT_TS = System.currentTimeMillis();

  private static final String PATH_PATTERN = "%s%s";

  protected static final String JSON_FOLDER = "/json/";
  protected static final String XML_FOLDER = "/xml/";
  protected static final String DELIMITED_FOLDER = "/delimited/";

  protected static final String TEXT_DOCUMENT = "text.txt";
  protected static final String JSON_DOCUMENT = "test.json";
  protected static final String XML_DOCUMENT = "test.xml";
  protected static final String DELIMITED_DOCUMENT = "test.txt";

  protected static boolean tearDown = true;
  private static int startCount;

  protected static final Map<String, String> BASE_PROPS = ImmutableMap.<String, String>builder()
    .put(BaseMarkLogicConfig.HOST, System.getProperty("marklogic.host", "localhost"))
    .put(BaseMarkLogicConfig.PORT, System.getProperty("marklogic.port", "8011"))
    .put(BaseMarkLogicConfig.DATABASE, System.getProperty("marklogic.database", "mydb"))
    .put(BaseMarkLogicConfig.USER, System.getProperty("marklogic.username", "admin"))
    .put(BaseMarkLogicConfig.PASSWORD, System.getProperty("marklogic.password", "123Qwe123"))
    .put(BaseMarkLogicConfig.AUTHENTICATION_TYPE, BaseMarkLogicConfig.AuthenticationType.DIGEST.toString())
    .put(BaseMarkLogicConfig.CONNECTION_TYPE, DatabaseClient.ConnectionType.DIRECT.toString())
    .build();

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }

    setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);

    addPluginArtifact(NamespaceId.DEFAULT.artifact("MarkLogic", "1.0.0"),
                      DATAPIPELINE_ARTIFACT_ID,
                      MarkLogicSource.class, MarkLogicSink.class, MarkLogicAction.class, DatabaseDocument.class,
                      DocumentURI.class, DocumentInputFormat.class, ConfiguredContentOutputFormat.class);

    DatabaseClient client = getDatabaseClient();

    writeTextToDocument(TEXT_DOCUMENT, "some text", client);
    createDocumentFromFile(JSON_DOCUMENT, JSON_FOLDER, client);
    createDocumentFromFile(DELIMITED_DOCUMENT, DELIMITED_FOLDER, client);
    createXMLDocument(client);

    client.release();
  }

  @AfterClass
  public static void tearDownDB() {
    if (!tearDown) {
      return;
    }

    String deleteQuery = "xquery version \"1.0-ml\";\nxdmp:document-delete(cts:uris((),(),cts:and-query(())))";
    DatabaseClient client = getDatabaseClient();
    ServerEvaluationCall theCall = client.newServerEval();
    theCall.xquery(deleteQuery);
    theCall.eval();
    client.release();
  }

  protected void runETLOnce(ApplicationManager appManager) throws TimeoutException,
    InterruptedException, ExecutionException {
    runETLOnce(appManager, ImmutableMap.<String, String>of());
  }

  protected void runETLOnce(ApplicationManager appManager,
                            Map<String, String> arguments) throws TimeoutException, InterruptedException,
    ExecutionException {
    final WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start(arguments);
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
  }

  protected static DatabaseClient getDatabaseClient() {
    DatabaseClientFactory.SecurityContext securityContext = new DatabaseClientFactory.DigestAuthContext(
      BASE_PROPS.get(BaseMarkLogicConfig.USER),
      BASE_PROPS.get(BaseMarkLogicConfig.PASSWORD)
    );

    return DatabaseClientFactory.newClient(
      BASE_PROPS.get(BaseMarkLogicConfig.HOST),
      Integer.parseInt(BASE_PROPS.get(BaseMarkLogicConfig.PORT)),
      BASE_PROPS.get(BaseMarkLogicConfig.DATABASE),
      securityContext,
      DatabaseClient.ConnectionType.DIRECT
    );
  }

  protected ApplicationManager deployETL(ETLPlugin sourcePlugin, ETLPlugin sinkPlugin,
                                         ArtifactSummary datapipelineArtifact, String appName)
    throws Exception {
    ETLBatchConfig etlConfig = getETLBatchConfig(sourcePlugin, sinkPlugin);
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(datapipelineArtifact, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);
    return deployApplication(appId, appRequest);
  }

  protected ETLBatchConfig getETLBatchConfig(ETLPlugin sourcePlugin, ETLPlugin sinkPlugin) {
    ETLStage source = new ETLStage("source", sourcePlugin);
    ETLStage sink = new ETLStage("sink", sinkPlugin);
    return ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();
  }

  private static void createXMLDocument(DatabaseClient client) {
    InputStream docStream = BaseMarkLogicTest.class.getClassLoader().getResourceAsStream(XML_DOCUMENT);
    XMLDocumentManager docMgr = client.newXMLDocumentManager();
    InputStreamHandle handle = new InputStreamHandle(docStream);
    String path = String.format(PATH_PATTERN, XML_FOLDER, XML_DOCUMENT);
    docMgr.write(path, handle);
  }

  private static void createDocumentFromFile(String fileName, String folder, DatabaseClient client) throws IOException {
    String path = String.format(PATH_PATTERN, folder, fileName);
    String file = BaseMarkLogicTest.class.getClassLoader().getResource(fileName).getFile();
    String content = new String(Files.readAllBytes(Paths.get(file)));
    writeTextToDocument(path, content, client);
  }

  private static void writeTextToDocument(String path, String data, DatabaseClient client) {
    TextDocumentManager docMgr = client.newTextDocumentManager();
    StringHandle content = new StringHandle(data);
    docMgr.write(path, content);
  }
}
