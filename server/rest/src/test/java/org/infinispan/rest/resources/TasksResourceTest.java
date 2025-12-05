package org.infinispan.rest.resources;

import static java.util.Collections.singletonMap;
import static org.infinispan.client.rest.RestTaskClient.ResultType.ALL;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_JAVASCRIPT;
import static org.infinispan.commons.util.Util.getResourceAsString;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Collections;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestTaskClient;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.tasks.DummyTaskEngine;
import org.infinispan.tasks.manager.TaskManager;
import org.infinispan.tasks.manager.spi.TaskEngine;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.TasksResourceTest")
public class TasksResourceTest extends AbstractRestResourceTest {

   @Override
   protected void defineCaches(EmbeddedCacheManager cm) {
      cm.defineConfiguration("default", getDefaultCacheBuilder().build());
      GlobalComponentRegistry gcr = GlobalComponentRegistry.of(cm);
      TaskManager taskManager = gcr.getComponent(TaskManager.class);
      TaskEngine taskEngine = new DummyTaskEngine();
      taskManager.registerTaskEngine(taskEngine);
   }

   @AfterClass
   public void tearDown() {
   }

   @Override
   public Object[] factory() {
      return new Object[]{
            new TasksResourceTest().withSecurity(true).browser(false),
            new TasksResourceTest().withSecurity(true).browser(true),
            new TasksResourceTest().withSecurity(false).browser(false),
            new TasksResourceTest().withSecurity(false).browser(true),
      };
   }

   @Test
   public void testTaskList() {
      RestTaskClient taskClient = adminClient.tasks();

      RestResponse response = join(taskClient.list(ALL));
      ResponseAssertion.assertThat(response).isOk();

      Json jsonNode = Json.read(response.body());
      assertEquals(DummyTaskEngine.DummyTaskTypes.values().length, jsonNode.asList().size());
      Json task = jsonNode.at(0);
      assertEquals("Dummy", task.at("type").asString());
      assertEquals("ONE_NODE", task.at("execution_mode").asString());
      assertEquals("DummyRole", task.at("allowed_role").asString());
   }

   @Test
   public void testTaskExec() {
      RestTaskClient taskClient = client.tasks();
      RestResponse response = join(taskClient.exec("SUCCESSFUL_TASK"));
      ResponseAssertion.assertThat(response).isOk();
      Json jsonNode = Json.read(response.body());
      assertEquals("result", jsonNode.asString());
   }

   @Test
   public void testParameterizedTaskExec() {
      RestTaskClient taskClient = client.tasks();
      CompletionStage<RestResponse> response = taskClient.exec("PARAMETERIZED_TASK", singletonMap("parameter", "Hello"));
      ResponseAssertion.assertThat(response).isOk();
      Json jsonNode = Json.read(join(response).body());
      assertEquals("Hello", jsonNode.asString());
   }

   @Test
   public void testFailingTaskExec() {
      RestTaskClient taskClient = client.tasks();
      CompletionStage<RestResponse> response = taskClient.exec("FAILING_TASK");
      ResponseAssertion.assertThat(response).isError();
   }

   @Test
   public void testTaskUpload() throws Exception {
      RestTaskClient taskClient = client.tasks();

      String script = getResourceAsString("hello.js", getClass().getClassLoader());
      RestEntity scriptEntity = RestEntity.create(TEXT_JAVASCRIPT, script);

      CompletionStage<RestResponse> response = taskClient.uploadScript("hello", scriptEntity);
      ResponseAssertion.assertThat(response).isOk();

      response = taskClient.exec("hello", Collections.singletonMap("greetee", "Friend"));
      ResponseAssertion.assertThat(response).isOk();
      Json jsonNode = Json.read(join(response).body());
      assertEquals("Hello Friend", jsonNode.asString());

      response = taskClient.downloadScript("hello");
      ResponseAssertion.assertThat(response).isOk();
      assertEquals(script, join(response).body());
   }

   @Test
   public void testCacheTask() {
      RestTaskClient taskClient = client.tasks();
      CompletionStage<RestResponse> response = taskClient.exec("CACHE_TASK", "default", Collections.emptyMap());
      ResponseAssertion.assertThat(response).isOk();
      Json jsonNode = Json.read(join(response).body());
      assertEquals("default", jsonNode.asString());
   }
}
