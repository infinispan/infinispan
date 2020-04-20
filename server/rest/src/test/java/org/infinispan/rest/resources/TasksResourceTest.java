package org.infinispan.rest.resources;

import static org.eclipse.jetty.http.HttpMethod.GET;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JAVASCRIPT_TYPE;
import static org.infinispan.commons.util.Util.getResourceAsString;
import static org.testng.AssertJUnit.assertEquals;

import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.tasks.DummyTaskEngine;
import org.infinispan.tasks.TaskManager;
import org.infinispan.tasks.spi.TaskEngine;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Test(groups = "functional", testName = "rest.TasksResourceTest")
public class TasksResourceTest extends AbstractRestResourceTest {

   private ObjectMapper mapper = new ObjectMapper();

   @Override
   protected void defineCaches(EmbeddedCacheManager cm) {
      cm.defineConfiguration("default", getDefaultCacheBuilder().build());
      GlobalComponentRegistry gcr = cm.getGlobalComponentRegistry();
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
            new TasksResourceTest().withSecurity(true),
            new TasksResourceTest().withSecurity(false),
      };
   }

   @Test
   public void testTaskList() throws Exception {
      String baseURL = String.format("http://localhost:%d/rest/v2/tasks", restServer().getPort());
      ContentResponse response = client.newRequest(baseURL).method(GET).send();
      ResponseAssertion.assertThat(response).isOk();
      JsonNode jsonNode = mapper.readTree(response.getContent());
      assertEquals(4, jsonNode.size());
      JsonNode task = jsonNode.get(0);
      assertEquals("Dummy", task.get("type").asText());
      assertEquals("ONE_NODE", task.get("execution_mode").asText());
      assertEquals("DummyRole", task.get("allowed_role").asText());
   }

   @Test
   public void testTaskExec() throws Exception {
      String baseURL = String.format("http://localhost:%d/rest/v2/tasks", restServer().getPort());
      ContentResponse response = client.newRequest(baseURL + "/SUCCESSFUL_TASK?action=exec").method(GET).send();
      ResponseAssertion.assertThat(response).isOk();
      JsonNode jsonNode = mapper.readTree(response.getContent());
      assertEquals("result", jsonNode.asText());
   }

   @Test
   public void testParameterizedTaskExec() throws Exception {
      String baseURL = String.format("http://localhost:%d/rest/v2/tasks", restServer().getPort());
      ContentResponse response = client.newRequest(baseURL + "/PARAMETERIZED_TASK?action=exec&param.parameter=Hello").method(GET).send();
      ResponseAssertion.assertThat(response).isOk();
      JsonNode jsonNode = mapper.readTree(response.getContent());
      assertEquals("Hello", jsonNode.asText());
   }

   @Test
   public void testFailingTaskExec() throws Exception {
      String baseURL = String.format("http://localhost:%d/rest/v2/tasks", restServer().getPort());
      ContentResponse response = client.newRequest(baseURL + "/FAILING_TASK?action=exec").method(GET).send();
      ResponseAssertion.assertThat(response).isError();
   }

   @Test
   public void testTaskUpload() throws Exception {
      String baseURL = String.format("http://localhost:%d/rest/v2/tasks", restServer().getPort());
      String script = getResourceAsString("hello.js", getClass().getClassLoader());
      ContentResponse response = client.newRequest(baseURL + "/hello").header("Content-type", APPLICATION_JAVASCRIPT_TYPE)
            .method(HttpMethod.POST).content(new StringContentProvider(script)).send();
      ResponseAssertion.assertThat(response).isOk();

      response = client.newRequest(baseURL + "/hello?action=exec&param.greetee=Friend").method(GET).send();
      ResponseAssertion.assertThat(response).isOk();
      JsonNode jsonNode = mapper.readTree(response.getContent());
      assertEquals("Hello Friend", jsonNode.asText());
   }
}
