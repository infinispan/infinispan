package org.infinispan.rest.resources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.security.auth.Subject;
import java.security.PrivilegedAction;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;
import static org.infinispan.commons.util.Util.getResourceAsString;
import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "functional", testName = "rest.ProtobufResourceTest")
public class ProtobufResourceTest extends AbstractRestResourceTest {

   @Override
   public Object[] factory() {
      return new Object[]{
            new ProtobufResourceTest().withSecurity(false),
            new ProtobufResourceTest().withSecurity(true),
      };
   }

   @BeforeMethod(alwaysRun = true)
   @Override
   public void createBeforeMethod() {
      //Clear schema cache to avoid conflicts between methods
      Subject.doAs(ADMIN_USER, (PrivilegedAction<Void>) () -> {
         cacheManagers.get(0).getCache(ProtobufMetadataManager.PROTOBUF_METADATA_CACHE_NAME).clear();
         return null;
      });
   }

   public void listSchemasWhenEmpty() throws Exception {
      String BASE_URL = String.format("http://localhost:%d/rest/v2/schemas", restServer().getPort());

      ContentResponse response = client.newRequest(BASE_URL)
            .method(HttpMethod.GET)
            .accept(APPLICATION_JSON_TYPE).send();

      ResponseAssertion.assertThat(response).isOk();
      JsonNode jsonNode = new ObjectMapper().readTree(response.getContentAsString());
      assertEquals(0, jsonNode.size());
   }

   @Test
   public void getNotExistingSchema() throws Exception {
      String BASE_URL = String.format("http://localhost:%d/rest/v2/schemas", restServer().getPort());

      ContentResponse response = client.newRequest(BASE_URL + "/coco")
            .method(HttpMethod.GET)
            .accept(TEXT_PLAIN_TYPE).send();

      ResponseAssertion.assertThat(response).isNotFound();
   }

   @Test
   public void updateNonExistingSchema() throws Exception {
      String person = getResourceAsString("person.proto", getClass().getClassLoader());

      ContentResponse response = updateSchema("person", person);

      ResponseAssertion.assertThat(response).isOk();
   }

   @Test
   public void putAndGetWrongProtobuf() throws Exception {
      String BASE_URL = String.format("http://localhost:%d/rest/v2/schemas", restServer().getPort());

      String errorProto = getResourceAsString("error.proto", getClass().getClassLoader());

      ContentResponse response = addSchema("error", errorProto);

      String cause = "java.lang.IllegalStateException:"
            + " Syntax error in error.proto at 3:8: unexpected label: messoge";

      ResponseAssertion.assertThat(response).isOk();
      JsonNode jsonNode = new ObjectMapper().readTree(response.getContent());
      assertEquals("error.proto", jsonNode.get("name").asText());
      assertEquals("Schema error.proto has errors", jsonNode.get("error").get("message").asText());
      assertEquals(cause, jsonNode.get("error").get("cause").asText());

      // Read adding .proto should also work
      response = client.newRequest(BASE_URL + "/error")
            .method(HttpMethod.GET)
            .accept(TEXT_PLAIN_TYPE).send();
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentEqualToFile("error.proto");

      checkListProtobufEndpointUrl("error.proto", cause);
   }

   @Test
   public void crudSchema() throws Exception {
      String BASE_URL = String.format("http://localhost:%d/rest/v2/schemas", restServer().getPort());
      String personProto = getResourceAsString("person.proto", getClass().getClassLoader());

      // Create
      ContentResponse response = addSchema("person", personProto);
      ResponseAssertion.assertThat(response).isOk();
      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode jsonNode = objectMapper.readTree(response.getContent());
      assertEquals("null", jsonNode.get("error").asText());

      // Read
      response = client.newRequest(BASE_URL + "/person")
            .method(HttpMethod.GET)
            .accept(TEXT_PLAIN_TYPE).send();

      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentEqualToFile("person.proto");

      // Read adding .proto should also work
      response = client.newRequest(BASE_URL + "/person.proto")
            .method(HttpMethod.GET)
            .accept(TEXT_PLAIN_TYPE).send();
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentEqualToFile("person.proto");

      // Update
      response = updateSchema("person", personProto);
      ResponseAssertion.assertThat(response).isOk();

      // Delete
      response = client.newRequest(BASE_URL + "/person")
            .content(new StringContentProvider(personProto))
            .method(HttpMethod.DELETE).send();

      ResponseAssertion.assertThat(response).isOk();

      response = client.newRequest(BASE_URL + "/person")
            .content(new StringContentProvider(personProto))
            .method(HttpMethod.GET)
            .accept(TEXT_PLAIN_TYPE).send();

      ResponseAssertion.assertThat(response).isNotFound();
   }

   @Test
   public void createTwiceSchema() throws Exception {
      String personProto = getResourceAsString("person.proto", getClass().getClassLoader());
      ContentResponse response = addSchema("person", personProto);
      ResponseAssertion.assertThat(response).isOk();
      response = addSchema("person", personProto);
      ResponseAssertion.assertThat(response).isConflicted();
   }

   @Test
   public void addAndGetListOrderedByName() throws Exception {
      String personProto = getResourceAsString("person.proto", getClass().getClassLoader());
      addSchema("users", personProto);
      addSchema("people", personProto);
      addSchema("dancers", personProto);

      String url = String.format("http://localhost:%d/rest/v2/schemas", restServer().getPort());
      ContentResponse response = client.newRequest(url)
            .method(HttpMethod.GET)
            .accept(APPLICATION_JSON_TYPE).send();

      ResponseAssertion.assertThat(response).isOk();
      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode jsonNode = objectMapper.readTree(response.getContent());
      assertEquals(3, jsonNode.size());
      assertEquals("dancers.proto", jsonNode.get(0).get("name").asText());
      assertEquals("people.proto", jsonNode.get(1).get("name").asText());
      assertEquals("users.proto", jsonNode.get(2).get("name").asText());
   }

   private ContentResponse addSchema(String schemaName, String schemaContent)
         throws InterruptedException, TimeoutException, ExecutionException {
      return callAPI(schemaName, schemaContent, HttpMethod.POST);
   }

   private ContentResponse updateSchema(String schemaName, String schemaContent)
         throws InterruptedException, TimeoutException, ExecutionException {
      return callAPI(schemaName, schemaContent, HttpMethod.PUT);
   }

   private ContentResponse callAPI(String schemaName, String schemaContent, HttpMethod method)
         throws InterruptedException, TimeoutException, ExecutionException {
      return client.newRequest(String.format("http://localhost:%d/rest/v2/schemas/", restServer().getPort()) + schemaName)
            .content(new StringContentProvider(schemaContent))
            .header("Content-type", "text/plain; charset=utf-8")
            .method(method).send();
   }

   private void checkListProtobufEndpointUrl(String fileName, String errorMessage) throws Exception {
      String url = String.format("http://localhost:%d/rest/v2/schemas", restServer().getPort());
      ContentResponse response = client.newRequest(url).method(HttpMethod.GET).accept(APPLICATION_JSON_TYPE).send();

      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode jsonNode = objectMapper.readTree(response.getContent());
      assertEquals(1, jsonNode.size());
      assertEquals(fileName, jsonNode.get(0).get("name").asText());

      assertEquals("Schema error.proto has errors", jsonNode.get(0).get("error").get("message").asText());
      assertEquals(errorMessage, jsonNode.get(0).get("error").get("cause").asText());
   }
}
