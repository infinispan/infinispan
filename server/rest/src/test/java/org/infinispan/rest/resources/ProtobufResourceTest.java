package org.infinispan.rest.resources;

import static org.infinispan.commons.util.Util.getResourceAsString;
import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import java.security.PrivilegedAction;
import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;

import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestSchemaClient;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.util.concurrent.CompletionStages;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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
      CompletionStage<RestResponse> response = client.schemas().names();

      ResponseAssertion.assertThat(response).isOk();
      JsonNode jsonNode = new ObjectMapper().readTree(join(response).getBody());
      assertEquals(0, jsonNode.size());
   }

   @Test
   public void getNotExistingSchema() {
      CompletionStage<RestResponse> response = client.schemas().get("coco");
      ResponseAssertion.assertThat(response).isNotFound();
   }

   @Test
   public void updateNonExistingSchema() throws Exception {
      String person = getResourceAsString("person.proto", getClass().getClassLoader());

      CompletionStage<RestResponse> response = client.schemas().put("person", person);
      ResponseAssertion.assertThat(response).isOk();
   }

   @Test
   public void putAndGetWrongProtobuf() throws Exception {
      RestSchemaClient schemaClient = client.schemas();

      String errorProto = getResourceAsString("error.proto", getClass().getClassLoader());

      RestResponse response = join(schemaClient.post("error", errorProto));

      String cause = "java.lang.IllegalStateException:"
            + " Syntax error in error.proto at 3:8: unexpected label: messoge";

      ResponseAssertion.assertThat(response).isOk();
      JsonNode jsonNode = new ObjectMapper().readTree(response.getBody());
      assertEquals("error.proto", jsonNode.get("name").asText());
      assertEquals("Schema error.proto has errors", jsonNode.get("error").get("message").asText());
      assertEquals(cause, jsonNode.get("error").get("cause").asText());

      // Read adding .proto should also work
      response = join(schemaClient.get("error"));
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentEqualToFile("error.proto");

      checkListProtobufEndpointUrl("error.proto", cause);
   }

   @Test
   public void crudSchema() throws Exception {
      RestSchemaClient schemaClient = client.schemas();
      String personProto = getResourceAsString("person.proto", getClass().getClassLoader());

      // Create
      RestResponse response = join(schemaClient.post("person", personProto));
      ResponseAssertion.assertThat(response).isOk();

      JsonNode jsonNode = new ObjectMapper().readTree(response.getBody());
      assertTrue(jsonNode.get("error").isNull());

      // Read
      response = join(schemaClient.get("person"));

      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentEqualToFile("person.proto");

      // Read adding .proto should also work
      response = join(schemaClient.get("person.proto"));
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentEqualToFile("person.proto");

      // Update
      response = join(schemaClient.put("person", personProto));
      ResponseAssertion.assertThat(response).isOk();

      // Delete
      response = join(schemaClient.delete("person"));
      ResponseAssertion.assertThat(response).isOk();

      response = join(schemaClient.get("person"));
      ResponseAssertion.assertThat(response).isNotFound();
   }

   @Test
   public void createTwiceSchema() throws Exception {
      RestSchemaClient schemaClient = client.schemas();

      String personProto = getResourceAsString("person.proto", getClass().getClassLoader());
      CompletionStage<RestResponse> response = schemaClient.post("person", personProto);
      ResponseAssertion.assertThat(response).isOk();
      response = schemaClient.post("person", personProto);
      ResponseAssertion.assertThat(response).isConflicted();
   }

   @Test
   public void addAndGetListOrderedByName() throws Exception {
      RestSchemaClient schemaClient = client.schemas();

      String personProto = getResourceAsString("person.proto", getClass().getClassLoader());

      join(CompletionStages.allOf(
            schemaClient.post("users", personProto),
            schemaClient.post("people", personProto),
            schemaClient.post("dancers", personProto)
      ));

      RestResponse response = join(schemaClient.names());

      ResponseAssertion.assertThat(response).isOk();
      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode jsonNode = objectMapper.readTree(response.getBody());
      assertEquals(3, jsonNode.size());
      assertEquals("dancers.proto", jsonNode.get(0).get("name").asText());
      assertEquals("people.proto", jsonNode.get(1).get("name").asText());
      assertEquals("users.proto", jsonNode.get(2).get("name").asText());
   }

   private void checkListProtobufEndpointUrl(String fileName, String errorMessage) throws Exception {
      RestSchemaClient schemaClient = client.schemas();

      RestResponse response = join(schemaClient.names());

      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode jsonNode = objectMapper.readTree(response.getBody());
      assertEquals(1, jsonNode.size());
      assertEquals(fileName, jsonNode.get(0).get("name").asText());

      assertEquals("Schema error.proto has errors", jsonNode.get(0).get("error").get("message").asText());
      assertEquals(errorMessage, jsonNode.get(0).get("error").get("cause").asText());
   }
}
