package org.infinispan.rest.resources;

import static org.infinispan.commons.util.Util.getResourceAsString;
import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import java.security.PrivilegedAction;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestSchemaClient;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.security.Security;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
      Security.doAs(ADMIN, (PrivilegedAction<Void>) () -> {
         cacheManagers.get(0).getCache(ProtobufMetadataManager.PROTOBUF_METADATA_CACHE_NAME).clear();
         return null;
      });
   }

   public void listSchemasWhenEmpty() {
      CompletionStage<RestResponse> response = client.schemas().names();

      ResponseAssertion.assertThat(response).isOk();
      Json jsonNode = Json.read(join(response).getBody());
      assertEquals(0, jsonNode.asList().size());
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
      Json jsonNode = Json.read(response.getBody());
      assertEquals("error.proto", jsonNode.at("name").asString());
      assertEquals("Schema error.proto has errors", jsonNode.at("error").at("message").asString());
      assertEquals(cause, jsonNode.at("error").at("cause").asString());

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

      Json jsonNode = Json.read(response.getBody());
      assertTrue(jsonNode.at("error").isNull());

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

      join(schemaClient.post("users", personProto));
      join(schemaClient.post("people", personProto));
      join(schemaClient.post("dancers", personProto));

      RestResponse response = join(schemaClient.names());

      ResponseAssertion.assertThat(response).isOk();
      Json jsonNode = Json.read(response.getBody());
      assertEquals(3, jsonNode.asList().size());
      assertEquals("dancers.proto", jsonNode.at(0).at("name").asString());
      assertEquals("people.proto", jsonNode.at(1).at("name").asString());
      assertEquals("users.proto", jsonNode.at(2).at("name").asString());
   }

   private void checkListProtobufEndpointUrl(String fileName, String errorMessage) {
      RestSchemaClient schemaClient = client.schemas();

      RestResponse response = join(schemaClient.names());

      Json jsonNode = Json.read(response.getBody());
      assertEquals(1, jsonNode.asList().size());
      assertEquals(fileName, jsonNode.at(0).at("name").asString());

      assertEquals("Schema error.proto has errors", jsonNode.at(0).at("error").at("message").asString());
      assertEquals(errorMessage, jsonNode.at(0).at("error").at("cause").asString());
   }
}
