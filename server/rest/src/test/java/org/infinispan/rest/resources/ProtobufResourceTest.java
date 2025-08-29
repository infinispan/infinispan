package org.infinispan.rest.resources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.commons.util.Util.getResourceAsString;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestSchemaClient;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.Util;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.security.Security;
import org.infinispan.server.core.query.ProtobufMetadataManager;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.ProtobufResourceTest")
public class ProtobufResourceTest extends AbstractRestResourceTest {

   @Override
   public Object[] factory() {
      return new Object[]{
            new ProtobufResourceTest().withSecurity(false).browser(false),
            new ProtobufResourceTest().withSecurity(false).browser(true),
            new ProtobufResourceTest().withSecurity(true).browser(false),
            new ProtobufResourceTest().withSecurity(true).browser(true),
      };
   }

   @BeforeMethod(alwaysRun = true)
   @Override
   public void createBeforeMethod() {
      //Clear schema cache to avoid conflicts between methods
      Security.doAs(ADMIN, () -> cacheManagers.get(0).getCache(ProtobufMetadataManager.PROTOBUF_METADATA_CACHE_NAME).clear());
   }

   public void listSchemasWhenEmpty() {
      CompletionStage<RestResponse> response = client.schemas().names();

      ResponseAssertion.assertThat(response).isOk();
      Json jsonNode = Json.read(join(response).body());
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

      String cause = "Syntax error in error.proto at 3:7: unexpected label: messoge";

      ResponseAssertion.assertThat(response).isOk();
      Json jsonNode = Json.read(response.body());
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

      Json jsonNode = Json.read(response.body());
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
      Json jsonNode = Json.read(response.body());
      assertEquals(3, jsonNode.asList().size());
      assertEquals("dancers.proto", jsonNode.at(0).at("name").asString());
      assertEquals("people.proto", jsonNode.at(1).at("name").asString());
      assertEquals("users.proto", jsonNode.at(2).at("name").asString());
   }

   @Test
   public void getSchemaTypes() throws Exception {
      RestSchemaClient schemaClient = client.schemas();

      String personProto = getResourceAsString("person.proto", getClass().getClassLoader());

      join(schemaClient.post("users", personProto));

      RestResponse response = join(schemaClient.types());
      ResponseAssertion.assertThat(response).isOk();
      Json jsonNode = Json.read(response.body());
      assertEquals(4, jsonNode.asList().size());
      assertTrue(jsonNode.asList().contains("org.infinispan.rest.search.entity.Person"));
   }

   @Test
   public void uploadEmptySchema() {
      CompletionStage<RestResponse> response = client.schemas().put("empty", "");
      ResponseAssertion.assertThat(response).isBadRequest();
   }

   @Test
   public void testSchemaCompatibilityCheck() {
      String v1 = Exceptions.unchecked(() -> Util.getResourceAsString("/v1.proto", getClass().getClassLoader()));
      RestResponse response = join(client.schemas().put("compatibility.proto", v1));
      ResponseAssertion.assertThat(response).isOk();
      String v2 = Exceptions.unchecked(() -> Util.getResourceAsString("/v2.proto", getClass().getClassLoader()));
      response = join(client.schemas().put("compatibility.proto", v2));
      ResponseAssertion.assertThat(response).isError();
      String body = response.body();
      assertThat(body).contains("IPROTO000039");
      assertThat(body).contains("IPROTO000035: Field 'evolution.m1.f1' number was changed from 1 to 8");
      assertThat(body).contains("IPROTO000036: Field 'evolution.m1.f2' was removed, but its name has not been reserved");
      assertThat(body).contains("IPROTO000037: Field 'evolution.m1.f2' was removed, but its number 2 has not been reserved");
      assertThat(body).contains("IPROTO000038: Field 'evolution.m1.f3''s type was changed from 'bool' to 'sfixed32'");
      assertThat(body).contains("IPROTO000033: Type 'evolution.m1' no longer reserves field names '[f6]'");
      assertThat(body).contains("IPROTO000034: Type 'evolution.m1' no longer reserves field numbers '{6, 7}'");
      assertThat(body).contains("IPROTO000036: Field 'evolution.e1.V2' was removed, but its name has not been reserved");
      assertThat(body).contains("IPROTO000037: Field 'evolution.e1.V2' was removed, but its number 2 has not been reserved");
      assertThat(body).contains("IPROTO000033: Type 'evolution.e1' no longer reserves field names '[V4]'");
      assertThat(body).contains("IPROTO000034: Type 'evolution.e1' no longer reserves field numbers '{4, 5}'");
   }

   private void checkListProtobufEndpointUrl(String fileName, String errorMessage) {
      RestSchemaClient schemaClient = client.schemas();

      RestResponse response = join(schemaClient.names());

      Json jsonNode = Json.read(response.body());
      assertEquals(1, jsonNode.asList().size());
      assertEquals(fileName, jsonNode.at(0).at("name").asString());

      assertEquals("Schema error.proto has errors", jsonNode.at(0).at("error").at("message").asString());
      assertEquals(errorMessage, jsonNode.at(0).at("error").at("cause").asString());
   }
}
