package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpHeaderNames.ACCEPT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;
import static org.infinispan.commons.util.Util.getResourceAsString;
import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Map;

import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.security.Security;
import org.infinispan.server.core.query.ProtobufMetadataManager;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for REST v3 Protobuf Schema API endpoints.
 * <p>
 * Note: The RestSchemaClient currently uses v2 endpoints internally.
 * This test class verifies that v3 endpoints work correctly when called directly.
 *
 * @since 16.1
 */
@Test(groups = "functional", testName = "rest.ProtobufResourceV3Test")
public class ProtobufResourceV3Test extends AbstractRestResourceTest {

   @Override
   public Object[] factory() {
      return new Object[]{
            new ProtobufResourceV3Test().withSecurity(false).browser(false),
            new ProtobufResourceV3Test().withSecurity(false).browser(true),
            new ProtobufResourceV3Test().withSecurity(true).browser(false),
            new ProtobufResourceV3Test().withSecurity(true).browser(true),
      };
   }

   @BeforeMethod(alwaysRun = true)
   @Override
   public void createBeforeMethod() {
      // Clear schema cache to avoid conflicts between methods
      Security.doAs(ADMIN, () -> cacheManagers.get(0).getCache(ProtobufMetadataManager.PROTOBUF_METADATA_CACHE_NAME).clear());
   }

   @Test
   public void testListSchemasWhenEmpty() {
      RestResponse response = join(client.raw().get("/rest/v3/schemas", Map.of(ACCEPT.toString(), APPLICATION_JSON_TYPE)));

      assertThat(response).isOk();
      Json jsonNode = Json.read(response.body());
      assertEquals(0, jsonNode.asList().size());
   }

   @Test
   public void testGetNotExistingSchema() {
      RestResponse response = join(client.raw().get("/rest/v3/schemas/coco"));
      assertThat(response).isNotFound();
   }

   @Test
   public void testUpdateNonExistingSchema() throws Exception {
      String person = getResourceAsString("person.proto", getClass().getClassLoader());

      RestEntity entity = RestEntity.create(MediaType.TEXT_PLAIN, person);
      RestResponse response = join(client.raw().put("/rest/v3/schemas/person", entity));
      assertThat(response).isOk();
   }

   @Test
   public void testPutAndGetWrongProtobuf() throws Exception {
      String errorProto = getResourceAsString("error.proto", getClass().getClassLoader());

      RestEntity entity = RestEntity.create(MediaType.TEXT_PLAIN, errorProto);
      RestResponse response = join(client.raw().post("/rest/v3/schemas/error", entity));

      String cause = "Syntax error in error.proto at 3:7: unexpected label: messoge";

      assertThat(response).isOk();
      Json jsonNode = Json.read(response.body());
      assertEquals("error.proto", jsonNode.at("name").asString());
      assertEquals("Schema error.proto has errors", jsonNode.at("error").at("message").asString());
      assertEquals(cause, jsonNode.at("error").at("cause").asString());

      // Read adding .proto should also work
      response = join(client.raw().get("/rest/v3/schemas/error"));
      assertThat(response).isOk();
      assertEquals(errorProto, response.body());

      checkListProtobufEndpointUrl("error.proto", cause);
   }

   @Test
   public void testCrudSchema() throws Exception {
      String personProto = getResourceAsString("person.proto", getClass().getClassLoader());

      // Create using v3 endpoint
      RestEntity entity = RestEntity.create(MediaType.TEXT_PLAIN, personProto);
      RestResponse response = join(client.raw().post("/rest/v3/schemas/person", entity));
      assertThat(response).isOk();

      Json jsonNode = Json.read(response.body());
      assertTrue(jsonNode.at("error").isNull());

      // Read using v3 endpoint
      response = join(client.raw().get("/rest/v3/schemas/person", Map.of(ACCEPT.toString(), TEXT_PLAIN_TYPE)));
      assertThat(response).isOk();
      assertEquals(personProto, response.body());

      // Read adding .proto should also work
      response = join(client.raw().get("/rest/v3/schemas/person.proto", Map.of(ACCEPT.toString(), TEXT_PLAIN_TYPE)));
      assertThat(response).isOk();
      assertEquals(personProto, response.body());

      // Update using v3 endpoint
      entity = RestEntity.create(MediaType.TEXT_PLAIN, personProto);
      response = join(client.raw().put("/rest/v3/schemas/person", entity));
      assertThat(response).isOk();

      // Delete using v3 endpoint
      response = join(client.raw().delete("/rest/v3/schemas/person"));
      assertThat(response).isOk();

      response = join(client.raw().get("/rest/v3/schemas/person"));
      assertThat(response).isNotFound();
   }

   @Test
   public void testGetSchemaDetailed() throws Exception {
      String personProto = getResourceAsString("person.proto", getClass().getClassLoader());

      // Create schema using v3 endpoint
      RestEntity entity = RestEntity.create(MediaType.TEXT_PLAIN, personProto);
      RestResponse response = join(client.raw().post("/rest/v3/schemas/person", entity));
      assertThat(response).isOk();

      // Get detailed schema information using v3 endpoint
      response = join(client.raw().get("/rest/v3/schemas/person/_detailed", Map.of(ACCEPT.toString(), APPLICATION_JSON_TYPE)));
      assertThat(response).isOk();

      Json schemaContent = Json.read(response.body());
      assertEquals("person.proto", schemaContent.at("name").asString());
      assertEquals(personProto, schemaContent.at("content").asString());
      assertTrue(schemaContent.at("error").isNull());
      assertTrue(schemaContent.at("caches").isArray());
      assertEquals(0, schemaContent.at("caches").asList().size());

      // Clean up
      join(client.raw().delete("/rest/v3/schemas/person"));
   }


   public void testCreateTwiceSchema() throws Exception {
      String personProto = getResourceAsString("person.proto", getClass().getClassLoader());

      RestEntity entity = RestEntity.create(MediaType.TEXT_PLAIN, personProto);
      RestResponse response = join(client.raw().post("/rest/v3/schemas/person", entity));
      assertThat(response).isOk();

      response = join(client.raw().post("/rest/v3/schemas/person", entity));
      assertThat(response).isConflicted();
   }

   @Test
   public void testAddAndGetListOrderedByName() throws Exception {
      String personProto = getResourceAsString("person.proto", getClass().getClassLoader());

      RestEntity entity = RestEntity.create(MediaType.TEXT_PLAIN, personProto);
      join(client.raw().post("/rest/v3/schemas/users", entity));
      join(client.raw().post("/rest/v3/schemas/people", entity));
      join(client.raw().post("/rest/v3/schemas/dancers", entity));

      RestResponse response = join(client.raw().get("/rest/v3/schemas", Map.of(ACCEPT.toString(), APPLICATION_JSON_TYPE)));

      assertThat(response).isOk();
      Json jsonNode = Json.read(response.body());
      assertEquals(3, jsonNode.asList().size());
      assertEquals("dancers.proto", jsonNode.at(0).at("name").asString());
      assertEquals("people.proto", jsonNode.at(1).at("name").asString());
      assertEquals("users.proto", jsonNode.at(2).at("name").asString());
   }

   @Test
   public void testGetSchemaTypes() throws Exception {
      String personProto = getResourceAsString("person.proto", getClass().getClassLoader());

      RestEntity entity = RestEntity.create(MediaType.TEXT_PLAIN, personProto);
      join(client.raw().post("/rest/v3/schemas/users", entity));

      RestResponse response = join(client.raw().get("/rest/v3/meta/schemas/_types", Map.of(ACCEPT.toString(), APPLICATION_JSON_TYPE)));
      assertThat(response).isOk();
      Json jsonNode = Json.read(response.body());
      assertEquals(4, jsonNode.asList().size());
      assertTrue(jsonNode.asList().contains("org.infinispan.rest.search.entity.Person"));
   }

   @Test
   public void testUploadEmptySchema() {
      RestEntity entity = RestEntity.create(MediaType.TEXT_PLAIN, "");
      RestResponse response = join(client.raw().put("/rest/v3/schemas/empty", entity));
      assertThat(response).isBadRequest();
   }

   private void checkListProtobufEndpointUrl(String fileName, String errorMessage) {
      RestResponse response = join(client.raw().get("/rest/v3/schemas", Map.of(ACCEPT.toString(), APPLICATION_JSON_TYPE)));

      Json jsonNode = Json.read(response.body());
      assertEquals(1, jsonNode.asList().size());
      assertEquals(fileName, jsonNode.at(0).at("name").asString());

      assertEquals("Schema error.proto has errors", jsonNode.at(0).at("error").at("message").asString());
      assertEquals(errorMessage, jsonNode.at(0).at("error").at("cause").asString());
   }
}
