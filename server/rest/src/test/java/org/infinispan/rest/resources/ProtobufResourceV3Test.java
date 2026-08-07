package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpHeaderNames.ACCEPT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;
import static org.infinispan.commons.util.Util.getResourceAsString;
import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.security.Security;
import org.infinispan.server.core.query.ProtobufMetadataManager;
import org.infinispan.server.core.query.impl.indexing.IndexingMetadata;
import org.infinispan.server.core.query.impl.indexing.infinispan.InfinispanAnnotations;
import org.infinispan.server.core.query.impl.indexing.search5.Search5Annotations;
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

   @Test
   public void testGetAnnotations() {
      RestResponse response = join(client.raw().get("/rest/v3/meta/schemas/_annotations", Map.of(ACCEPT.toString(), APPLICATION_JSON_TYPE)));
      assertThat(response).isOk();

      Json jsonNode = Json.read(response.body());
      assertTrue(jsonNode.isArray());
      int size = jsonNode.asList().size();
      assertTrue(size > 0);

      Set<String> names = new HashSet<>();
      Json basic = null;
      for (int i = 0; i < size; i++) {
         Json annot = jsonNode.at(i);
         String name = annot.at("name").asString();
         names.add(name);
         if (InfinispanAnnotations.BASIC_ANNOTATION.equals(name)) {
            basic = annot;
         }
      }

      // Infinispan-native annotations
      assertTrue(names.contains(IndexingMetadata.INDEXED_ANNOTATION), "Missing @Indexed annotation");
      assertTrue(names.contains(InfinispanAnnotations.BASIC_ANNOTATION), "Missing @Basic annotation");
      assertTrue(names.contains(InfinispanAnnotations.KEYWORD_ANNOTATION), "Missing @Keyword annotation");
      assertTrue(names.contains(InfinispanAnnotations.TEXT_ANNOTATION), "Missing @Text annotation");
      assertTrue(names.contains(InfinispanAnnotations.DECIMAL_ANNOTATION), "Missing @Decimal annotation");
      assertTrue(names.contains(InfinispanAnnotations.EMBEDDED_ANNOTATION), "Missing @Embedded annotation");
      assertTrue(names.contains(InfinispanAnnotations.VECTOR_ANNOTATION), "Missing @Vector annotation");
      assertTrue(names.contains(InfinispanAnnotations.GEO_POINT_ANNOTATION), "Missing @GeoPoint annotation");
      assertTrue(names.contains(InfinispanAnnotations.GEO_FIELD_ANNOTATION), "Missing @GeoField annotation");
      assertTrue(names.contains(InfinispanAnnotations.LATITUDE_ANNOTATION), "Missing @Latitude annotation");
      assertTrue(names.contains(InfinispanAnnotations.LONGITUDE_ANNOTATION), "Missing @Longitude annotation");

      // Legacy Search5 annotations
      assertTrue(names.contains(Search5Annotations.FIELD_ANNOTATION), "Missing @Field annotation");
      assertTrue(names.contains(Search5Annotations.ANALYZER_ANNOTATION), "Missing @Analyzer annotation");
      assertTrue(names.contains(Search5Annotations.SORTABLE_FIELD_ANNOTATION), "Missing @SortableField annotation");

      // Internal and container annotations should be filtered out
      assertFalse(names.contains("TypeId"), "TypeId should be filtered out");
      assertFalse(names.contains("ProtoTypeId"), "ProtoTypeId should be filtered out");
      assertFalse(names.contains(Search5Annotations.FIELDS_ANNOTATION), "Container annotation Fields should be filtered out");
      assertFalse(names.contains(Search5Annotations.SORTABLE_FIELDS_ANNOTATION), "Container annotation SortableFields should be filtered out");
      assertFalse(names.contains(InfinispanAnnotations.GEO_POINTS_ANNOTATION), "Container annotation GeoPoints should be filtered out");

      // Verify structure of @Basic
      assertNotNull(basic);

      assertTrue(basic.at("target").isArray());
      assertEquals(1, basic.at("target").asList().size());
      assertEquals("FIELD", basic.at("target").at(0).asString());

      Json attributes = basic.at("attributes");
      assertTrue(attributes.isObject());
      assertTrue(attributes.has(InfinispanAnnotations.NAME_ATTRIBUTE));
      assertTrue(attributes.has(InfinispanAnnotations.SEARCHABLE_ATTRIBUTE));
      assertTrue(attributes.has(InfinispanAnnotations.PROJECTABLE_ATTRIBUTE));

      Json searchable = attributes.at(InfinispanAnnotations.SEARCHABLE_ATTRIBUTE);
      assertEquals("BOOLEAN", searchable.at("type").asString());
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
