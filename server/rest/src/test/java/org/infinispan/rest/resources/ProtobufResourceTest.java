package org.infinispan.rest.resources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.util.Util.getResourceAsString;
import static org.infinispan.testing.Testing.tmpDirectory;
import static org.testng.AssertJUnit.assertEquals;

import java.nio.file.Paths;
import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestSchemaClient;
import org.infinispan.commons.internal.InternalCacheNames;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.resources.ProtobufResource.ProtoSchema;
import org.infinispan.security.Security;
import org.infinispan.server.core.query.ProtobufMetadataManager;
import org.infinispan.testing.Exceptions;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

@Test(groups = "functional", testName = "rest.ProtobufResourceTest")
public class ProtobufResourceTest extends AbstractRestResourceTest {

   private static final ObjectMapper MAPPER = new ObjectMapper();
   protected String PERSISTENT_LOCATION = tmpDirectory(ProtobufResourceTest.class.getName());
   public static final String PERSON_PROTO = "person.proto";
   public static final String ERROR_PROTO = "error.proto";

   @Override
   public Object[] factory() {
      return new Object[]{
            new ProtobufResourceTest().withSecurity(false).browser(false),
            new ProtobufResourceTest().withSecurity(false).browser(true),
            new ProtobufResourceTest().withSecurity(true).browser(false),
            new ProtobufResourceTest().withSecurity(true).browser(true),
      };
   }

   protected GlobalConfigurationBuilder getGlobalConfigForNode(int id) {
      GlobalConfigurationBuilder config = super.getGlobalConfigForNode(id);
      config.globalState().enable()
            .configurationStorage(ConfigurationStorage.OVERLAY)
            .persistentLocation(Paths.get(PERSISTENT_LOCATION, Integer.toString(id)).toString())
            .metrics().accurateSize(true);
      return config;
   }

   @BeforeMethod(alwaysRun = true)
   @Override
   public void createBeforeMethod() {
      //Remove user schemas to avoid conflicts between methods.
      //Do not use clear() as it also unregisters built-in protobuf types (e.g. WrappedMessage) from the serialization context.
      Security.doAs(ADMIN, () -> {
         Cache<String, String> cache = cacheManagers.get(0).getCache(InternalCacheNames.PROTOBUF_METADATA_CACHE_NAME);
         cache.keySet().forEach(cache::remove);
      });
   }

   public void listSchemasWhenEmpty() throws Exception {
      CompletionStage<RestResponse> response = client.schemas().names();

      ResponseAssertion.assertThat(response).isOk();
      ProtoSchema[] schemas = MAPPER.readValue(join(response).body(), ProtoSchema[].class);
      assertEquals(0, schemas.length);
   }

   @Test
   public void getNotExistingSchema() {
      CompletionStage<RestResponse> response = client.schemas().get("coco");
      ResponseAssertion.assertThat(response).isNotFound();
   }

   @Test
   public void updateNonExistingSchema() throws Exception {
      String person = getResourceAsString(PERSON_PROTO, getClass().getClassLoader());

      CompletionStage<RestResponse> response = client.schemas().put("person", person);
      ResponseAssertion.assertThat(response).isOk();
   }

   @Test
   public void putAndGetWrongProtobuf() throws Exception {
      RestSchemaClient schemaClient = client.schemas();

      String errorProto = getResourceAsString(ERROR_PROTO, getClass().getClassLoader());

      RestResponse response = join(schemaClient.post("error", errorProto));

      String cause = "Syntax error in error.proto at 3:7: unexpected label: messoge";

      ResponseAssertion.assertThat(response).isOk();
      ProtoSchema protoSchema = MAPPER.readValue(response.body(), ProtoSchema.class);
      assertEquals(ERROR_PROTO, protoSchema.name);
      assertEquals("Schema error.proto has errors", protoSchema.error.message);
      assertEquals(cause, protoSchema.error.cause);

      // Read adding .proto should also work
      response = join(schemaClient.get("error"));
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentEqualToFile(ERROR_PROTO);

      checkListProtobufEndpointUrl(ERROR_PROTO, cause);
   }

   @Test
   public void crudSchema() throws Exception {
      final String SIMPLE = "simple";
      final String SIMPLE_PROTO = SIMPLE + ProtobufMetadataManager.PROTO_KEY_SUFFIX;

      RestSchemaClient schemaClient = client.schemas();
      String simpleProto = getResourceAsString(SIMPLE_PROTO, getClass().getClassLoader());

      // Create
      RestResponse response = join(schemaClient.post(SIMPLE, simpleProto));
      ResponseAssertion.assertThat(response).isOk();

      ProtoSchema protoSchema = MAPPER.readValue(response.body(), ProtoSchema.class);
      assertThat(protoSchema.error).isNull();
      assertThat(protoSchema.name).isEqualTo(SIMPLE_PROTO);

      // Read
      response = join(schemaClient.get(SIMPLE));

      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentEqualToFile(SIMPLE_PROTO);

      // Read adding .proto should also work
      response = join(schemaClient.get(SIMPLE_PROTO));
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentEqualToFile(SIMPLE_PROTO);

      // Delete a cache with the schema
      RestCacheClient cacheClient = adminClient.cache("testSchemasList");
      join(cacheClient.delete());

      // Read with metadata
      response = join(schemaClient.getDetailed(SIMPLE_PROTO));
      ResponseAssertion.assertThat(response).isOk();
      ProtobufResource.ProtoSchemaContent schemaContent = MAPPER.readValue(response.body(), ProtobufResource.ProtoSchemaContent.class);
      assertEquals(SIMPLE_PROTO, schemaContent.name);
      assertEquals(simpleProto, schemaContent.content);
      assertThat(schemaContent.error).isNull();
      assertThat(schemaContent.caches).isEmpty();

      // Create a cache with the schema
      String cacheConfig = cacheConfigToJson("testSchemasList", new ConfigurationBuilder()
            .indexing().enable().addIndexedEntities("org.infinispan.rest.search.simple.Simple").build());
      RestEntity config = RestEntity.create(APPLICATION_JSON, cacheConfig);
      ResponseAssertion.assertThat(cacheClient.createWithConfiguration(config)).isOk();

      // Read with metadata
      response = join(schemaClient.getDetailed(SIMPLE_PROTO));
      ResponseAssertion.assertThat(response).isOk();
      schemaContent = MAPPER.readValue(response.body(), ProtobufResource.ProtoSchemaContent.class);
      assertEquals(SIMPLE_PROTO, schemaContent.name);
      assertEquals(simpleProto, schemaContent.content);
      assertThat(schemaContent.error).isNull();
      assertThat(schemaContent.caches).contains("testSchemasList");

      // Update
      response = join(schemaClient.put(SIMPLE, simpleProto));
      ResponseAssertion.assertThat(response).isOk();

      // Delete
      response = join(schemaClient.delete(SIMPLE));
      ResponseAssertion.assertThat(response).isOk();

      response = join(schemaClient.get(SIMPLE));
      ResponseAssertion.assertThat(response).isNotFound();
   }

   @Test
   public void createTwiceSchema() throws Exception {
      RestSchemaClient schemaClient = client.schemas();

      String personProto = getResourceAsString(PERSON_PROTO, getClass().getClassLoader());
      CompletionStage<RestResponse> response = schemaClient.post("person", personProto);
      ResponseAssertion.assertThat(response).isOk();
      response = schemaClient.post("person", personProto);
      ResponseAssertion.assertThat(response).isConflicted();
   }

   @Test
   public void addAndGetListOrderedByName() throws Exception {
      RestSchemaClient schemaClient = client.schemas();

      String personProto = getResourceAsString(PERSON_PROTO, getClass().getClassLoader());

      join(schemaClient.post("users", personProto));
      join(schemaClient.post("people", personProto));
      join(schemaClient.post("dancers", personProto));

      RestResponse response = join(schemaClient.names());

      ResponseAssertion.assertThat(response).isOk();
      ProtoSchema[] schemas = MAPPER.readValue(response.body(), ProtoSchema[].class);
      assertEquals(3, schemas.length);
      assertEquals("dancers.proto", schemas[0].name);
      assertEquals("people.proto", schemas[1].name);
      assertEquals("users.proto", schemas[2].name);
   }

   @Test
   public void getSchemaTypes() throws Exception {
      RestSchemaClient schemaClient = client.schemas();

      String personProto = getResourceAsString(PERSON_PROTO, getClass().getClassLoader());

      join(schemaClient.post("users", personProto));

      RestResponse response = join(schemaClient.types());
      ResponseAssertion.assertThat(response).isOk();
      String[] types = MAPPER.readValue(response.body(), String[].class);
      assertThat(types).contains("org.infinispan.rest.search.entity.Person");
      assertThat(types).contains("org.infinispan.rest.search.entity.Address");
      assertThat(types).contains("org.infinispan.rest.search.entity.PhoneNumber");
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

   private void checkListProtobufEndpointUrl(String fileName, String errorMessage) throws Exception {
      RestSchemaClient schemaClient = client.schemas();

      RestResponse response = join(schemaClient.names());

      ProtoSchema[] schemas = MAPPER.readValue(response.body(), ProtoSchema[].class);
      assertEquals(1, schemas.length);
      assertEquals(fileName, schemas[0].name);

      assertEquals("Schema error.proto has errors", schemas[0].error.message);
      assertEquals(errorMessage, schemas[0].error.cause);
   }
}
